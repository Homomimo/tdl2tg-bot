import os
import re
import json
import logging
import asyncio
import time
import sys
import subprocess
from telethon import TelegramClient, events
from telethon.sessions import MemorySession
from typing import Optional

# ─────────────────────────── Logging ───────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

# ─────────────────────────── Config ────────────────────────────
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TDL_PATH = "/usr/local/bin/tdl"
FORWARD_LOG_FILE = "forward_history.json"
FORWARD_LOG_MAX_DAYS = 7  # 保留最近7天
TELEGRAM_LINK_PATTERN = re.compile(r"https://t\.me/(?:c/)?[A-Za-z0-9_]+/\d+")
EXCLUDE_KEYWORDS = ["addlist"]

# 对管理员展示完整帮助，对普通用户只展示基础说明
START_MESSAGE_PUBLIC = (
    '🤖 机器人已启动！\n\n'
    '👋 你好！我是消息转发助手\n\n'
    '📌 支持两种输入方式：\n'
    '1) 直接转发消息给我\n'
    '2) 粘贴 https://t.me/xxx/123 链接\n\n'
    '🚀 收到消息后会自动转发到目标频道'
)

START_MESSAGE_ADMIN = (
    START_MESSAGE_PUBLIC + '\n\n'
    '📌 管理员命令:\n'
    '/forwardto <ID> - 修改接收频道ID\n'
    '/showto         - 查看接收频道ID\n'
    '/flog           - 查看转发记录\n'
    '/flog clear     - 清除历史记录\n'
    '/restart        - 重启机器人\n'
    '/help           - 查看帮助'
)


# ─────────────────────────── Helpers ───────────────────────────
def parse_admin_ids(raw: str) -> list[int]:
    result = []
    for item in raw.split(','):
        item = item.strip()
        if not item:
            continue
        try:
            result.append(int(item))
        except ValueError:
            logger.warning(f"忽略无效管理员ID: {item}")
    return result


def parse_chat_id(text: str) -> Optional[int]:
    try:
        return int(text.strip())
    except (ValueError, AttributeError):
        return None


ADMIN_IDS: list[int] = parse_admin_ids(os.getenv("ADMIN_IDS", ""))
TARGET_CHAT_ID: Optional[int] = parse_chat_id(os.getenv("FORWARD_TO_CHAT_ID", ""))


# ─────────────────────────── ForwardLog ────────────────────────
class ForwardLog:
    def __init__(self, log_file: str = FORWARD_LOG_FILE):
        self.log_file = log_file
        self.logs: list[dict] = self._load()

    def _load(self) -> list[dict]:
        if not os.path.exists(self.log_file):
            return []
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载转发日志失败: {e}")
            return []

    def _save(self) -> None:
        # 自动清理超过天数的记录
        if FORWARD_LOG_MAX_DAYS > 0:
            cutoff = time.time() - (FORWARD_LOG_MAX_DAYS * 86400)
            self.logs = [r for r in self.logs if r.get('timestamp', 0) > cutoff]
        
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(self.logs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存转发日志失败: {e}")

    def add(self, from_link: str, to_chat_id: int, success: bool, error_msg: str = "") -> str:
        record_id = f"{len(self.logs) + 1}"
        record = {
            'id': record_id,
            'from': from_link,
            'to': to_chat_id,
            'success': success,
            'error': error_msg,
            'timestamp': time.time(),
            'date': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
        self.logs.append(record)
        self._save()
        return record_id

    def get_all(self) -> list[dict]:
        return self.logs

    def get_recent(self, count: int = 10) -> list[dict]:
        return self.logs[-count:] if self.logs else []

    def clear(self) -> None:
        self.logs = []
        self._save()


# ─────────────────────────── TDLDownloader ─────────────────────
class TDLDownloader:
    def __init__(self, tdl_path: str = TDL_PATH):
        self.tdl_path = tdl_path

    async def forward_clone(self, from_link: str, to_chat_id: int) -> tuple[bool, str]:
        cmd = [self.tdl_path, "forward", "--from", from_link, "--to", str(to_chat_id), "--mode", "clone"]
        logger.info(f"执行 TDL 命令: {' '.join(cmd)}")
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            out = stdout.decode('utf-8', errors='ignore').strip()
            err = stderr.decode('utf-8', errors='ignore').strip()

            if process.returncode == 0:
                return True, out or "转发成功"
            return False, err or out or "未知错误"
        except Exception as e:
            logger.error(f"TDL forward 执行异常: {e}")
            return False, str(e)


# ─────────────────────────── TelegramBot ───────────────────────
class TelegramBot:
    def __init__(self):
        self.client = TelegramClient(MemorySession(), API_ID, API_HASH)
        self.tdl = TDLDownloader()
        self.target_chat_id: Optional[int] = TARGET_CHAT_ID
        self.forward_log = ForwardLog()
        self._album_processed: set[int] = set()
        self._album_handling: set[int] = set()

    # ── Link utilities ──────────────────────────────────────────

    def extract_link(self, text: str) -> Optional[str]:
        if not text:
            return None
        match = TELEGRAM_LINK_PATTERN.search(text)
        if not match:
            return None
        link = match.group(0)
        return None if any(kw in link.lower() for kw in EXCLUDE_KEYWORDS) else link

    def build_message_link(self, chat, msg_id: int) -> str:
        username = getattr(chat, 'username', None)
        if username:
            return f"https://t.me/{username}/{msg_id}"
        return f"https://t.me/c/{chat.id}/{msg_id}"

    async def resolve_link(self, event) -> Optional[str]:
        # 1. 转发来源自带链接
        if event.message.forward:
            fwd = event.message.forward
            
            # 优先使用 forward.link
            if getattr(fwd, 'link', None):
                return fwd.link
            
            # 尝试从 from_id 构建链接
            if hasattr(fwd, 'from_id') and fwd.from_id:
                try:
                    from_id = fwd.from_id
                    chat = await event.client.get_entity(from_id)
                    
                    # 获取消息ID
                    if hasattr(fwd, 'channel_post') and fwd.channel_post:
                        msg_id = fwd.channel_post
                    elif hasattr(fwd, 'message') and fwd.message:
                        msg_id = fwd.message
                    else:
                        msg_id = event.message.id
                    
                    return self.build_message_link(chat, msg_id)
                except Exception as e:
                    logger.error(f"从转发构建链接失败: {e}")
        
        # 2. 消息文本中直接含链接
        link = self.extract_link(event.message.text or "")
        if link:
            return link
        
        # 3. 消息本身的链接属性
        if getattr(event.message, 'link', None):
            return event.message.link
        
        # 4. 根据 chat 信息构建
        chat = await event.get_chat()
        return self.build_message_link(chat, event.message.id)

    # ── Album handling ──────────────────────────────────────────

    def is_album(self, event) -> bool:
        return getattr(event.message, 'grouped_id', None) is not None

    def add_single_param(self, link: str) -> str:
        """给链接添加 ?single 参数"""
        if "?single" not in link:
            link = link + "?single"
        return link

    # ── Core forward logic ──────────────────────────────────────

    async def do_forward(self, event, status_msg) -> None:
        if self.target_chat_id is None:
            await status_msg.edit("❌ 未配置目标频道ID，请管理员使用 /forwardto 设置")
            return

        # Album 合集只处理第一条
        if self.is_album(event):
            gid = getattr(event.message, 'grouped_id', None)
            if gid and gid in self._album_processed:
                logger.info(f"跳过 Album 后续消息: grouped_id={gid}")
                return
            if gid:
                self._album_processed.add(gid)
                # 限制集合大小
                if len(self._album_processed) > 500:
                    self._album_processed.clear()

        link = await self.resolve_link(event)
        
        if not link:
            await status_msg.edit("❌ 无法识别消息链接，请转发消息或发送 https://t.me/... 链接")
            return

        if any(kw in link.lower() for kw in EXCLUDE_KEYWORDS):
            await status_msg.edit(f"❌ 链接包含排除关键词，已跳过: {link}")
            return

        # Album 消息添加 ?single 参数
        if self.is_album(event):
            link = self.add_single_param(link)
            logger.info(f"Album 消息添加 ?single 参数: {link}")

        await status_msg.edit(
            f"🔄 开始转发（clone）...\n"
            f"FROM: {link}\n"
            f"TO:   {self.target_chat_id}"
        )

        ok, output = await self.tdl.forward_clone(link, self.target_chat_id)
        self.forward_log.add(link, self.target_chat_id, ok, "" if ok else output)

        if ok:
            await status_msg.edit(
                f"✅ 转发成功\n"
                f"FROM: {link}\n"
                f"TO:   {self.target_chat_id}"
            )
        else:
            await status_msg.edit(
                f"❌ 转发失败\n"
                f"FROM: {link}\n"
                f"错误: {output}"
            )

    # ── Admin commands ──────────────────────────────────────────

    async def handle_admin_command(self, event) -> bool:
        if event.sender_id not in ADMIN_IDS:
            return False

        raw = event.message.text or ""
        parts = raw.split()
        command = parts[0].lower()

        if command == '/help':
            await event.respond(START_MESSAGE_ADMIN)
            return True

        if command == '/restart':
            await event.respond("🔄 正在重启机器人...")
            await self._restart()
            return True

        if command == '/forwardto':
            if len(parts) < 2:
                await event.respond("❌ 用法: /forwardto <频道ID>\n示例: /forwardto 1234567890")
                return True
            new_id = parse_chat_id(parts[1])
            if new_id is None:
                await event.respond("❌ 频道ID无效，请输入数字（通常以 -100 开头，去除-100填入）")
                return True
            self.target_chat_id = new_id
            await event.respond(f"✅ 目标频道ID已更新: {self.target_chat_id}")
            return True

        if command == '/showto':
            if self.target_chat_id is None:
                await event.respond("ℹ️ 尚未设置目标频道ID，请使用 /forwardto <频道ID>")
            else:
                await event.respond(f"🎯 当前目标频道ID: {self.target_chat_id}")
            return True

        if command == '/flog':
            # /flog clear - 清除历史记录
            if len(parts) > 1 and parts[1].lower() == 'clear':
                self.forward_log.clear()
                await event.respond("🗑️ 已清除所有转发记录")
                return True
            
            logs = self.forward_log.get_recent(10)
            if not logs:
                await event.respond("📝 暂无转发记录")
                return True

            lines = ["📝 转发记录（最近10条）：\n"]
            for log in logs:
                status = "✅" if log['success'] else "❌"
                entry = (
                    f"{status} #{log['id']}\n"
                    f"FROM: {log['from']}\n"
                    f"TO:   {log['to']}\n"
                    f"📅 {log['date']}\n"
                )
                if not log['success']:
                    entry += f"错误: {log['error']}\n"
                entry += "\n"
                if sum(len(l) for l in lines) + len(entry) > 3800:
                    break
                lines.append(entry)

            lines.append(f"共 {len(self.forward_log.get_all())} 条记录")
            await event.respond("".join(lines))
            return True

        return False

    # ── Restart ─────────────────────────────────────────────────

    async def _restart(self) -> None:
        script_path = os.path.abspath(sys.argv[0])
        work_dir = os.path.dirname(script_path)
        logger.info(f"重启机器人: {script_path}")
        try:
            await self.stop()
            with open("google_bot.log", "a") as log_file:
                subprocess.Popen(
                    [sys.executable, script_path],
                    cwd=work_dir,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            await asyncio.sleep(2)
            os._exit(0)
        except Exception as e:
            logger.error(f"重启失败: {e}")
            for admin_id in ADMIN_IDS:
                try:
                    await self.client.send_message(admin_id, f"⚠️ 重启失败: {e}")
                except Exception:
                    pass

    # ── Lifecycle ───────────────────────────────────────────────

    async def start(self) -> None:
        await self.client.start(bot_token=BOT_TOKEN)

        for admin_id in ADMIN_IDS:
            try:
                await self.client.send_message(admin_id, START_MESSAGE_ADMIN)
                logger.info(f"已向管理员 {admin_id} 发送启动消息")
            except Exception as e:
                logger.error(f"向管理员 {admin_id} 发送启动消息失败: {e}")

        @self.client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            msg = START_MESSAGE_ADMIN if event.sender_id in ADMIN_IDS else START_MESSAGE_PUBLIC
            await event.respond(msg)

        @self.client.on(events.NewMessage)
        async def message_handler(event):
            try:
                text = event.message.text or ""

                # 管理员命令优先
                if text.startswith('/'):
                    if await self.handle_admin_command(event):
                        return

                has_forward = bool(event.message.forward)
                has_link = bool(self.extract_link(text))

                if has_forward or has_link:
                    # Album 合集只显示一条处理消息
                    is_album = self.is_album(event)
                    gid = getattr(event.message, 'grouped_id', None) if is_album else None
                    
                    if is_album and gid and gid in self._album_handling:
                        logger.info(f"跳过 Album 消息（已在处理中）: grouped_id={gid}")
                        return
                    
                    if is_album and gid:
                        self._album_handling.add(gid)
                        if len(self._album_handling) > 500:
                            self._album_handling.clear()
                    
                    status_msg = await event.respond("⏳ 正在处理转发任务...")
                    await self.do_forward(event, status_msg)
                    
                    # 处理完成后从_handling中移除
                    if is_album and gid:
                        self._album_handling.discard(gid)

            except Exception as e:
                logger.error(f"消息处理异常: {e}", exc_info=True)

        logger.info("Bot 已启动（TDL 集成模式）")
        await self.client.run_until_disconnected()

    async def stop(self) -> None:
        try:
            await self.client.disconnect()
        except Exception as e:
            logger.error(f"断开连接时出错: {e}")


# ─────────────────────────── Entry ─────────────────────────────
async def main() -> None:
    bot = TelegramBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("用户手动停止")
    except Exception as e:
        error_msg = str(e)
        if "FloodWaitError" in error_msg or "wait of" in error_msg.lower():
            import re
            match = re.search(r'wait of (\d+) seconds', error_msg)
            if match:
                wait_time = int(match.group(1))
                logger.error(f"Telegram 限流，需要等待 {wait_time} 秒 ({wait_time//60} 分钟)")
                logger.error("请等待限流结束后手动重启: docker-compose restart")
        else:
            logger.error(f"致命错误: {e}", exc_info=True)
    finally:
        await bot.stop()


if __name__ == "__main__":
    # 清理可能残留的旧 session 文件
    for f in ("bot_session.session",):
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    asyncio.run(main())
