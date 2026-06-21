import os
import re
import json
import logging
import asyncio
import time
import sys
import subprocess
import tempfile
from telethon import TelegramClient, events
from telethon.sessions import MemorySession
from typing import Optional
from dataclasses import dataclass
from enum import Enum

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


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ForwardTask:
    event: events.NewMessage.Event
    link: str
    status_msg_id: int
    chat_id: int
    status: TaskStatus = TaskStatus.PENDING
    error_msg: str = ""
    created_at: float = 0
    grouped_id: Optional[int] = None
    mode: str = "clone"  # "clone" 或 "export"
    message_ids: Optional[list[int]] = None
    source_chat_id: Optional[int] = None


class TaskQueue:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.queue: asyncio.Queue[ForwardTask] = asyncio.Queue()
        self.current_task: Optional[ForwardTask] = None
        self.worker_task: Optional[asyncio.Task] = None
        self.running = False
        self.task_counter = 0

    async def start(self):
        self.running = True
        self.worker_task = asyncio.create_task(self._worker())
        logger.info("任务队列已启动")

    async def stop(self):
        self.running = False
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
        logger.info("任务队列已停止")

    async def add_task(self, event, link: str, status_msg_id: int, chat_id: int, grouped_id: Optional[int] = None,
                       mode: str = "clone", message_ids: Optional[list[int]] = None, source_chat_id: Optional[int] = None) -> int:
        self.task_counter += 1
        task = ForwardTask(
            event=event,
            link=link,
            status_msg_id=status_msg_id,
            chat_id=chat_id,
            created_at=time.time(),
            grouped_id=grouped_id,
            mode=mode,
            message_ids=message_ids,
            source_chat_id=source_chat_id
        )
        await self.queue.put(task)
        position = self.queue.qsize()
        logger.info(f"任务已加入队列，位置: {position}, mode: {mode}")
        return position

    async def _worker(self):
        while self.running:
            try:
                task = await self.queue.get()
                self.current_task = task
                task.status = TaskStatus.PROCESSING
                await self._process_task(task)
                self.queue.task_done()
                self.current_task = None
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"队列处理异常: {e}", exc_info=True)
                if self.current_task:
                    self.current_task.status = TaskStatus.FAILED
                    self.current_task.error_msg = str(e)
                self.current_task = None

    async def _process_task(self, task: ForwardTask):
        try:
            status_msg = await task.event.client.get_messages(task.chat_id, ids=task.status_msg_id)
            if not status_msg:
                logger.error("无法获取状态消息")
                return

            target_id = self.bot.target_chat_id
            if target_id is None:
                await status_msg.edit("❌ 未配置目标频道ID")
                task.status = TaskStatus.FAILED
                task.error_msg = "未配置目标频道ID"
                if task.grouped_id:
                    self.bot._album_handling.discard(task.grouped_id)
                return

            if task.mode == "clone":
                await status_msg.edit(
                    f"🔄 正在转发（队列中）...\n"
                    f"FROM: {task.link}\n"
                    f"TO:   {target_id}"
                )
                ok, output = await self.bot.tdl.forward_clone(task.link, target_id)
                self.bot.forward_log.add(task.link, target_id, ok, "" if ok else output)

                if ok:
                    await status_msg.edit(
                        f"✅ 转发成功\n"
                        f"FROM: {task.link}\n"
                        f"TO:   {target_id}"
                    )
                    task.status = TaskStatus.COMPLETED
                else:
                    await status_msg.edit(
                        f"❌ 转发失败\n"
                        f"FROM: {task.link}\n"
                        f"错误: {output}"
                    )
                    task.status = TaskStatus.FAILED
                    task.error_msg = output

            elif task.mode == "export_range":
                await status_msg.edit(
                    f"📥 正在导出消息范围...\n"
                    f"源: {task.link}\n"
                    f"范围: {task.message_ids[0]}-{task.message_ids[1]}"
                )
                with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
                    export_file = tmp.name
                try:
                    ok, output = await self.bot.tdl.export_range(
                        task.source_chat_id, task.message_ids[0], task.message_ids[1], export_file
                    )
                    if not ok:
                        await status_msg.edit(f"❌ 导出失败: {output}")
                        task.status = TaskStatus.FAILED
                        task.error_msg = output
                        return

                    await status_msg.edit(
                        f"📤 正在转发导出文件...\n"
                        f"文件: {os.path.basename(export_file)}"
                    )
                    ok, output = await self.bot.tdl.forward_from_export(export_file, target_id)
                    self.bot.forward_log.add(task.link, target_id, ok, "" if ok else output)

                    if ok:
                        await status_msg.edit(
                            f"✅ 导出+转发成功 (范围)\n"
                            f"源: {task.link}\n"
                            f"范围: {task.message_ids[0]}-{task.message_ids[1]}\n"
                            f"TO: {target_id}"
                        )
                        task.status = TaskStatus.COMPLETED
                    else:
                        await status_msg.edit(
                            f"❌ 导出+转发失败 (范围)\n"
                            f"源: {task.link}\n"
                            f"范围: {task.message_ids[0]}-{task.message_ids[1]}\n"
                            f"错误: {output}"
                        )
                        task.status = TaskStatus.FAILED
                        task.error_msg = output
                finally:
                    if os.path.exists(export_file):
                        os.remove(export_file)

        except Exception as e:
            logger.error(f"任务处理异常: {e}", exc_info=True)
            task.status = TaskStatus.FAILED
            task.error_msg = str(e)
            try:
                status_msg = await task.event.client.get_messages(task.chat_id, ids=task.status_msg_id)
                if status_msg:
                    await status_msg.edit(f"❌ 处理异常: {e}")
            except Exception:
                pass
        finally:
            if task.grouped_id:
                self.bot._album_handling.discard(task.grouped_id)

    def get_queue_status(self) -> dict:
        return {
            "queue_size": self.queue.qsize(),
            "current_task": {
                "link": self.current_task.link[:50] if self.current_task else None,
                "status": self.current_task.status.value if self.current_task else None
            } if self.current_task else None,
            "running": self.running
        }

# 管理员帮助说明
START_MESSAGE_ADMIN = (
    '🤖 消息转发助手管理面板\n\n'
    '【基础用法】\n'
    '1. 直接转发频道/群消息给机器人\n'
    '2. 发送公开或私有频道消息链接：\n'
    '   https://t.me/channel_name/123\n'
    '   https://t.me/c/123456789/123\n'
    '3. 机器人会按队列克隆转发到当前目标频道\n\n'
    '【管理员命令】\n'
    '/forwardto <频道ID>\n'
    '  设置或修改目标频道，使用 tdl 可识别的裸 ID，不需要加 -100 前缀\n'
    '  示例：/forwardto 1234567890\n\n'
    '/showto\n'
    '  查看当前目标频道 ID\n\n'
    '/queue\n'
    '  查看当前等待中的转发任务数量\n\n'
    '/flog\n'
    '  查看最近 10 条转发记录和失败原因\n\n'
    '/flog clear\n'
    '  清空本地转发记录文件\n\n'
    '/export_range <来源> <起始消息ID> <结束消息ID>\n'
    '  从指定 Chat 导出一段消息并转发到目标频道\n'
    '  来源支持裸 Chat ID、t.me/c 频道链接或消息链接\n'
    '  示例：/export_range 3482276715 100 200\n'
    '  示例：/export_range https://t.me/c/3482276715 100 200\n'
    '  示例：/export_range https://t.me/c/3482276715/100 https://t.me/c/3482276715/200\n\n'
    '/restart\n'
    '  重启机器人进程\n\n'
    '/help\n'
    '  显示本帮助\n\n'
    '【注意事项】\n'
    '- 目标频道必须先用 /forwardto 设置，或通过 FORWARD_TO_CHAT_ID 环境变量配置\n'
    '- 机器人和 tdl 登录账号需要有源频道读取权限、目标频道发帖权限\n'
    '- export_range 适合批量补发，消息 ID 范围不宜一次过大'
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


def strip_url_query(text: str) -> str:
    return text.strip().split('?', 1)[0].rstrip('/')


def parse_private_channel_link(text: str) -> tuple[Optional[int], Optional[int]]:
    """解析 t.me/c/<chat_id>[/message_id] 链接。

    返回: (chat_id, message_id)。频道链接没有 message_id 时返回 (chat_id, None)。
    公开用户名链接无法得到 tdl 裸 Chat ID，因此不在这里解析。
    """
    link = strip_url_query(text)
    match = re.match(r"https?://t\.me/c/(-?\d+)(?:/(\d+))?/?$", link)
    if not match:
        return None, None

    chat_id = int(match.group(1))
    message_id = int(match.group(2)) if match.group(2) else None
    return chat_id, message_id


def parse_export_range_args(parts: list[str]) -> tuple[Optional[int], Optional[int], Optional[int], Optional[str]]:
    """解析 /export_range 参数。

    支持:
    - /export_range <Chat ID> <起始ID> <结束ID>
    - /export_range <频道链接> <起始ID> <结束ID>
    - /export_range <起始消息链接> <结束消息链接>
    - /export_range <消息链接> <结束ID>
    """
    args = parts[1:]
    if len(args) < 2:
        return None, None, None, "missing"

    first_chat_id, first_msg_id = parse_private_channel_link(args[0])

    if first_chat_id is not None:
        if first_msg_id is not None:
            if len(args) < 2:
                return None, None, None, "missing_end_id"

            second_chat_id, second_msg_id = parse_private_channel_link(args[1])
            if second_chat_id is not None:
                if second_chat_id != first_chat_id:
                    return None, None, None, "chat_mismatch"
                if second_msg_id is None:
                    return None, None, None, "missing_end_id"
                return first_chat_id, first_msg_id, second_msg_id, None

            end_id = parse_chat_id(args[1])
            if end_id is None:
                return None, None, None, "invalid_end_id"
            return first_chat_id, first_msg_id, end_id, None

        if len(args) < 3:
            return None, None, None, "missing_range"
        start_id = parse_chat_id(args[1])
        end_id = parse_chat_id(args[2])
        if start_id is None or end_id is None:
            return None, None, None, "invalid_range_id"
        return first_chat_id, start_id, end_id, None

    if args[0].startswith(("http://t.me/", "https://t.me/")):
        return None, None, None, "public_link_not_supported"

    if len(args) < 3:
        return None, None, None, "missing_range"

    chat_id = parse_chat_id(args[0])
    start_id = parse_chat_id(args[1])
    end_id = parse_chat_id(args[2])
    if chat_id is None or start_id is None or end_id is None:
        return None, None, None, "invalid_range_id"
    return chat_id, start_id, end_id, None


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

    @staticmethod
    def normalize_chat_id(to_chat_id: int) -> str:
        """剥掉 -100 前缀，tdl 接受裸 ID"""
        raw = str(to_chat_id).lstrip('-')
        if raw.startswith('100') and len(raw) > 10:
            raw = raw[3:]
        return raw

    @staticmethod
    def parse_message_link(link: str) -> tuple[Optional[int], Optional[int]]:
        """从 t.me 链接解析 chat_id 和 message_id
        支持格式:
        - https://t.me/username/123
        - https://t.me/c/123456789/123 (私有频道，数字 ID)
        - http://t.me/... 也支持
        - 带查询参数如 ?single 也支持
        返回的 chat_id 为链接中的原始数字，直接供 tdl 使用
        """
        # 去除可能的空白字符
        link = link.strip()
        # 去除查询参数
        if '?' in link:
            link = link.split('?')[0]
        logger.debug(f"解析链接: {link}")
        patterns = [
            r"https?://t\.me/([A-Za-z0-9_]+)/(\d+)/?",        # 公开频道
            r"https?://t\.me/c/(-?\d+)/(\d+)/?",              # 私有频道
        ]
        for pattern in patterns:
            match = re.match(pattern, link)
            if match:
                chat_part = match.group(1)
                msg_id = int(match.group(2))
                if chat_part.lstrip('-').isdigit():
                    # 链接中已是 tdl 需要的裸 ID，直接使用
                    chat_id = int(chat_part)
                    logger.debug(f"解析成功: chat_id={chat_id}, msg_id={msg_id}")
                else:
                    chat_id = None  # 需要用户名解析
                return chat_id, msg_id
        logger.warning(f"链接格式不匹配: {link}")
        return None, None

    async def export_range(self, chat_id: int, start_id: int, end_id: int, output_file: str) -> tuple[bool, str]:
        """导出消息 ID 范围到文件"""
        # chat_id 已是链接中的裸 ID，直接使用
        # tdl 使用 chat export 子命令，参数: -c chat_id -T id -i "start,end" -o output
        ids_str = f"{start_id},{end_id}"
        cmd = [
            self.tdl_path, "chat", "export",
            "-c", str(chat_id),
            "-T", "id",
            "-i", ids_str,
            "-o", output_file,
        ]
        logger.info(f"执行 TDL export range 命令: {' '.join(cmd)}")
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            out = stdout.decode('utf-8', errors='ignore').strip()
            err = stderr.decode('utf-8', errors='ignore').strip()

            if process.returncode == 0 and os.path.exists(output_file):
                return True, out or f"导出成功: {output_file}"
            return False, err or out or "导出失败"
        except Exception as e:
            logger.error(f"TDL export range 执行异常: {e}")
            return False, str(e)

    async def forward_from_export(self, export_file: str, to_chat_id: int) -> tuple[bool, str]:
        """从导出文件转发"""
        to_arg = self.normalize_chat_id(to_chat_id)
        # tdl forward --from file.json --to chat_id --mode clone --desc
        cmd = [
            self.tdl_path, "forward",
            "--from", export_file,
            "--to", to_arg,
            "--mode", "clone",
            "--desc",
        ]
        logger.info(f"执行 TDL forward 命令: {' '.join(cmd)}")
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

    async def forward_clone(self, from_link: str, to_chat_id: int) -> tuple[bool, str]:
        to_arg = self.normalize_chat_id(to_chat_id)
        cmd = [
            self.tdl_path, "forward",
            "--from", from_link,
            "--to", to_arg,
            "--mode", "clone",
        ]
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
        self.task_queue = TaskQueue(self)

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

            if getattr(fwd, 'link', None):
                return fwd.link

            if hasattr(fwd, 'from_id') and fwd.from_id:
                try:
                    from_id = fwd.from_id
                    chat = await event.client.get_entity(from_id)

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
        if "?single" not in link:
            link = link + "?single"
        return link

    # ── Core forward logic ──────────────────────────────────────

    async def do_forward(self, event, status_msg) -> None:
        if self.target_chat_id is None:
            await status_msg.edit("❌ 未配置目标频道ID，请管理员使用 /forwardto 设置")
            return

        if self.is_album(event):
            gid = getattr(event.message, 'grouped_id', None)
            if gid and gid in self._album_processed:
                logger.info(f"跳过 Album 后续消息: grouped_id={gid}")
                return
            if gid:
                self._album_processed.add(gid)
                if len(self._album_processed) > 500:
                    self._album_processed.clear()

        link = await self.resolve_link(event)

        if not link:
            await status_msg.edit("❌ 无法识别消息链接，请转发消息或发送 https://t.me/... 链接")
            return

        if any(kw in link.lower() for kw in EXCLUDE_KEYWORDS):
            await status_msg.edit(f"❌ 链接包含排除关键词，已跳过: {link}")
            return

        if self.is_album(event):
            link = self.add_single_param(link)
            logger.info(f"Album 消息添加 ?single 参数: {link}")

        gid = getattr(event.message, 'grouped_id', None)
        position = await self.task_queue.add_task(event, link, status_msg.id, event.chat_id, gid)
        await status_msg.edit(
            f"⏳ 已加入转发队列\n"
            f"队列位置: {position}\n"
            f"FROM: {link}\n"
            f"TO:   {self.target_chat_id}"
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
                await event.respond(
                    "❌ 缺少目标频道 ID\n\n"
                    "用法：/forwardto <频道ID>\n"
                    "示例：/forwardto 1234567890\n\n"
                    "说明：频道 ID 使用 tdl 可识别的裸 ID，不需要加 -100 前缀；机器人和 tdl 账号需要具备发帖权限。"
                )
                return True
            new_id = parse_chat_id(parts[1])
            if new_id is None:
                await event.respond(
                    "❌ 频道 ID 格式无效\n\n"
                    "请只输入数字，不需要加 -100 前缀，例如：\n"
                    "/forwardto 1234567890"
                )
                return True
            self.target_chat_id = new_id
            await event.respond(
                f"✅ 目标频道已更新\n"
                f"当前目标频道 ID：{self.target_chat_id}\n\n"
                "之后收到的有效消息会转发到该频道。"
            )
            return True

        if command == '/showto':
            if self.target_chat_id is None:
                await event.respond(
                    "ℹ️ 尚未设置目标频道\n\n"
                    "请使用以下命令设置：\n"
                    "/forwardto 1234567890"
                )
            else:
                await event.respond(
                    f"🎯 当前目标频道 ID：{self.target_chat_id}\n\n"
                    "所有新加入队列的任务都会转发到该频道。"
                )
            return True

        if command == '/flog':
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

        if command == '/queue':
            status = self.task_queue.get_queue_status()
            queue_size = status["queue_size"]
            current_task = status["current_task"]

            if queue_size == 0 and not current_task:
                await event.respond("📋 转发队列为空，当前没有正在处理或等待中的任务。")
                return True

            lines = [
                "📋 转发队列状态：\n",
                f"运行状态：{'运行中' if status['running'] else '已停止'}\n",
                f"等待中：{queue_size}\n",
            ]
            if current_task:
                lines.extend([
                    "\n正在处理：\n",
                    f"状态：{current_task['status']}\n",
                    f"来源：{current_task['link']}\n",
                ])
            await event.respond("".join(lines))
            return True

        if command == '/export_range':
            chat_id, start_id, end_id, parse_error = parse_export_range_args(parts)
            if parse_error:
                error_tips = {
                    "missing": (
                        "❌ 缺少范围导出参数\n\n"
                        "用法 1：/export_range <Chat ID> <起始消息ID> <结束消息ID>\n"
                        "用法 2：/export_range <频道链接> <起始消息ID> <结束消息ID>\n"
                        "用法 3：/export_range <起始消息链接> <结束消息链接>\n\n"
                        "示例：\n"
                        "/export_range 3482276715 100 200\n"
                        "/export_range https://t.me/c/3482276715 100 200\n"
                        "/export_range https://t.me/c/3482276715/100 https://t.me/c/3482276715/200"
                    ),
                    "missing_range": (
                        "❌ 缺少起始或结束消息 ID\n\n"
                        "频道链接后面还需要填写起始消息 ID 和结束消息 ID。\n"
                        "示例：/export_range https://t.me/c/3482276715 100 200"
                    ),
                    "missing_end_id": (
                        "❌ 缺少结束消息 ID\n\n"
                        "使用消息链接作为起点时，需要再提供结束消息 ID 或结束消息链接。\n"
                        "示例：/export_range https://t.me/c/3482276715/100 200"
                    ),
                    "invalid_end_id": (
                        "❌ 结束消息 ID 格式错误\n\n"
                        "结束位置请填写数字，或填写同一频道的消息链接。\n"
                        "示例：/export_range https://t.me/c/3482276715/100 200"
                    ),
                    "invalid_range_id": (
                        "❌ 参数格式错误\n\n"
                        "Chat ID、起始消息 ID、结束消息 ID 必须是数字；也可以使用 t.me/c 私有频道链接。\n"
                        "示例：/export_range 3482276715 100 200"
                    ),
                    "chat_mismatch": (
                        "❌ 起始消息链接和结束消息链接不属于同一个频道\n\n"
                        "请确认两个链接中的 Chat ID 一致。"
                    ),
                    "public_link_not_supported": (
                        "❌ 公开用户名链接无法自动提取数字 Chat ID\n\n"
                        "请改用 t.me/c/<Chat ID>/<消息ID> 链接，或手动填写 Chat ID。\n"
                        "示例：/export_range 3482276715 100 200"
                    ),
                }
                await event.respond(error_tips.get(parse_error, "❌ 参数解析失败，请检查 /export_range 用法。"))
                return True

            if start_id > end_id:
                await event.respond(
                    "❌ 消息范围无效\n\n"
                    "起始消息 ID 不能大于结束消息 ID。\n"
                    "示例：/export_range 3482276715 100 200"
                )
                return True

            logger.info(f"处理 export_range 命令, chat_id: {chat_id}, range: {start_id}-{end_id}")

            # 用于显示的链接占位
            display_link = f"chat_id:{chat_id}"

            status_msg = await event.respond("⏳ 正在加入范围导出队列...")
            gid = None
            position = await self.task_queue.add_task(
                event, display_link, status_msg.id, event.chat_id, gid,
                mode="export_range",
                message_ids=[start_id, end_id],
                source_chat_id=chat_id
            )
            await status_msg.edit(
                f"⏳ 已加入范围导出队列\n"
                f"队列位置: {position}\n"
                f"源: {display_link}\n"
                f"范围: {start_id}-{end_id}\n"
                f"TO: {self.target_chat_id}"
            )
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
        await self.task_queue.start()

        for admin_id in ADMIN_IDS:
            try:
                await self.client.send_message(admin_id, START_MESSAGE_ADMIN)
                logger.info(f"已向管理员 {admin_id} 发送启动消息")
            except Exception as e:
                logger.error(f"向管理员 {admin_id} 发送启动消息失败: {e}")

        @self.client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            if event.sender_id in ADMIN_IDS:
                await event.respond(START_MESSAGE_ADMIN)
            else:
                await event.respond("🤖 机器人已启动。请直接转发消息或发送 Telegram 消息链接。")

        @self.client.on(events.NewMessage)
        async def message_handler(event):
            try:
                text = event.message.text or ""

                if text.startswith('/'):
                    if await self.handle_admin_command(event):
                        return

                has_forward = bool(event.message.forward)
                has_link = bool(self.extract_link(text))

                if has_forward or has_link:
                    is_album = self.is_album(event)
                    gid = getattr(event.message, 'grouped_id', None) if is_album else None

                    if is_album and gid and gid in self._album_handling:
                        logger.info(f"跳过 Album 消息（已在处理中）: grouped_id={gid}")
                        return

                    if is_album and gid:
                        self._album_handling.add(gid)
                        if len(self._album_handling) > 500:
                            self._album_handling.clear()

                    status_msg = await event.respond("⏳ 正在加入转发队列...")
                    await self.do_forward(event, status_msg)

            except Exception as e:
                logger.error(f"消息处理异常: {e}", exc_info=True)

        logger.info("Bot 已启动（TDL 集成模式）")
        await self.client.run_until_disconnected()

    async def stop(self) -> None:
        await self.task_queue.stop()
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
    for f in ("bot_session.session",):
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    asyncio.run(main())
