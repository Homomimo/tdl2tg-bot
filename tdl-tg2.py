import os
import json
import logging
import asyncio
import humanize
import time
import re
from telethon import TelegramClient, events, types
from telethon.sessions import MemorySession
from typing import Optional, Dict, Any

# --- 日志系统配置 ---
# 去掉了 FileHandler，仅保留 StreamHandler 供 Docker 实时抓取输出
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- 基础运行参数 ---
API_ID =               # Telegram API ID (从 my.telegram.org 获取)
API_HASH = ''           # Telegram API Hash
BOT_TOKEN = ''          # 从 @BotFather 获取的机器人 Token

TARGET_CHANNEL = ''   # 目标频道 ID (转发目的地)
ADMIN_IDS = []        # 管理员 ID 列表 (仅限这些人触发转发)

TDL_PATH = "/usr/local/bin/tdl" # TDL 执行文件在容器内的路径

class ForwardHistory:
    """
    转发历史记录管理类
    负责持久化保存已处理的任务，防止由于重启导致的任务信息丢失
    """
    def __init__(self, history_file="forward_history.json"):
        self.history_file = history_file
        self.history = self._load_history()
        
    def _load_history(self) -> list:
        """从 JSON 文件加载历史数据"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            logger.error(f"加载历史记录失败: {e}")
            return []
            
    def _save_history(self):
        """将当前内存中的历史记录保存到磁盘"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存历史记录失败: {e}")
            
    def add_forward(self, file_id: int, filename: str, source_link: str, size: int, success: bool):
        """记录一笔新的转发操作"""
        record = {
            'id': len(self.history) + 1,        # 内部自增 ID
            'file_id': file_id,                 # 原消息消息 ID
            'filename': filename,               # 文件名或描述
            'source_link': source_link,         # 原始消息链接
            'size_human': humanize.naturalsize(size) if size else "未知",
            'status': "✅ 成功" if success else "❌ 失败",
            'time': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        self.history.append(record)
        self._save_history()
        return record['id']
        
    def get_recent(self, limit=10):
        """获取最近 N 条记录"""
        return self.history[-limit:]

class FileStats:
    """
    运行统计类
    负责记录 Bot 启动后的累计处理量、成功率和运行时间
    """
    def __init__(self):
        self.processed_count = 0        # 总处理次数
        self.total_size = 0             # 累计处理文件总大小
        self.start_time = time.time()    # 启动时间戳
        self.successful_forwards = 0    # 成功计数

    def add_processed_file(self, size: int, success: bool):
        """累加统计数据"""
        self.processed_count += 1
        if size: self.total_size += size
        if success: self.successful_forwards += 1

    def get_stats(self) -> Dict[str, Any]:
        """生成人类可读的统计摘要"""
        return {
            'processed_count': self.processed_count,
            'total_size': humanize.naturalsize(self.total_size),
            'success_rate': f"{(self.successful_forwards/self.processed_count*100):.1f}%" if self.processed_count > 0 else "0%",
            'uptime': humanize.naturaldelta(time.time() - self.start_time)
        }

class TDLForwarder:
    """
    TDL 核心执行类
    通过外部进程调用 tdl 工具执行真正的转发动作
    """
    def __init__(self, tdl_path=TDL_PATH):
        self.tdl_path = tdl_path
        
    async def forward_file(self, message_link: str, progress_callback=None) -> bool:
        """
        调用 TDL 执行克隆转发
        :param message_link: 消息的 t.me 链接
        :param progress_callback: 处理进度日志的回调函数
        """
        target = str(TARGET_CHANNEL)
        # 构造执行命令
        cmd = [
            self.tdl_path, "forward",
            "--from", str(message_link),
            "--to", target,
            "--mode", "clone"
        ]
        
        logger.info(f"正在执行 TDL 指令: {' '.join(cmd)}")
        
        try:
            # 创建异步子进程
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # 定义实时流读取器
            async def read_stream(stream, is_stderr=False):
                while True:
                    line = await stream.readline()
                    if not line: break
                    line_str = line.decode('utf-8', errors='ignore').strip()
                    # 实时输出到 Docker 控制台
                    if is_stderr: logger.error(f"TDL 报错: {line_str}")
                    else: logger.info(f"TDL 输出: {line_str}")
                    # 传递给消息回调显示在 TG 客户端
                    if progress_callback: await progress_callback(line_str)

            # 同时读取标准输出和错误输出
            await asyncio.gather(
                read_stream(process.stdout),
                read_stream(process.stderr, True)
            )
            await process.wait()
            return process.returncode == 0
        except Exception as e:
            logger.error(f"调用 TDL 时发生底层异常: {e}")
            return False

class TelegramBot:
    """
    机器人逻辑主类
    负责监听消息、鉴权、解析链接并分发任务
    """
    def __init__(self):
        # 使用 MemorySession 保证容器重启时不会因为 Session 文件锁定报错
        self.client = TelegramClient(MemorySession(), API_ID, API_HASH)
        self.stats = FileStats()
        self.history = ForwardHistory()
        self.tdl_forwarder = TDLForwarder()

    async def get_message_link(self, event) -> Optional[str]:
        """
        链接提取逻辑 (优先级: 纯文本链接 > 转发来源属性)
        """
        msg = event.message
        # 1. 如果消息内容中包含链接 (正则匹配)
        if msg.text:
            link_match = re.search(r'(https?://t\.me/[^\s]+)', msg.text)
            if link_match: return link_match.group(1)
        
        # 2. 如果消息是手动转发的 (构造私有链接格式)
        if msg.forward:
            fwd = msg.forward
            if fwd.from_id and isinstance(fwd.from_id, types.PeerChannel):
                return f"https://t.me/c/{fwd.from_id.channel_id}/{fwd.channel_post}"
        return None

    async def handle_admin_commands(self, event) -> bool:
        """管理员命令处理器"""
        if event.sender_id not in ADMIN_IDS: return False
        text = event.message.text.lower()
        
        if text.startswith('/stats'):
            s = self.stats.get_stats()
            await event.respond(f"📊 **运行统计**\n累计任务: {s['processed_count']}\n成功率: {s['success_rate']}\n启动时长: {s['uptime']}")
            return True
            
        elif text.startswith('/history'):
            recent = self.history.get_recent(10)
            if not recent:
                await event.respond("📜 暂无任务记录")
                return True
            msg_lines = ["📜 **最近 10 次克隆记录**:"]
            for r in recent:
                msg_lines.append(f"• `{r['time']}` | {r['status']} | {r['filename']}")
            await event.respond("\n".join(msg_lines))
            return True
        return False

    async def process_task(self, event, message_link: str, file_size=0, file_name="未知"):
        """处理任务生命周期: 启动 -> TDL 运行 -> 结果保存 -> UI 反馈"""
        success = False
        status_msg = await event.respond(f"⏳ **正在启动克隆任务**\n来源: `{message_link}`")
        
        try:
            async def update_progress_ui(log_line):
                """更新 TG 上的实时进度条 (通过过滤关键字符)"""
                if "%" in log_line or "progress" in log_line.lower():
                    try: await status_msg.edit(f"⏳ **克隆进度更新**:\n`{log_line}`")
                    except: pass # 忽略 Telethon 频繁编辑导致的报错

            success = await self.tdl_forwarder.forward_file(message_link, update_progress_ui)
            
            # 存入 JSON 历史数据库
            self.history.add_forward(event.message.id, file_name, message_link, file_size, success)

            if success:
                await status_msg.edit(f"✅ **转发成功！**\n文件: `{file_name}`")
            else:
                await status_msg.edit("❌ **转发失败！**\n请在服务器终端查看 `docker logs` 获取详情。")
                
        except Exception as e:
            logger.error(f"执行任务流水线时出错: {e}")
            await status_msg.edit(f"❌ **系统执行异常**: `{e}`")
        finally:
            self.stats.add_processed_file(file_size, success)

    async def start(self):
        """机器人启动入口"""
        await self.client.start(bot_token=BOT_TOKEN)
        logger.info("--- 机器人已就绪并开始监听消息 ---")

        @self.client.on(events.NewMessage)
        async def message_handler(event):
            # 权限检查
            if event.sender_id not in ADMIN_IDS: return
            
            # 命令检查
            if event.message.text and event.message.text.startswith('/'):
                if await self.handle_admin_commands(event): return

            # 链接与任务解析
            link = await self.get_message_link(event)
            if link:
                # 尽量获取文件名
                file_size = event.message.file.size if event.message.media and event.message.file else 0
                file_name = event.message.file.name if event.message.media and event.message.file else "纯链接任务"
                await self.process_task(event, link, file_size, file_name)

if __name__ == '__main__':
    # 实例化并运行
    bot = TelegramBot()
    bot.client.loop.run_until_complete(bot.start())
    bot.client.run_until_disconnected()
