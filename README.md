


# 📥 Telegram 转发机器人 (Docker + TDL) 
支持禁止复制与转发的文件

![Docker](https://img.shields.io/badge/Docker-支持-blue?logo=docker)
![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![Telegram](https://img.shields.io/badge/Telegram-Bot-0088cc?logo=telegram)

通过 Telegram Bot 转发消息到私人频道，使用 [TDL](https://github.com/iyear/tdl) 加速大文件传输，支持 Docker 容器化部署。

## 🌟 功能特性
- ⚡ **TDL 加速** - 使用宿主机的 `tdl` 工具高效下载
- 📊 **转发统计** - 记录转发历史
- 🔒 **安全存储** - 文件持久化保存到宿主机目录
- 🛡️ **管理员控制** - 支持查看历史、清理文件等管理命令
- 📦 **Docker 集成** - 一键部署，环境隔离

## 🚀 快速部署

### 前提条件
- 已安装 [Docker](https://docs.docker.com/engine/install/) 和 [Docker Compose](https://docs.docker.com/compose/install/)
- 宿主机已安装 `tdl` 并配置执行权限：
  ```bash
  chmod +x /usr/local/bin/tdl
  ```
- 配tdl目录权限
  ```bash
  chmod -R 755 ~/.tdl
  ```
- Telegram API 凭证（从 [@BotFather](https://t.me/BotFather) 获取）

### 步骤 1：克隆仓库
```bash
git clone https://github.com/yourusername/telegram-file-bot.git
cd telegram-file-bot
```

### 步骤 2：配置环境
复制环境模板文件并填写真实值：
```bash
nano docker-compose.yml #编辑配置文件
```

`.env` 文件示例：
```ini
services:
  telegram-bot:
    build: .
    container_name: tdl2tg-bot
    restart: "no"
    volumes:
      - ./forward_history.json:/app/forward_history.json
      - /usr/local/bin/tdl:/usr/local/bin/tdl
      - ~/.tdl:/root/.tdl
    environment:
      - TZ=Asia/Shanghai  # 强制指定时区变量
      - API_ID=16612890 #修改为自己的
      - API_HASH=c0fc7dab1acc44f2a2da55cba248d656 #修改为自己的
      - BOT_TOKEN=7965940462:AAFCSi5PlG5xl9cqQDk6AFuZP4AT-K9OZQM #修改为自己的
      - ADMIN_IDS=1227176277 #修改为自己的
      - FORWARD_TO_CHAT_ID=1234567890 #接收频道ID
```

### 步骤 3：启动服务
```bash
docker-compose build --no-cache && docker-compose up -d
```

### 步骤 4：验证运行状态
```bash
docker-compose logs -f
```

### 常用命令
```bash
docker-compose down #停止容器
docker restart tdl2tg-bot #重启容器
```


## 🛠️ 使用指南

### 基础命令
- 发送任意文件给机器人自动下载
- 回复 `/start` 查看帮助菜单

## 📜 许可证
本项目采用 [MIT License](LICENSE)
