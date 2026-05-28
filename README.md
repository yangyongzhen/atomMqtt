# AtomMQTT Broker

> 一个基于 Rust 实现的高性能 MQTT Broker，支持 MQTT 3.1.1 (v311) 和 MQTT 5.0 协议，附带 Web 管理界面和 CLI 测试客户端。

![Language](https://img.shields.io/badge/language-Rust-orange)
![MQTT](https://img.shields.io/badge/MQTT-3.1.1%20%7C%205.0-blue)
![License](https://img.shields.io/badge/license-MIT-green)

view blog : (AtomMQTT--使用Rust语音实现的轻量级高性能MQtt服务器)
[https://blog.csdn.net/qq8864/article/details/161432518]

---

## 特性

- ✅ **SQLite 持久化存储** — 会话、订阅、保留消息、遗嘱消息自动持久化到本地数据库，重启后自动恢复
- ✅ **双协议支持** — 同时支持 MQTT 3.1.1 和 MQTT 5.0
- ✅ **主题订阅树** — 基于 Trie 树的高效主题匹配（支持 `+` / `#` 通配符）
- ✅ **消息路由** — PUBLISH 消息自动转发给所有匹配订阅者
- ✅ **QoS 0/1/2** — 完整的服务质量支持
- ✅ **保留消息** — 支持 Retained Message 存储与分发
- ✅ **遗嘱消息** — 支持 Will Message（异常断开时自动发布）
- ✅ **Web 管理界面** — 内置 Actix-Web 仪表盘，实时监控 Broker 指标
- ✅ **WebSocket 订阅** — 浏览器可直接订阅 MQTT 主题，接收实时消息
- ✅ **REST API** — 提供完整的 HTTP API 用于发布消息、管理客户端
- ✅ **匿名/文件认证** — 支持无认证和基于文件的密码认证
- ✅ **ACL Topic 访问控制** — 基于文件的 publish/subscribe/readwrite 权限管理
- ✅ **Web 管理界面认证** — HTTP Basic Auth + JSON 登录页面双重认证
- ✅ **CLI 客户端** — 内置 `mqtt-client` 工具，支持发布/订阅/交互式 Shell
- ✅ **性能指标** — 内置计数器（连接数、消息数、字节数、包数等）

---

## 项目结构

```
rust_mqtt_broker/
├── Cargo.toml                 # 工作空间配置
├── mqtt-core/                 # MQTT 协议核心
│   ├── src/
│   │   ├── common.rs          #   通用类型 (QoS, TopicFilter, ProtocolVersion)
│   │   ├── codec.rs           #   编码/解码公共接口
│   │   ├── v3/                #   MQTT 3.1.1 实现
│   │   │   ├── types.rs       #     包类型定义
│   │   │   └── codec.rs       #     编码解码器
│   │   └── v5/                #   MQTT 5.0 实现
│   │       ├── types.rs       #     包类型定义（含属性）
│   │       ├── codec.rs       #     编码解码器
│   │       └── properties.rs  #     属性定义
│   └── Cargo.toml
├── mqtt-broker/               # Broker 引擎
│   ├── src/
│   │   ├── persistence.rs    #   SQLite 持久化存储（异步批量写入）
│   │   ├── lib.rs             #   BrokerState, BrokerMessage, BrokerHandle
│   │   ├── server.rs          #   TCP 监听、连接处理、消息路由
│   │   ├── config.rs          #   配置结构
│   │   ├── session.rs         #   会话状态管理
│   │   ├── subscription.rs    #   主题订阅树（Trie 实现）
│   │   ├── retention.rs       #   保留消息存储
│   │   ├── will.rs            #   遗嘱消息管理
│   │   ├── metrics.rs         #   性能指标采集
│   │   └── auth.rs            #   认证与授权
│   └── Cargo.toml
├── mqtt-web/                  # Web 管理界面
│   ├── src/
│   │   ├── main.rs            #   入口：启动 Broker + Web 服务器
│   │   ├── api.rs             #   REST API + WebSocket 端点
│   │   └── models.rs          #   响应模型
│   ├── static/                #   前端静态文件
│   │   ├── index.html         #   主页面
│   │   ├── login.html         #   登录页面
│   │   ├── css/dashboard.css  #   样式
│   │   └── js/dashboard.js    #   交互逻辑
│   └── Cargo.toml
├── mqtt-client/               # CLI 测试客户端
│   ├── src/main.rs            # 发布/订阅/Shell 模式
│   └── Cargo.toml
├── Doc/                       # 文档
│   ├── architecture.md        # 架构设计
│   ├── article.md             # 原理与实现
│   ├── message-routing.md     # 消息路由机制
│   ├── protocol-support.md    # MQTT 协议支持
│   └── web-api.md             # Web API 文档
├── config.toml                # Broker 配置文件
├── passwd                     # 密码文件（认证用）
├── acl.conf                   # ACL 规则文件
└── CHANGELOG.md               # 更新日志
```

---

## 快速开始

### 环境要求

- Rust 1.70+（推荐使用 [rustup](https://rustup.rs/) 安装）
- 操作系统：Windows / Linux / macOS

### 构建

```bash
# 克隆项目
git clone <repo-url>
cd rust_mqtt_broker

# 构建所有 crate
cargo build --release

# 仅构建 Web Broker（含前端）
cargo build -p mqtt-web --release
```

### 启动 Broker

```bash
# 启动 MQTT Broker + Web 管理界面（默认端口）
cargo run -p mqtt-web

# 或使用 release 模式
cargo run -p mqtt-web --release
```

启动后：
- MQTT TCP 监听：`tcp://0.0.0.0:1883`
- Web 管理界面：`http://localhost:8081`
- 数据库文件：`broker.db`（自动创建于运行目录）

> **注意**：数据库文件 `broker.db` 在首次启动时自动创建，使用 WAL 模式提升并发性能。

### 使用 CLI 客户端测试

```bash
# 订阅主题
cargo run -p mqtt-client -- sub 127.0.0.1:1883 "test/#" --client-id sub1

# 发布消息
cargo run -p mqtt-client -- pub 127.0.0.1:1883 "test/hello" "Hello MQTT!" --client-id pub1 --qos 1

# 交互式 Shell 模式
cargo run -p mqtt-client -- shell 127.0.0.1:1883 --client-id my-shell
```

---

## Web 管理界面

打开 `http://localhost:8081`，首先进入登录页面：

- **默认用户名**: `admin`
- **默认密码**: `admin`

登录后可以看到以下功能页面：

| 页面 | 功能 |
|------|------|
| 📊 仪表盘 | 实时监控：在线客户端、活跃订阅、消息统计、网络流量 |
| 👥 客户端 | 查看在线客户端详情、手动断开连接 |
| 📋 订阅 | 查看所有活跃订阅（Client ID / 主题过滤器 / QoS）|
| 💾 保留消息 | 查看所有保留消息 |
| 📤 发布消息 | 通过 HTTP API 发布消息到任意主题 |
| 📡 订阅消息 | **通过 WebSocket 实时接收订阅的消息** |
| ℹ️ 服务器信息 | Broker 配置和运行状态 |

> **前端嵌入**：前端静态文件（HTML/CSS/JS）在编译时通过 `include_dir!` 宏直接嵌入到二进制中，运行时无需读取磁盘。
> 生成单文件 `.exe` 即可部署，无额外依赖，Windows/macOS/Linux 全平台兼容。

---

## API 接口

### REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/login` | 用户登录（JSON）|
| `GET` | `/api/metrics` | 获取 Broker 指标快照 |
| `GET` | `/api/broker/info` | 获取 Broker 配置和版本信息 |
| `GET` | `/api/clients` | 获取所有在线客户端 |
| `GET` | `/api/clients/{client_id}` | 获取单个客户端详情 |
| `GET` | `/api/subscriptions` | 获取所有活跃订阅 |
| `GET` | `/api/retained` | 获取所有保留消息 |
| `DELETE` | `/api/retained/{topic}` | 删除指定保留消息 |
| `POST` | `/api/publish` | 发布消息到主题 |
| `POST` | `/api/clients/{client_id}/disconnect` | 断开指定客户端 |

### WebSocket

| 路径 | 协议 | 说明 |
|------|------|------|
| `ws://host:8081/ws/subscribe` | JSON | 实时订阅 MQTT 主题消息 |
| `ws://host:8081/mqtt` | 二进制 MQTT 包 | 原生 WebSocket-MQTT 桥接 |

> **认证**：所有 `/api/` 路由受 HTTP Basic Auth 保护。前端通过登录页面获取验证，后续请求自动携带认证凭据。`POST /api/login` 端点免认证。

#### WebSocket JSON 命令

**订阅主题**：
```json
{"type": "subscribe", "topic_filter": "test/#", "qos": 1}
```

**取消订阅**：
```json
{"type": "unsubscribe", "topic_filter": "test/#"}
```

**心跳**：
```json
{"type": "ping"}
```

**收到消息**：
```json
{
  "type": "publish",
  "topic": "test/hello",
  "payload": "Hello MQTT!",
  "qos": 1,
  "source_client": "pub1",
  "timestamp": "2025-01-15T10:30:00+08:00"
}
```

---

## 配置

Broker 通过 `config.toml` 配置文件读取设置。首次启动时自动生成默认配置。示例：

```toml
[tcp]
host = "0.0.0.0"
port = 1883

[web]
host = "0.0.0.0"
port = 8081

[broker]
max_packet_size = 10485760    # 10 MB
max_qos = 2                    # ExactlyOnce
allow_anonymous = false
session_expiry_interval = 3600

[auth]
method = "file"                # "none" 或 "file"
auth_file = "passwd"

[web_auth]
enabled = true
username = "admin"
password = "admin"

[acl]
method = "file"                # "none" 或 "file"
acl_file = "acl.conf"
```

### 持久化存储

Broker 自动将以下数据持久化到 SQLite 数据库：

| 数据 | 表名 | 恢复时机 |
|------|------|----------|
| 会话信息 | `sessions` | Broker 启动时 |
| 主题订阅 | `subscriptions` | Broker 启动时 |
| 保留消息 | `retained_messages` | Broker 启动时 |
| 遗嘱消息 | `will_messages` | Broker 启动时 |

持久化采用 **异步批量写入** 策略：
- 事件通过 mpsc 通道发送到独立的后台写入任务
- 每 100ms 或累计 50 个事件触发一次批量事务写入
- Broker 关闭时自动 flush 所有待处理事件

---

## 开发

### 运行测试

```bash
# 运行所有单元测试
cargo test

# 运行单个 crate 测试
cargo test -p mqtt-broker
cargo test -p mqtt-core
```

### 调试模式

```bash
# 启用详细日志
RUST_LOG=mqtt_broker=debug,mqtt_web=debug cargo run -p mqtt-web
```

---

## 集成测试

项目根目录 `test/` 下提供了基于 Python 的集成测试脚本，覆盖认证、发布/订阅、保留消息、ACL 权限和离线消息队列等核心功能。

### 环境准备

```bash
# 安装依赖
pip install paho-mqtt
```

### 运行测试

先确保 Broker 已启动，然后运行：

```bash
cd rust_mqtt_broker
python test/test_mqtt.py
```

### 测试报告

| 类别 | 测试用例 | 预期 | 结果 |
|------|---------|------|------|
| **认证** | 正确凭据 (admin:admin123) 连接 | 成功 (rc=0) | [PASS] |
| | 错误密码 (wrongpass) 连接 | 拒绝 (rc=134) | [PASS] |
| | 错误用户名 (nobody) 连接 | 拒绝 (rc=134) | [PASS] |
| | 匿名连接 (无凭据) | 拒绝 (rc=135) | [PASS] |
| | 仅用户名无密码 | 拒绝 (rc=134) | [PASS] |
| **发布/订阅** | QoS 0 发布+订阅 | 消息可达 | [PASS] |
| | QoS 1 发布+订阅 | 消息可达 (至少一次) | [PASS] |
| | QoS 2 发布+订阅 | 消息可达 (恰好一次) | [PASS] |
| | 多级通配符 `#` 订阅 | 匹配多层主题 | [PASS] |
| | 单级通配符 `+` 订阅 | 匹配单层主题 | [PASS] |
| **保留消息** | 发布保留消息并接收 | 新订阅即收 | [PASS] |
| | 清除保留消息 | 清除后不再收到 | [PASS] |
| **ACL** | testuser 发布 test/ (ACL allow) | 消息可达 | [PASS] |
| | testuser 发布 secret/ (ACL deny) | 消息不可达 | [PASS] |
| | testuser 订阅 test/# (无权限) | 订阅被拒 | [PASS] |
| **离线队列** | clean_session=false + 离线消息 | 重连后收到 | [PASS] |

> 测试基于 `config.toml` 默认配置：认证方式 `method = "file"`，ACL 方式 `method = "file"`，密码文件 `passwd`，ACL 规则文件 `acl.conf`。

### 测试脚本说明

`test/test_mqtt.py` 使用 `paho-mqtt` 库编写，每个测试用例通过 `threading.Event` 实现异步等待，超时控制为 8 秒/用例。测试使用的用户凭据：

| 用户 | 密码 | 角色 |
|------|------|------|
| `admin` | `admin123` | 管理员（读写下放） |
| `testuser` | `testpass` | 仅允许发布 `test/#` |

---

## 许可证

[MIT](./LICENSE)

Copyright (c) 2026 AtomMQTT
