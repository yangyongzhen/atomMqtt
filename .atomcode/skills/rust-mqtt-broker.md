# Rust MQTT Broker — 项目指令模板

## 项目结构

```
Cargo.toml                          # Workspace 根
mqtt-core/                          # 协议层（编解码、包类型）
mqtt-broker/                        # 引擎层（状态、路由、持久化）
mqtt-web/                           # 展示层（REST API、WebSocket、前端）
mqtt-client/                        # 测试用 CLI 客户端
Doc/                                # 设计文档
Knowledge/                          # 知识库
config.toml                         # Broker 配置文件
acl.conf                            # ACL 规则文件
```

## 技术栈

| 层级 | 技术选型 |
|------|----------|
| 异步运行时 | tokio（tokio::select!, tokio::spawn） |
| Web 框架 | actix-web（actix_web::web::Data 共享状态） |
| 数据库 | rusqlite（WAL 模式，批量写入） |
| 并发容器 | DashMap（无锁读）、Mutex（SubscriptionTree） |
| 通道 | mpsc::unbounded_channel（内部消息传递） |
| 编解码 | bytes::BytesMut + 自定义 MqttFramedCodec |
| WebSocket | actix-web 内置 WebSocket |

## 核心架构模式

### BrokerState — 全局共享状态

```rust
pub struct BrokerState {
    pub sessions: DashMap<String, SessionState>,
    pub subscriptions: Mutex<SubscriptionTree>,
    pub retained: DashMap<String, RetainedMessage>,
    pub wills: DashMap<String, WillMessage>,
    pub connections: DashMap<String, UnboundedSender<Vec<u8>>>,
    pub web_subscribers: DashMap<String, UnboundedSender<String>>,
    pub metrics: Mutex<BrokerMetrics>,
    pub broker_handle: Mutex<Option<BrokerHandle>>,
    pub persistence: Arc<Persistence>,
    pub authenticator: Authenticator,
    pub acl: AclChecker,
}
```

> 所有连接处理器通过 `Arc<BrokerState>` 共享同一状态。

### 消息路由 — 后台单线程路由器

```
Publisher → mpsc::unbounded_channel → Background Router Loop → DashMap 投递
```

- 所有消息**统一进入后台路由器**，避免并发投递竞争
- TCP 订阅者收到 MQTT V311 PUBLISH 二进制包
- WebSocket 订阅者收到 JSON 字符串
- `BrokerMessage` 枚举：`Publish` + `ClientDisconnected`

### 持久化 — 异步批量写入

```
内存操作 → send(PersistEvent) → mpsc channel → bg_writer (批量 50 个或 100ms 定时)
```

- 主流程零等待，后台异步写入
- 事务批量提交，减少 I/O 次数
- `PersistEvent` 枚举定义所有持久化动作

### 订阅树 — Trie (前缀树)

```
TopicNode {
    children: Vec<(String, TopicNode)>,
    subscriptions: Vec<Subscription>,
}
```

- `#` 子节点 = 多级通配符，`+` 子节点 = 单级通配符
- `lookup(topic)` 同时收集精确匹配、`+`、`#` 三种路径

## 新增功能的通用流程

1. **添加持久化字段** → `persistence.rs` 的 `PersistEvent` + SQL 表/列 + `flush_all`
2. **添加 REST API** → `api.rs` 新函数 → `main.rs` 注册路由
3. **添加 WebSocket 消息类型** → `handle_ws_session` 的 `match` 分支
4. **添加 MQTT 包处理** → `server.rs` 的 `process_v3_packet` / `process_v5_packet`

## config.toml 规范

```toml
[tcp]
host = "0.0.0.0"
port = 1883

[web]
host = "0.0.0.0"
port = 8081

[broker]
max_packet_size = 10485760
max_qos = 2
allow_anonymous = true
session_expiry_interval = 3600

[auth]
method = "none"           # "none" | "file(path=passwd)"

[persistence]
path = "broker.db"        # 不配置 = 关闭持久化

[web_auth]
enabled = false
username = "admin"
password = "admin"

[acl]
method = "none"           # "none" | "file(path=acl.conf)"
```

## 日志规范

```rust
use tracing::{info, warn, error, debug};
```

默认 `RUST_LOG=info,mqtt_broker=debug`。
所有持久化错误用 `error!`，连接事件用 `info!`，包转发用 `debug!`。

## 测试约定

- 单元测试写在每个模块文件末尾的 `#[cfg(test)]` 模块中
- 使用 `#[test]` 而非 `#[tokio::test]`（纯逻辑无需异步）
- ACL / 订阅树 / 编解码等核心逻辑必须有测试
