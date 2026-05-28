# AtomMQTT 架构设计

> **版本**: 0.1.0  
> **更新**: 2025-06-01

---

## 1. 系统架构概览

```
┌────────────────────────────────────────────────────────────┐
│                      AtomMQTT Broker                       │
│                                                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │   mqtt-core   │    │  mqtt-broker  │    │   mqtt-web    │ │
│  │  (协议层)     │◄──►│  (引擎层)     │◄──►│  (展示层)    │ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
│                             │                               │
│                             ▼                               │
│                      ┌──────────────┐                      │
│                      │  mqtt-client  │                      │
│                      │  (测试工具)   │                      │
│                      └──────────────┘                      │
└────────────────────────────────────────────────────────────┘
```

### 层次说明

| 层次 | Crate | 职责 |
|------|-------|------|
| **协议层** | `mqtt-core` | MQTT 3.1.1 / 5.0 协议编解码、类型定义 |
| **引擎层** | `mqtt-broker` | 连接管理、订阅树、消息路由、会话状态、SQLite 持久化 |
| **展示层** | `mqtt-web` | Web 管理界面、REST API、WebSocket 端点 |
| **测试层** | `mqtt-client` | CLI 发布/订阅/Shell 测试工具 |

---

## 2. 核心数据流

### 2.1 消息发布与投递

```
发布者 (TCP Client / Web API)
       │
       │ PUBLISH (TCP) 或 POST /api/publish (HTTP)
       ▼
┌──────────────────┐
│  handle_connection │  (或 api::publish_message)
│  解码 → 验证      │
│  → BrokerMessage   │
└────────┬─────────┘
         │
         │ BrokerMessage::Publish (mpsc 通道)
         ▼
┌──────────────────┐
│  后台路由循环      │
│  1. lookup() 查找  │
│     订阅者         │
│  2. 编码为 V311    │
│     PUBLISH 包     │
│  3. 遍历 connections│
│     DashMap 投递   │
│  4. 遍历 web_      │
│     subscribers    │
│     DashMap 投递   │
└──────────────────┘
         │
         ├──────────────────┐
         ▼                  ▼
┌──────────────────┐ ┌──────────────────┐
│  TCP 订阅者       │ │  WebSocket 订阅者  │
│  (MQTT Client)   │ │  (浏览器)         │
│  收到 V311        │ │  收到 JSON 消息   │
│  PUBLISH 包       │ │                   │
└──────────────────┘ └──────────────────┘
```

### 2.2 WebSocket 订阅流程

```
浏览器                              Web Broker
   │                                     │
   │──── WS connect /ws/subscribe ──────►│
   │                                     │── 注册 web_subscribers 通道
   │◄──── {"type":"subscribed",...}──────│
   │                                     │
   │──── {"type":"subscribe",            │
   │       "topic_filter":"test/#"}─────►│── 调用 subscription.subscribe()
   │◄─── {"type":"subscribed",...}───────│
   │                                     │
   │  (MQTT 客户端发布消息后...)          │
   │◄─── {"type":"publish",              │
   │       "topic":"test/hello",         │
   │       "payload":"Hello",...}────────│
   │                                     │
   │──── {"type":"unsubscribe",          │
   │       "topic_filter":"test/#"}─────►│── 调用 subscription.unsubscribe()
   │                                     │
   │──── (连接断开) ────────────────────►│── 清理订阅和通道
```

---

## 3. 模块详解

### 3.1 mqtt-core — 协议层

```
mqtt-core/src/
├── lib.rs              # 模块导出
├── common.rs           # 共享类型：QoS, TopicFilter, ProtocolVersion
├── codec.rs            # 编解码器 trait 定义
├── v3/
│   ├── mod.rs          # MQTT 3.1.1 模块
│   ├── types.rs        # MQTT 3.1.1 包类型 (ConnectPacket, PublishPacket, ...)
│   └── codec.rs        # MQTT 3.1.1 编码/解码
└── v5/
    ├── mod.rs          # MQTT 5.0 模块
    ├── types.rs        # MQTT 5.0 包类型（含属性）
    ├── codec.rs        # MQTT 5.0 编码/解码
    └── properties.rs   # 5.0 属性定义
```

**关键设计**：
- `MqttFramedCodec` 实现 tokio-util 的 `Decoder`/`Encoder` trait
- `MqttPacket` 枚举统一表示 V311 和 V5 两种版本的包
- 剩余长度使用 MQTT 标准 Variable Byte Integer 编码
- 解码器使用 `BytesMut` 零拷贝切片

### 3.2 mqtt-broker — 引擎层

```
mqtt-broker/src/
├── persistence.rs     # SQLite 持久化存储（异步批量写入）
├── lib.rs              # BrokerState, BrokerMessage, BrokerHandle
├── server.rs           # TCP 监听、连接处理、消息路由
├── config.rs           # BrokerConfig 配置
├── session.rs          # SessionState 会话状态
├── subscription.rs     # SubscriptionTree 订阅树 (Trie)
├── retention.rs        # RetainedMessage 保留消息
├── will.rs             # WillMessage 遗嘱消息
├── metrics.rs          # BrokerMetrics 性能指标
└── auth.rs             # Authenticator 认证授权
```

#### BrokerState (lib.rs)

全局共享状态，通过 `Arc<BrokerState>` 在所有任务间共享：

| 字段 | 类型 | 说明 |
|------|------|------|
| `config` | `BrokerConfig` | 配置（只读，从 `config.toml` 加载） |
| `sessions` | `DashMap<String, SessionState>` | 活跃会话 |
| `subscriptions` | `Mutex<SubscriptionTree>` | 订阅树 |
| `retained` | `DashMap<String, RetainedMessage>` | 保留消息 |
| `wills` | `DashMap<String, WillMessage>` | 遗嘱消息 |
| `metrics` | `Mutex<BrokerMetrics>` | 指标 |
| `persistence` | `Arc<Persistence>` | SQLite 持久化层 |
| `broker_handle` | `Mutex<Option<BrokerHandle>>` | 后台路由器句柄 |
| `connections` | `DashMap<String, UnboundedSender<Vec<u8>>>` | TCP 连接通道 |
| `web_subscribers` | `DashMap<String, UnboundedSender<String>>` | WebSocket 订阅通道 |

#### 连接生命周期 (server.rs)

```
1. TCP 连接到达 → accept()
2. handle_connection() 启动:
   a. 读取第一个包 (CONNECT)
   b. decode_first_packet() 检测协议版本 (v311/v5)
   c. 认证 → 创建 SessionState → 持久化 SaveSession
   d. 发送 CONNACK
   e. 创建 mpsc 通道 → 存入 connections DashMap
   f. 进入主循环:
      - select! 等待: TCP 数据 | 内部转发消息
      - 收到 TCP 数据: 解码 → process_packet()
      - 收到内部转发: 编码 → 写入 TCP 流
   g. 每个状态变更（订阅/取消订阅/发布 retain/disconnect）发送持久化事件
3. 断开:
   a. 从 connections 移除
   b. 发送 BrokerMessage::ClientDisconnected
   c. 后台路由器根据 clean_session 决定:
      - true: 清理订阅、会话，发送 RemoveXxx 持久化事件
      - false: 标记会话断连，保留数据
   d. 发送遗嘱消息 (如有) → 持久化 RemoveWill
```

### 3.3 SubscriptionTree — 主题订阅树

基于 Trie（前缀树）的高效主题匹配实现。

```
                    root
                     │
         ┌──────┬───┼───┬──────┐
         │      │   │   │      │
       sensor  home  +  $SYS  #
         │      │   │
      ┌──┼──┐   │   │
      │  │  │   │   │
    temp + humidity #
```

**匹配算法**：
1. 精确匹配：逐段比较 topic 层级
2. `+` 通配符：匹配任意单个层级
3. `#` 通配符：匹配剩余所有层级（必须在末尾）
4. 结果去重：通过 `(client_id, filter)` 元组 HashSet 去重

### 3.4 mqtt-web — 展示层

```
mqtt-web/src/
├── main.rs         # 入口：启动 Broker + Web 服务器
├── api.rs          # REST API 处理器 + WebSocket 处理
└── models.rs       # 响应模型
```

> 前端静态文件（HTML / CSS / JS）在编译时通过 `include_dir!` 宏直接嵌入到二进制中，运行时无需读取磁盘。
> 无需额外的静态文件中间件依赖，生成单文件 `.exe` 即可部署。

#### 嵌入式静态文件

前端的 `index.html`、`dashboard.css`、`dashboard.js` 等文件在 `build.rs` 阶段被编译到 `mqtt-web` 的二进制中。一条通配路由 `/{path:.*}` 指向 `serve_embedded_file` 函数：

```rust
static STATIC_DIR: Dir<'_> = include_dir!("mqtt-web/static");

async fn serve_embedded_file(req: actix_web::HttpRequest) -> HttpResponse {
    let path = req.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    match STATIC_DIR.get_file(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream().to_string();
            HttpResponse::Ok().content_type(mime).body(file.contents())
        }
        None => HttpResponse::NotFound().body("404 Not Found"),
    }
}
```

优点：
- **单文件分发** — 一个 `.exe` 包含全部前端，放到任何目录都可直接运行
- **零 I/O 等待** — 不走磁盘读取，性能恒定
- **Windows 兼容** — 规避了 Windows 下 actix-files 的 0 字节 Bug
- **自动 MIME** — 通过 `mime_guess` 根据文件扩展名自动识别 Content-Type
- **添加文件无需改代码** — 往 `static/` 目录加任何文件，`include_dir!` 自动嵌入

#### REST API 路由

| 路由 | 处理器 | 说明 |
|------|--------|------|
| `GET /api/metrics` | `get_metrics` | 指标快照 |
| `GET /api/broker/info` | `get_broker_info` | Broker 信息 |
| `GET /api/clients` | `get_clients` | 客户端列表 |
| `GET /api/clients/{id}` | `get_client_detail` | 客户端详情 |
| `GET /api/subscriptions` | `get_subscriptions` | 订阅列表 |
| `GET /api/retained` | `get_retained_messages` | 保留消息 |
| `POST /api/login` | `login` | 用户登录（JWT 认证） |
| `POST /api/publish` | `publish_message` | 发布消息 |
| `POST /api/clients/{id}/disconnect` | `disconnect_client` | 断开客户端 |
| `DELETE /api/retained/{topic}` | `delete_retained` | 删除指定保留消息 |
| `GET /ws/subscribe` | `ws_subscribe` | WebSocket 订阅 |
| `GET /mqtt` | `ws_mqtt_bridge` | WebSocket MQTT 桥接（直接 MQTT 协议代理） |

---

## 4. 关键设计决策

### 4.1 为什么用 DashMap 而非 HashMap + RwLock？

- DashMap 使用内部分片锁，高并发场景下性能优于全局锁
- Broker 需要频繁读写 `connections` / `sessions` / `web_subscribers`
- DashMap 条目级锁避免了大粒度的互斥

### 4.2 为什么用 mpsc 通道而非共享队列？

- 异步 mpsc 天然适配 `tokio::select!` 模式
- 后台路由器是一个独立的任务，通过通道接收消息
- `UnboundedSender` 避免反压导致的路由阻塞

### 4.3 为什么后台路由使用 V311 统一编码？

- 简化投递逻辑：不在路由层维护版本兼容性
- V311 是广泛兼容的子集，绝大数 MQTT 客户端支持
- 节省编码资源：不需要为每个订阅者单独编码

### 4.4 为什么订阅树用 Trie 而不是哈希匹配？

- Trie 支持高效的前缀匹配和通配符扩展
- 时间复杂度 O(k)，k 为主题层级深度
- 易于实现 `+` 和 `#` 通配符匹配

---

## 5. SQLite 持久化存储

### 5.1 设计目标

- **数据不丢失**：Broker 重启后自动恢复所有状态（会话、订阅、保留消息、遗嘱消息）
- **热路径零开销**：内存操作不受持久化影响，所有持久化通过异步通道完成
- **优雅关闭**：SIGTERM 时自动 flush 所有待处理数据

### 5.2 架构

```
┌──────────────────────────────────────────────────┐
│                热路径（内存操作）                    │
│  DashMap / Mutex  ←  快速读写，无同步等待           │
└──────────────────────┬───────────────────────────┘
                       │
          PersistEvent (mpsc::UnboundedSender)
                       ▼
┌──────────────────────────────────────────────────┐
│             后台写入任务 (bg_writer)                │
│                                                   │
│  ┌─────────────────────────────────────────┐      │
│  │  批量队列 (batch: Vec<PersistEvent>)    │      │
│  │  触发策略: ┌── 50个事件                  │      │
│  │            └── 100ms 定时器              │      │
│  └──────────────────┬──────────────────────┘      │
│                      ▼                            │
│  ┌─────────────────────────────────────────┐      │
│  │  SQLite 事务批量写入 (BEGIN...COMMIT)    │      │
│  └─────────────────────────────────────────┘      │
└──────────────────────────────────────────────────┘
                       │
                       ▼
              broker.db (WAL 模式)
              ├── sessions
              ├── subscriptions
              ├── retained_messages
              └── will_messages
```

### 5.3 数据库表结构

```sql
CREATE TABLE IF NOT EXISTS sessions (
    client_id       TEXT PRIMARY KEY,
    protocol_version INTEGER NOT NULL,
    clean_session   INTEGER NOT NULL DEFAULT 1,
    keep_alive      INTEGER NOT NULL DEFAULT 60,
    username        TEXT
);

CREATE TABLE IF NOT EXISTS subscriptions (
    client_id   TEXT NOT NULL,
    filter      TEXT NOT NULL,
    qos         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (client_id, filter)
);

CREATE TABLE IF NOT EXISTS retained_messages (
    topic   TEXT PRIMARY KEY,
    payload BLOB,
    qos     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS will_messages (
    client_id       TEXT PRIMARY KEY,
    topic           TEXT NOT NULL,
    payload         BLOB,
    qos             INTEGER NOT NULL DEFAULT 0,
    retain          INTEGER NOT NULL DEFAULT 0,
    delay_interval  INTEGER NOT NULL DEFAULT 0
);
```

### 5.4 持久化事件类型

| 事件 | 触发时机 | SQL 操作 |
|------|----------|----------|
| `SaveSession` | CONNECT 处理完成 | INSERT OR REPLACE INTO sessions |
| `RemoveSession` | clean_session=true 断开 | DELETE FROM sessions |
| `SaveSubscription` | SUBSCRIBE 处理完成 | INSERT OR REPLACE INTO subscriptions |
| `RemoveSubscription` | UNSUBSCRIBE 处理完成 | DELETE FROM subscriptions |
| `RemoveClientSubscriptions` | 客户端断开 (clean) | DELETE FROM subscriptions WHERE client_id=? |
| `SaveRetained` | PUBLISH with retain | INSERT OR REPLACE INTO retained_messages |
| `RemoveRetained` | PUBLISH with retain & zero-length payload | DELETE FROM retained_messages |
| `SaveWill` | CONNECT 携带 Will 信息 | INSERT OR REPLACE INTO will_messages |
| `RemoveWill` | 遗嘱已投递 / 正常断开 | DELETE FROM will_messages |
| `Shutdown` | Broker 收到 SIGTERM | 触发 flush 后退出 |

### 5.5 启动恢复流程

```
Broker 启动
    │
    ├── 1. Persistence::new() → 打开/创建 broker.db (WAL 模式)
    │
    ├── 2. persistence.load_sessions()
    │       └── state.sessions.insert() 恢复会话
    │
    ├── 3. persistence.load_subscriptions()
    │       └── state.subscriptions.lock().subscribe() 恢复订阅
    │
    ├── 4. persistence.load_retained()
    │       └── state.retained.insert() 恢复保留消息
    │
    ├── 5. persistence.load_wills()
    │       └── state.wills.insert() 恢复遗嘱消息
    │
    └── 6. 启动 bg_writer 后台任务 (从 mpsc 接收事件)
```

---

## 6. 安全设计

- `#![deny(unsafe_code)]` — 全程无 unsafe Rust
- 包大小限制 — `max_packet_size` 配置防 DoS
- 认证机制 — 支持匿名、文件密码认证和 JWT 令牌认证
- ACL 访问控制 — 支持基于客户端 ID 和用户名的发布/订阅权限控制，可在 `config.toml` 中配置 ACL 规则，限制特定主题的访问范围
- 会话过期 — Keep Alive 超时检测

---

## 7. 性能考虑

- 全异步 I/O（Tokio）
- 零拷贝编解码（BytesMut）
- DashMap 无锁并发访问
- 后台路由单线程避免竞争
- 连接通道解耦：每个 TCP 连接独立 mpsc 通道
