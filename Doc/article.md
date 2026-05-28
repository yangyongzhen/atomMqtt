# AtomMQTT Broker — 原理与实现

> **摘要**：本文深入剖析 AtomMQTT Broker 的设计原理与 Rust 实现细节，涵盖协议编解码、异步架构、订阅树、消息路由、SQLite 持久化及 Web 管理界面。适合对 MQTT 协议、异步 Rust 和中间件开发感兴趣的读者。

---

## 1. 概述

AtomMQTT Broker 是一个纯 Rust 实现的 MQTT 3.1.1/5.0 消息代理，核心目标是在**高性能**与**数据安全**之间取得平衡。项目采用四层架构：

| 层 | Crate | 职责 |
|---|-------|------|
| 协议层 | `mqtt-core` | MQTT 协议编解码与类型系统 |
| 引擎层 | `mqtt-broker` | 连接管理、订阅树、路由、持久化 |
| 展示层 | `mqtt-web` | Web 管理界面、REST API、WebSocket |
| 客户端 | `mqtt-client` | CLI 测试工具 |

架构的核心设计哲学是：

- **热路径（hot path）纯内存操作** — 所有消息路由、订阅匹配在内存中完成，无锁竞争
- **冷路径（cold path）异步批量写入** — 持久化通过 mpsc 通道异步写入 SQLite，不阻塞主逻辑
- **零拷贝与异步 I/O** — 利用 Tokio 运行时和 BytesMut 实现高效网络处理

---

## 2. MQTT 协议编解码

### 2.1 协议结构

MQTT 协议包由三部分组成：

```
[固定头] [可变头] [载荷]
  └── 1字节控制类型 + 剩余长度编码
```

固定头是 MQTT 最巧妙的设计之一。第一个字节的高 4 位标识包类型（CONNECT=1, PUBLISH=3, SUBSCRIBE=8 等），剩余长度使用 **Variable Byte Integer** 编码——每字节 7 位数据 + 1 位延续标志，最大支持 268 MB 的包。

### 2.2 解码器实现

在 `mqtt-core/src/codec.rs` 中，实现了一个面向流的 `Decoder` trait：

```rust
impl Decoder for MqttFramedCodec {
    type Item = MqttPacket;
    type Error = MqttError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<MqttPacket>> {
        // 1. 至少 2 字节才能读固定头
        // 2. 解码剩余长度，计算总包长
        // 3. 等待完整包到达（`src.remaining() < packet_len` → None）
        // 4. 调用 decode_packet() 按类型分发
    }
}
```

**为什么返回 `Result<Option<T>>` 而非 `Result<T>`？** 这是 Tokio 框架的要求——当数据不足时返回 `None`，框架会自动等待更多数据到达后再调用。这是**零拷贝粘包处理**的典型模式。

### 2.3 MQTT 3.1.1 vs 5.0 的差异处理

两种协议版本共存在一个代码库中。解码时通过剩余长度后的第一个字节来区分：

```rust
pub fn decode_first_packet(src: &mut BytesMut) -> Result<(ProtocolVersion, MqttPacket)> {
    let protocol_name = &src[..];
    if protocol_name.starts_with(b"\x00\x04MQTT") { Ok((V311, ...)) }
    else if protocol_name.starts_with(b"\x00\x05MQTT") { Ok((V5, ...)) }
    else { Err(MqttError::InvalidProtocol) }
}
```

V5 相较于 V311 增加了 Properties（属性）系统，用于传递会话过期、用户属性、订阅标识符等元数据。代码中通过 `ProtocolVersion` 枚举进行条件编译式的路由。

---

## 3. 核心状态管理

### 3.1 BrokerState — 全局共享状态

整个 Broker 的状态集中在 `BrokerState` 中，通过 `Arc<BrokerState>` 在所有异步任务间共享：

```rust
pub struct BrokerState {
    pub config: BrokerConfig,                          // 只读配置
    pub sessions: DashMap<String, SessionState>,       // 会话
    pub subscriptions: Mutex<SubscriptionTree>,         // 订阅树
    pub retained: DashMap<String, RetainedMessage>,    // 保留消息
    pub wills: DashMap<String, WillMessage>,           // 遗嘱消息
    pub metrics: Mutex<BrokerMetrics>,                 // 性能指标
    pub persistence: Arc<Persistence>,                 // SQLite 持久化
    pub broker_handle: Mutex<Option<BrokerHandle>>,    // 后台句柄
    pub connections: DashMap<String, UnboundedSender<Vec<u8>>>,   // TCP 连接
    pub web_subscribers: DashMap<String, UnboundedSender<String>>, // WS 订阅
}
```

这里有一个重要的设计取舍：**为什么同时使用 `DashMap` 和 `Mutex`？**

- `DashMap`（分片锁）：适合**高频读取、低竞争写入**的场景，如 `sessions`、`connections`
- `Mutex<SubscriptionTree>`：订阅树的修改（insert/remove）需要整个树的遍历一致性

### 3.2 订阅树（SubscriptionTree）

订阅树是基于 **Trie（前缀树）** 的实现，每一层对应主题中的一个层级：

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

**匹配算法** 是递归的层级遍历：

```rust
fn match_topic(&self, topic: &str) -> HashSet<(String, u8)> {
    // 1. 按 '/' 分割 topic 为 segment 数组
    // 2. 从根节点开始逐层匹配
    // 3. 三种匹配模式：
    //    - 精确匹配：segment 相等
    //    - '+' 通配符：跳过当前层
    //    - '#' 通配符：匹配所有剩余层级（必须在末尾）
}
```

**时间复杂度**：`O(k)`，其中 `k` 是主题的层级深度。相比哈希匹配的全量扫描，Trie 在大规模订阅场景下优势明显。

### 3.3 去重机制

MQTT 允许多个客户端用相同的主题过滤器订阅。在投递消息时，如果树中不同分支都匹配到同一个客户端（例如 `foo/+` 和 `+/bar` 同时匹配 `foo/bar`），需要去重：

```rust
// lookup() 返回的是 HashSet<(client_id, qos)>
// HashSet 天然去重，后续遍历时保留最高 QoS
```

---

## 4. 消息路由机制

### 4.1 架构

消息路由采用**显式后台路由器模式**，而非在连接处理任务中直接投递。所有 TCP 连接处理器通过 `mpsc::UnboundedSender` 向后台路由器发送 `BrokerMessage`：

```
Client A (PUBLISH) ──→ 后台路由器 ──→ Client B (SUBSCRIBER)
                              │
                         [订阅树 lookup]
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                 connections DashMap + web_subscribers
```

### 4.2 为什么使用后台路由器？

| 方案 | 问题 |
|------|------|
| 直接在连接中投递 | 需要获取 `connections` 锁，可能阻塞 |
| 每个连接广播 | 需要每个连接持有所有其他连接的句柄 |
| **后台路由器** | 单线程处理，无竞争，连接与路由解耦 |

后台路由器运行在一个独立的 Tokio 任务中，其主循环如下：

```rust
loop {
    tokio::select! {
        Some(msg) = rx.recv() => { handle_message(msg); }
        _ = &mut flush_timer => { /* 定时 flush 无操作 */ }
    }
}
```

### 4.3 消息投递路径

当后台路由器收到 `BrokerMessage::Publish` 时：

1. **订阅查找**：`subscription_tree.lookup(topic)` 获取所有匹配的 `(client_id, qos)`
2. **统一编码**：将所有投递消息统一编码为 MQTT 3.1.1 格式（向后兼容）
3. **TCP 投递**：遍历 `connections` DashMap，通过每个连接的 `UnboundedSender<Vec<u8>>` 投递
4. **WebSocket 投递**：遍历 `web_subscribers` DashMap，投递 JSON 格式消息
5. **保留消息**：如果 PUBLISH 的 retain 标志为 true，存入 `retained` DashMap

**有趣的事实**：Web 管理界面的"订阅消息"功能也是通过后台路由器实现的。浏览器通过 WebSocket 发送 JSON 命令（`{"type":"subscribe","topic_filter":"test/#"}`），API 处理器调用 `subscription_tree.subscribe()`，然后后台路由器就会向该 WebSocket 连接投递匹配的消息。

---

## 5. SQLite 持久化存储

### 5.1 设计原则

持久化系统遵循三条核心原则：

| 原则 | 含义 |
|------|------|
| **热路径零开销** | 内存操作完全不等待持久化完成 |
| **最终一致性** | 允许 Broker 崩溃时丢失最近 <100ms 的数据 |
| **幂等操作** | 所有 SQL 使用 INSERT OR REPLACE / DELETE |

### 5.2 架构

```
┌──────────────────────┐
│    内存数据结构       │ ← 主流程操作这里，零等待
│  DashMap / Mutex     │
└─────────┬────────────┘
          │ PersistEvent (mpsc::UnboundedSender)
          ▼
┌──────────────────────┐
│    后台写入任务        │
│                      │
│  触发策略:            │
│    ┌─ 50 个事件       │
│    └─ 100ms 定时器    │
│                      │
│  BEGIN TRANSACTION   │
│   批量执行 SQL        │
│  COMMIT              │
└─────────┬────────────┘
          ▼
    broker.db (WAL 模式)
```

`Persistence` 结构体持有 `mpsc::UnboundedSender<PersistEvent>`，所有状态变更点调用其 `send()` 方法。例如：

```rust
// 订阅成功后
state.persistence.send(PersistEvent::SaveSubscription {
    client_id: client_id.clone(),
    filter: topic_filter.clone(),
    qos: qos as u8,
});
```

### 5.3 批量写入优化

后台 writer 使用 `tokio::select!` 在两种触发条件间竞争：

```rust
loop {
    tokio::select! {
        // 条件 1：50 个事件积压时立即触发
        event = rx.recv() => {
            batch.push(event);
            if batch.len() >= 50 { flush_batch(&db, &batch); batch.clear(); }
        }
        // 条件 2：100ms 定时器触发
        _ = &mut debounce => {
            if !batch.is_empty() { flush_batch(&db, &batch); batch.clear(); }
        }
    }
}
```

这种设计在**高吞吐**下以事件数触发（50个一批），在**低负载**下以时间触发（最多延迟 100ms）。

### 5.4 启动恢复

Broker 启动时，按此顺序从 SQLite 恢复状态：

```
启动 → 打开 broker.db (WAL) → 1. 恢复 sessions → 2. 恢复 subscriptions
    → 3. 恢复 retained → 4. 恢复 wills → 5. 启动后台 writer
```

恢复后，旧会话保持 `clean_session=false` 状态，当客户端重新连接时自动恢复其订阅。

### 5.5 为什么会话也要持久化？

MQTT 规范要求 `clean_session=false` 的客户端断开后，其订阅在 Broker 端保持有效。如果不持久化会话，Broker 重启后这些订阅就丢失了，客户端重连后需要重新订阅。对于 IoT 场景中大量"发布但不订阅"的传感器节点，会话持久化保证了**断连重连的透明性**。

---

## 6. Web 管理界面

### 6.1 分层设计

Web 管理界面是典型的 **Server-Side API + Client-Side SPA** 模式：

```
                    ┌─────────────────┐
                    │   index.html     │
                    │   dashboard.js   │  ← 前端 SPA
                    │   dashboard.css  │
                    └────────┬────────┘
                             │ HTTP / WebSocket
                    ┌────────▼────────┐
                    │   api.rs        │  ← Actix-Web 处理器
                    │   models.rs     │
                    └────────┬────────┘
                             │ BrokerMessage (mpsc)
                    ┌────────▼────────┐
                    │   BrokerState   │  ← 引擎层
                    └─────────────────┘
```

### 6.2 REST API 与 WebSocket 的消息路径对比

| 操作 | 路径 | 是否持久化 |
|------|------|-----------|
| TCP SUBSCRIBE | `server.rs → subscription.subscribe()` | ✅ |
| WS JSON subscribe | `api.rs → subscription.subscribe()` | ❌ (WS 是临时订阅) |
| TCP PUBLISH | `server.rs → BrokerMessage::Publish` | ✅ (仅 retain) |
| HTTP POST /api/publish | `api.rs → BrokerMessage::Publish` | ✅ (仅 retain) |

**为什么 WS 订阅不持久化？** WebSocket 订阅者通常是浏览器中打开的临时页面，关闭后自然消失。如果持久化，Broker 重启后会保留过时的浏览器订阅，导致不必要的消息发送。

### 6.3 前端数据流

前端 `dashboard.js` 使用原生 Fetch API + DOM 操作（无框架依赖），每 2 秒轮询 `/api/metrics` 刷新仪表盘，订阅和客户端列表则在页面切换时按需加载。出于安全考虑，前端通过 JavaScript **路径拼接**而非用户输入构造 WebSocket URL，避免 XSS 注入。

---

## 7. 安全与可靠性

### 7.1 内存安全

整个项目使用 `#![deny(unsafe_code)]` 确保零 unsafe Rust 代码。这意味着所有内存安全由 Rust 编译器保证——没有空指针解引用、缓冲区溢出或释放后使用。

### 7.2 DoS 防护

| 防护措施 | 配置项 | 默认值 |
|----------|--------|--------|
| 最大包大小 | `max_packet_size` | 10 MB |
| Keep Alive 超时 | 协议字段 | 客户端声明 |
| 连接数限制 | 无硬限制（依赖 OS） | — |
| ACL 访问控制 | `acl_file` | 允许所有（无认证时） |

### 7.3 优雅关闭

Broker 注册了 SIGTERM 和 SIGINT 信号处理器：

```rust
tokio::signal::ctrl_c().await?;
state.persistence.send(PersistEvent::Shutdown);
// 后台 writer 收到 Shutdown 后 flush 所有待处理事件
```

确保 Broker 关闭时不会丢失最近 <100ms 的状态变更。

### 7.4 ACL Topic 访问控制

AtomMQTT 支持基于文件配置的 ACL（Access Control List）规则，用于限制特定客户端对 Topic 的发布与订阅权限。

**ACL 配置文件格式**：

```
# 用户规则
user alice
topic write sensor/#
topic read home/+/temperature

user bob
topic readwrite garden/#

# 匿名规则
user anonymous
topic read $SYS/#

# 默认拒绝
topic deny all
```

**权限类型**：

| 权限 | 含义 |
|------|------|
| `read` | 允许订阅该主题（SUBSCRIBE） |
| `write` | 允许发布该主题（PUBLISH） |
| `readwrite` | 同时允许订阅和发布 |

**匹配逻辑**：
1. Broker 优先匹配客户端用户名对应的 `user` 规则块
2. 若未匹配到用户名规则，则匹配 `user anonymous` 匿名规则
3. ACL 规则中的主题支持 `+` 和 `#` 通配符，与 MQTT 订阅语义一致
4. 规则遵循**首次匹配**原则——一旦匹配到允许或拒绝规则，立即生效
5. 若没有任何规则匹配，则执行**默认拒绝**策略（Deny All）

**文件配置路径**：通过 Broker 配置项 `acl_file` 指定 ACL 文件路径。若未配置此选项，默认允许所有操作（兼容无认证模式）。

```rust
// ACL 检查核心逻辑
fn check_acl(acl: &AclConfig, client_id: &str, topic: &str, 
             access: AccessKind) -> Result<(), AclError> {
    // 1. 查找匹配的用户规则
    // 2. 在规则中对 topic 进行通配符匹配
    // 3. 首次匹配命中则返回 Allow/Deny
    // 4. 无匹配则默认拒绝
}
```

**应用场景**：
- 多租户场景下隔离不同用户的 Topic 空间
- 系统主题 `$SYS/#` 只允许特定管理客户端发布
- 传感器数据只写不读，控制指令只读不写

---

## 8. 性能考虑

### 8.1 为什么不用全局锁？

传统方案使用 `RwLock<HashMap<...>>`，但读操作多时写操作会被饿死。DashMap 将 map 分片为多个 shard，每个 shard 有独立的锁，读写不同 key 时可以并行。

### 8.2 为什么消息路由不用广播？

一种简单方案是让每个连接持有所有其他连接的 Sender，收到消息后直接向所有连接广播。但这样：

- 每个连接都要遍历所有订阅者
- 连接创建/销毁时需要更新所有连接的订阅表
- 消息会重复发送（发给"自己"）

后台路由器方案避免了这些问题——单线程处理，路由逻辑集中。

### 8.3 为什么用 UnboundedSender？

`UnboundedSender`（无界通道）不会对发送者施加反压。选择它的理由是：

- 消息路由失败（队列满）比延迟更糟糕——宁可占用更多内存也不要丢消息
- 路由器的消费速度通常远快于生产速度
- MQTT 消息通常较短（KB 级别），内存压力可控

---

## 9. 总结

AtomMQTT Broker 的设计体现了 Rust 在中间件领域的独特优势：

| 方面 | Rust 的优势 |
|------|------------|
| **并发安全** | 所有权模型 + Send/Sync trait 保证无数据竞争 |
| **零拷贝** | BytesMut + 引用计数，避免不必要的内存复制 |
| **异步生态** | Tokio 提供了完整的异步运行时和网络栈 |
| **FFI 零成本** | rusqlite 通过直接链接 libsqlite3，无 JNI/FFI 开销 |

对于想进一步了解完整实现的读者，代码库中每个模块都有详细的 Rustdoc 注释：

- `mqtt-broker/src/persistence.rs` — SQLite 持久化完整实现（~450 行）
- `mqtt-core/src/common.rs` — 主题匹配和 QoS 类型系统
- `mqtt-broker/src/subscription.rs` — 订阅树 Trie 实现

---

> **作者**: AtomMQTT Team  
> **版本**: 0.1.0  
> **仓库**: <repo-url>  
> **许可**: MIT
