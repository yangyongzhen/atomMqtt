# 架构决策知识库

## 为什么使用 DashMap 而非 HashMap + RwLock？

**决策**：并发读多的 map 用 `DashMap`（如 `sessions`, `connections`, `web_subscribers`）

**理由**：
- DashMap 内部对 key 做 hash 分片（shard），每个 shard 独立加锁
- 读操作无需阻塞其他 shard，高并发场景吞吐远高于全局 `RwLock<HashMap>`
- 适用场景：频繁的 `get()` / `insert()` / `remove()` 操作

**例外**：`subscriptions` 使用 `Mutex<SubscriptionTree>`，因为 lookup 需要遍历整个 Trie 树，不适合分片。

## 为什么用 mpsc 通道而非共享队列？

**决策**：所有跨任务通信使用 `tokio::sync::mpsc::unbounded_channel`

**理由**：
- 每个 TCP 连接和 WebSocket 连接都是一个独立 async task
- mpsc 通道让 producer（后台路由器）和 consumer（连接 task）解耦
- `DashMap<client_id, Sender>` 模式：路由器按 client_id 查找通道并投递
- 通道的 `send()` 返回 `Err` 天然标记断连，无需额外心跳

## 为什么后台路由使用单线程模型？

**决策**：消息路由在单条 `tokio::spawn` 的 async task 中顺序处理

**理由**：
- 避免并发投递的锁竞争（所有投递走同一通道）
- 消息顺序保证（同一主题的消息按发布顺序投递）
- 简化订阅匹配：不需要对 SubscriptionTree 做并发读

## 为什么用 V311 统一编码转发？

**决策**：后台路由器将消息编码为 MQTT 3.1.1 PUBLISH 包投递给 TCP 订阅者

**理由**：
- V311 格式比 V5 简单，编解码开销更低
- MQTT 5.0 客户端必须兼容 V311 的下行包（协议规范允许）
- 路由器不需要关心客户端协议版本，统一投递

## 为什么订阅树用 Trie 而非哈希匹配？

**决策**：`SubscriptionTree` 使用 Trie（前缀树）结构

**理由**：
- 支持 `+`（单级通配符）和 `#`（多级通配符）的高效匹配
- 新增/移除订阅是 O(depth) 而非 O(N)
- 通配符匹配与 Trie 遍历天然契合
- 哈希匹配需要对每个订阅做通配符展开，无法复用

## 为什么持久化用异步批量写入？

**决策**：`mpsc::UnboundedSender<PersistEvent>` + 后台 `bg_writer` 批量刷入 SQLite

**理由**：
- 主流程（消息路由、连接处理）不受磁盘 I/O 阻塞
- 批量事务（50 条或 100ms 定时器）降低 I/O 次数 50-100 倍
- SQLite WAL 模式：读不阻塞写，写不阻塞读
- 事件溯源模式（event sourcing）：状态变更→事件→持久化，方便调试恢复

## 为什么用 SQLite 而非专用 MQTT 数据库？

**决策**：使用 SQLite 做持久化存储

**理由**：
- 嵌入式，零配置，无需额外进程
- 单文件，备份迁移简单
- 足够支撑 IoT 场景（几千客户端、几万订阅）
- Rust 的 `rusqlite` 库提供编译时 SQL 检查

## 为什么 Session 也要持久化？

**决策**：非 clean_session 的客户端会话需要持久化到 SQLite

**理由**：
- 支持 MQTT 持久会话语义（clean_session=false）
- Broker 重启后恢复客户端状态，无需客户端重订阅
- 与订阅、保留消息、遗嘱消息一起构成完整的 MQTT 持久化方案

## 为什么用 include_dir! 嵌入静态文件？

**决策**：前端 HTML/CSS/JS 通过 `include_dir!` 编译进二进制文件

**理由**：
- 部署只需一个 exe 文件，无外部依赖
- 避免跨域问题（无需独立静态文件服务器）
- 版本对齐：前端代码与后端 API 同步更新

## 为什么选择 actix-web 而非 axum？

**决策**：使用 actix-web 作为 Web 框架

**理由**：
- 成熟的 WebSocket 支持（`actix-web-actors` 或 `actix-ws`）
- `web::Data` 与 actix 的 App 状态管理整合良好
- 内置 middleware 机制（Logger, auth middleware via `from_fn`）
- 项目启动时 actix-web 在 Rust Web 框架中生态更成熟

## 为什么不使用全局锁？

**决策**：避免在消息路由路径中使用全局锁

**理由**：
- 消息路由是性能关键路径，全局锁会严重限制吞吐
- DashMap + mpsc 通道的组合已经消除大部分锁竞争
- 唯一使用 Mutex 的是 SubscriptionTree（读多写少，锁持有时间极短）
