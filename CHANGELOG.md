# Changelog

## [0.1.0] — 2025-01-15

### 新增

#### MQTT 协议核心 (mqtt-core)
- MQTT 3.1.1 完整支持：CONNECT / CONNACK / PUBLISH / PUBACK / SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK / PINGREQ / PINGRESP / DISCONNECT
- MQTT 5.0 完整支持（含属性机制）
- 高效的编码/解码器框架
- 剩余长度编码（Variable Length Integer）
- UTF-8 字符串编码/解码
- 数据包收发单元测试

#### Broker 引擎 (mqtt-broker)
- 基于 Trie 树的高效主题订阅树（支持 `+` 单级通配符和 `#` 多级通配符）
- 客户端会话状态管理（保持活跃、过期检测）
- 后台消息路由器（异步消息投递）
- 保留消息（Retained Message）存储与分发
- 遗嘱消息（Will Message）支持
- 内建认证框架（支持无认证和文件密码认证）
- **ACL 访问控制**：文件级 Topic 授权（publish / subscribe / readwrite）
- 性能指标采集（连接数、消息数、字节数、包数）
- TCP 监听与连接管理（异步 Tokio）
- 客户端连接通道（用于 TCP 订阅者消息投递）

#### Web 管理界面 (mqtt-web)
- Actix-Web HTTP 服务器集成
- REST API 端点：指标、客户端管理、订阅查询、发布消息、断开连接
- Broker 状态管理（启动/停止/指标更新）
- **WebSocket 订阅端点**：浏览器通过 WebSocket 实时接收 MQTT 主题消息
- 前端仪表盘：7 个功能页面
  - 📊 仪表盘 — 8 项实时指标卡片
  - 👥 客户端 — 在线客户端列表与操作
  - 📋 订阅 — 全部活跃订阅查看
  - 💾 保留消息 — 保留消息列表
  - 📤 发布消息 — 通过 HTTP 发布消息
  - 📡 订阅消息 — WebSocket 实时接收消息
  - ℹ️ 服务器信息 — Broker 配置详情
- 前端自动刷新与重连机制（每 2 秒拉取指标，WebSocket 自动重连）

#### CLI 客户端 (mqtt-client)
- 发布模式：连接 → 发布 → 等待 PubAck（QoS 1）→ 断开
- 订阅模式：连接 → 订阅 → 持续接收并显示消息
- 交互式 Shell 模式：支持 `pub` / `sub` / `unsub` / `ping` / `quit` 命令
- 自动重连和 Ping 保活机制
- 彩色终端输出

### 修复

- 修复服务端主循环 TCP 读取逻辑：避免在缓冲区有数据时阻塞读取
- 修复发布路径路由：Web API 发布的 `Publish` 消息现在通过 `BrokerHandle` 通道正确投递给后台路由器
- 修复消息投递：后台路由器现在通过 `connections` 通道将 PUBLISH 包转发给 TCP 订阅者
- 修复 WebSocket 订阅者注册：使用 `DashMap` 存储 Web 订阅者通道，Disconnect 时自动清理

### 技术细节

- **架构**：工作空间多 Crate 架构，解耦协议层、引擎层、展示层
- **异步**：Tokio 全异步运行时，`mpsc` 通道用于任务间通信
- **线程安全**：`DashMap` 无锁并发 Map + `Mutex` 保护的关键数据结构
- **包格式**：V311/V5 双格式支持，后台路由使用 V311 统一编码投递
- **前端**：纯原生 HTML/CSS/JS，无前端框架依赖
- **ACL**：文件配置文件格式，规则顺序评估（第一条匹配生效），默认拒绝
