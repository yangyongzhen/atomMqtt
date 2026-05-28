# Web API 文档

> **版本**: 0.1.0  
> **基础 URL**: `http://localhost:8081`  
> **更新**: 2025-01-15

---

## 1. REST API

### 1.0 用户登录

```
POST /api/login
Content-Type: application/json
```

**请求**:
```json
{
  "username": "admin",
  "password": "your_password"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `username` | string | 是 | 用户名 |
| `password` | string | 是 | 密码 |

**响应**:
```json
{
  "success": true,
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**说明**:
- 登录成功后返回 JWT 令牌，需在后续请求的 `Authorization` 头中携带
- 令牌有效期默认为 24 小时

---

### 1.0.1 API 认证说明

除 `/api/login` 外，所有 REST API 请求需要在 HTTP 头中携带 JWT 令牌进行认证：

```
Authorization: Bearer <token>
```

**认证流程**:
1. 调用 `POST /api/login` 获取 JWT 令牌
2. 在后续请求的 `Authorization` 头中携带该令牌
3. 服务端验证令牌有效性后处理请求

**令牌校验规则**:
- 令牌过期（默认 24 小时）→ 返回 `401 Unauthorized`
- 令牌无效或签名错误 → 返回 `401 Unauthorized`
- 未携带令牌 → 返回 `401 Unauthorized`

**ACL 访问控制**:
- 每个用户关联一组 ACL 规则，控制其对主题的发布和订阅权限
- 发布操作需主题具有 `write` 权限
- 订阅操作需主题具有 `read` 权限
- ACL 规则支持 MQTT 通配符（`+` / `#`），灵活匹配主题范围
- 未配置 ACL 规则默认拒绝所有操作

---

### 1.1 获取 Broker 指标

```
GET /api/metrics
```

**响应**:
```json
{
  "bytes_received": 1024,
  "bytes_sent": 512,
  "messages_published": 10,
  "messages_received": 20,
  "subscriptions_active": 5,
  "clients_connected": 3,
  "clients_total": 10,
  "packets_received": 30,
  "packets_sent": 25,
  "rejected_connections": 0,
  "uptime_seconds": 3600
}
```

**字段说明**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `bytes_received` | u64 | 累计接收字节数 |
| `bytes_sent` | u64 | 累计发送字节数 |
| `messages_published` | u64 | 累计发布消息数 |
| `messages_received` | u64 | 累计接收消息数 |
| `subscriptions_active` | u64 | 当前活跃订阅数 |
| `clients_connected` | u64 | 当前在线客户端数 |
| `clients_total` | u64 | 累计连接客户端数 |
| `packets_received` | u64 | 累计接收数据包数 |
| `packets_sent` | u64 | 累计发送数据包数 |
| `rejected_connections` | u64 | 被拒绝的连接数 |
| `uptime_seconds` | u64 | 运行时间（秒） |

---

### 1.2 获取 Broker 信息

```
GET /api/broker/info
```

**响应**:
```json
{
  "version": "0.1.0",
  "name": "AtomMQTT Broker",
  "uptime_seconds": 3600,
  "config": {
    "tcp_host": "0.0.0.0",
    "tcp_port": 1883,
    "web_host": "0.0.0.0",
    "web_port": 8081,
    "max_packet_size": 10485760,
    "allow_anonymous": false,
    "session_expiry_interval": 3600
  },
  "protocol_versions": ["MQTT 3.1.1", "MQTT 5.0"]
}
```

---

### 1.3 获取客户端列表

```
GET /api/clients
```

**响应**:
```json
[
  {
    "client_id": "sensor-01",
    "protocol_version": "V311",
    "connected": true,
    "keep_alive": 60,
    "username": "anonymous",
    "uptime_seconds": 120,
    "subscriptions_count": 3
  }
]
```

---

### 1.4 获取客户端详情

```
GET /api/clients/{client_id}
```

**响应**:
```json
{
  "client_id": "sensor-01",
  "protocol_version": "V311",
  "connected": true,
  "clean_session": false,
  "keep_alive": 60,
  "username": "anonymous",
  "created_at_seconds": 1705300000,
  "last_active_seconds": 1705300120,
  "subscriptions": [
    {"filter": "sensor/#", "qos": 1}
  ]
}
```

---

### 1.5 获取订阅列表

```
GET /api/subscriptions
```

**响应**:
```json
[
  {
    "client_id": "sensor-01",
    "filter": "sensor/#",
    "qos": 1
  },
  {
    "client_id": "web_xxx",
    "filter": "test/#",
    "qos": 1
  }
]
```

---

### 1.6 获取保留消息

```
GET /api/retained
```

**响应**:
```json
[
  {
    "topic": "sensor/config",
    "qos": 1,
    "payload": "{\"interval\": 30}",
    "timestamp_seconds": 1705300000
  }
]
```

---

### 1.7 发布消息

```
POST /api/publish
Content-Type: application/json
```

**请求**:
```json
{
  "topic": "test/hello",
  "payload": "Hello MQTT!",
  "qos": 1,
  "retain": false
}
```

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `topic` | string | 是 | - | 发布主题 |
| `payload` | string | 是 | - | 消息内容 |
| `qos` | number | 否 | 0 | QoS 级别 (0/1/2) |
| `retain` | boolean | 否 | false | 是否保留消息 |

**响应**:
```json
{
  "success": true,
  "topic": "test/hello",
  "subscriber_count": 2
}
```

**说明**:
- 消息通过 Broker 后台路由器投递，同时送达 TCP 订阅者和 WebSocket 订阅者
- `subscriber_count` 表示匹配该主题的活跃订阅者数量
- QoS 1 和 QoS 2 的发布会生成对应的 PubAck/PubRec 响应

---

### 1.8 断开客户端

```
POST /api/clients/{client_id}/disconnect
```

**响应**:
```json
{
  "success": true,
  "client_id": "sensor-01"
}
```

**说明**:
- 断开后自动清理订阅、会话和连接通道
- 如果有遗嘱消息，由后台路由器发送

---

### 1.9 删除保留消息

```
DELETE /api/retained/{topic}
```

**响应**:
```json
{
  "success": true,
  "topic": "sensor/config"
}
```

**说明**:
- 删除指定主题的保留消息
- 如果主题不存在保留消息，仍返回 `success: true`
- 删除后新订阅者将不再收到该主题的保留消息

---

## 2. WebSocket API

### 2.1 连接

```
ws://localhost:8081/ws/subscribe
```

建立连接后，服务器会发送欢迎消息：
```json
{"status": "connected"}
```

### 2.2 订阅主题

**请求** (客户端 → 服务器):
```json
{
  "type": "subscribe",
  "topic_filter": "test/#",
  "qos": 1
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `type` | string | 是 | 固定为 `"subscribe"` |
| `topic_filter` | string | 是 | MQTT 主题过滤器（支持 `+` `#`）|
| `qos` | number | 否 | 订阅 QoS (0/1/2，默认 0) |

**响应** (服务器 → 客户端):
```json
{
  "type": "subscribed",
  "topic_filter": "test/#",
  "qos": 1
}
```

### 2.3 取消订阅

**请求**:
```json
{
  "type": "unsubscribe",
  "topic_filter": "test/#"
}
```

**响应**:
```json
{
  "type": "unsubscribed",
  "topic_filter": "test/#"
}
```

### 2.4 心跳

**请求**:
```json
{
  "type": "ping"
}
```

**响应**:
```json
{
  "type": "pong"
}
```

### 2.5 接收消息

当有匹配的消息发布时，服务器主动推送：

```json
{
  "type": "publish",
  "topic": "test/hello",
  "payload": "Hello MQTT!",
  "qos": 1,
  "source_client": "sensor-01",
  "timestamp": "2025-01-15T10:30:00+08:00"
}
```

### 2.6 取消所有订阅

**请求**:
```json
{
  "type": "unsubscribe_all"
}
```

**响应**:
```json
{
  "type": "unsubscribed_all"
}
```

---

## 3. 错误处理

所有 API 使用标准的 HTTP 状态码：

| 状态码 | 说明 | 典型场景 |
|--------|------|----------|
| 200 | 成功 | 正常响应 |
| 400 | 请求错误 | JSON 格式错误、缺少必填字段 |
| 404 | 未找到 | 客户端 ID 不存在 |
| 405 | 方法不允许 | 使用了错误 HTTP 方法 |
| 500 | 服务器错误 | 内部处理异常 |

WebSocket 错误以 JSON 消息返回：
```json
{
  "type": "error",
  "message": "Unknown command type: xxx"
}
```

---

## 4. 前端集成

### 前端架构

```
static/  (编译时嵌入到二进制)
├── index.html          (单页应用入口)
├── css/dashboard.css   (样式)
└── js/dashboard.js     (交互逻辑)
```

> 前端文件在编译时通过 `include_dir!("mqtt-web/static")` 宏直接嵌入到 `mqtt-web` 二进制中。
> 运行时使用一条通配路由 `/{path:.*}` + `serve_embedded_file` 函数提供服务，无需任何磁盘 I/O。
> 添加新文件到 `static/` 目录后重新编译即可自动包含，无需修改代码。

### 自动刷新机制

- 仪表盘指标：每 2 秒通过 `GET /api/metrics` 刷新
- WebSocket：自动重连（指数退避，最大 30 秒间隔）
- 客户端列表：点击刷新按钮触发

### WebSocket 前端流程

```javascript
// 连接
ws = new WebSocket("ws://" + host + "/ws/subscribe");

// 订阅
ws.send(JSON.stringify({
    type: "subscribe",
    topic_filter: "test/#",
    qos: 1
}));

// 接收消息
ws.onmessage = function(event) {
    const msg = JSON.parse(event.data);
    if (msg.type === "publish") {
        // 显示在表格中
        addMessage(msg.topic, msg.payload, msg.qos, msg.timestamp);
    }
};

// 自动重连
ws.onclose = function() {
    setTimeout(reconnect, delay);
    delay = Math.min(delay * 2, 30000);
};
```
