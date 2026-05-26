# 消息路由机制

> **版本**: 0.1.0  
> **更新**: 2025-01-15

---

## 1. 路由架构

```
                ┌──────────────────────┐
                │  消息来源 (Sources)    │
                │                      │
                │  ┌─────┐ ┌─────────┐ │
                │  │ TCP  │ │ Web API │ │
                │  │Client│ │  POST   │ │
                │  └──┬──┘ └────┬────┘ │
                └─────┼─────────┼──────┘
                      │         │
                ┌─────▼─────────▼──────┐
                │                    │
                │  BrokerHandle       │
                │  (mpsc::Unbounded   │
                │   Sender)           │
                │                    │
                └─────────┬──────────┘
                          │
                ┌─────────▼──────────┐
                │  后台路由器任务      │
                │  (Background        │
                │   Router Loop)      │
                │                    │
                │  match msg {        │
                │   Publish → route   │
                │   ClientDisconn →   │
                │     cleanup         │
                │  }                  │
                └────┬────┬───────────┘
                     │    │
           ┌─────────┘    └─────────┐
           ▼                        ▼
┌──────────────────┐    ┌──────────────────┐
│ TCP 连接通道       │    │ WebSocket 通道    │
│ (connections      │    │ (web_subscribers │
│  DashMap)         │    │  DashMap)        │
│                   │    │                  │
│ Sender<Vec<u8>>   │    │ Sender<String>   │
│  V311 PUBLISH 包   │    │  JSON 消息        │
└──────────────────┘    └──────────────────┘
```

---

## 2. 消息类型

### BrokerMessage 枚举

```rust
pub enum BrokerMessage {
    /// 发布消息：主题 + 载荷 + QoS + 保留标志 + 来源客户端
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
        source_client: String,
    },
    /// 客户端断开事件
    ClientDisconnected {
        client_id: String,
    },
}
```

---

## 3. 路由流程

### 3.1 消息发布

```rust
// 来源 1: TCP 客户端通过 MQTT PUBLISH 包
process_v3_packet / process_v5_packet
    → 解析 SUBSCRIBE/PUBLISH 等
    → 通过 broker_handle.sender 发送 BrokerMessage::Publish

// 来源 2: Web API POST /api/publish
publish_message()
    → 构造 BrokerMessage::Publish
    → 通过 state.broker_handle.sender 发送
```

### 3.2 后台路由

```rust
// 后台路由循环 (start_broker 中 spawn)
while let Some(msg) = msg_rx.recv().await {
    match msg {
        BrokerMessage::Publish { topic, payload, qos, retain, source_client } => {
            // 步骤 1: 查找订阅者
            let subscribers = subscriptions.lock().unwrap().lookup(&topic);

            // 步骤 2: 投递给 TCP 订阅者
            for sub in &subscribers {
                if let Some(tx) = connections.get(&sub.client_id) {
                    // 编码为 V311 PUBLISH 包
                    let publish_pkt = encode_publish(topic, payload, qos);
                    tx.send(encoded_bytes);
                }
            }

            // 步骤 3: 投递给 WebSocket 订阅者
            for sub in &subscribers {
                if let Some(entry) = web_subscribers.get(&sub.client_id) {
                    let json_msg = json!({
                        "type": "publish",
                        "topic": topic,
                        "payload": payload_str,
                        "qos": qos,
                        "source_client": source_client,
                        "timestamp": now,
                    });
                    entry.send(json_msg.to_string());
                }
            }

            // 步骤 4: 处理保留消息
            if retain { retained.insert(topic, retained_message); }
        }

        BrokerMessage::ClientDisconnected { client_id } => {
            // 步骤 1: 发送遗嘱消息
            // 步骤 2: 清理订阅
            // 步骤 3: 清理会话
        }
    }
}
```

### 3.3 TCP 连接内部投递

每个 TCP 连接在 CONNACK 发送后，创建一个 `mpsc::unbounded_channel`，发送端存入 `connections` DashMap，接收端在主循环的 `tokio::select!` 中等待：

```rust
// 创建通道
let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
state.connections.insert(client_id.clone(), conn_tx);

// 主循环: 同时等待 TCP 数据和内部转发
loop {
    tokio::select! {
        // TCP 数据到达
        result = framed.next() => {
            // 解码 → 处理
        }
        // 内部转发消息
        Some(bytes) = conn_rx.recv() => {
            // 直接写入 TCP 流
            stream.write_all(&bytes).await?;
        }
    }
}
```

---

## 4. 订阅者查找

使用 `SubscriptionTree::lookup(topic)`：

```
输入: "sensor/room1/temp"

匹配过程:
1. root.children["sensor"] ✓  → 递归
2.   → children["room1"] ✓    → 递归
3.     → children["temp"] ✓   → 找到订阅
       → children["+"] ✓      → 找到订阅 (如果有)
4.   回溯检查 "#" 订阅
5. 最终返回: [Sub("client1", "sensor/room1/temp"), ...]
```

---

## 5. 连接通道管理

### 5.1 注册 (CONNECT 时)

```rust
let (conn_tx, conn_rx) = mpsc::unbounded_channel::<Vec<u8>>();
state.connections.insert(client_id.clone(), conn_tx);
// conn_rx 传递给 handle_connection 主循环
```

### 5.2 注销 (DISCONNECT 或 TCP 断开时)

```rust
state.connections.remove(&client_id);
// 通过 BrokerHandle 发送 ClientDisconnected
broker_handle.sender.send(BrokerMessage::ClientDisconnected {
    client_id: client_id.clone(),
});
```

---

## 6. WebSocket 订阅通道管理

### 6.1 注册 (WebSocket 连接建立时)

```rust
let sub_id = format!("web_{}", uuid::Uuid::new_v4());
let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<String>();
state.web_subscribers.insert(sub_id.clone(), ws_tx);

// 添加到订阅树
state.subscriptions.lock().unwrap()
    .subscribe(&sub_id, &topic_filter, qos);
```

### 6.2 注销 (WebSocket 断开时)

```rust
state.web_subscribers.remove(&sub_id);
state.subscriptions.lock().unwrap()
    .unsubscribe_all(&sub_id);
```

---

## 7. 消息格式

### TCP 投递 (connections)

使用 **MQTT 3.1.1 PUBLISH 包** 编码的二进制数据：

```
[固定头] [剩余长度] [主题长度(2B)] [主题(UTF-8)] [包ID(仅QoS>0)] [载荷]
```

### WebSocket 投递 (web_subscribers)

使用 **JSON 字符串**：

```json
{
  "type": "publish",
  "topic": "sensor/temp",
  "payload": "25.5",
  "qos": 1,
  "source_client": "sensor-01",
  "timestamp": "2025-01-15T10:30:00+08:00"
}
```

---

## 8. 异常处理

### 8.1 连接断开

- TCP 连接中断 → `connections.remove()` 自动清理
- 发送 `ClientDisconnected` 消息给路由器
- 路由器清理订阅、会话，发送遗嘱消息

### 8.2 通道发送失败

- `tx.send()` 返回 `Err` → 表示接收端已关闭
- 自动从 `connections` / `web_subscribers` 中移除

### 8.3 编码错误

- 后台路由中 `encode_packet` 失败 → 记录错误日志（不崩溃）
- 该订阅者跳过投递，继续处理下一个
