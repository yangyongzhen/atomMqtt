# 新增 WebSocket 消息类型

## 背景

WebSocket 用于浏览器实时接收 MQTT 消息和状态通知。消息格式为 JSON，通过 `web_subscribers DashMap<String, UnboundedSender<String>>` 投递。

## 步骤

### 1. 在后台路由器 (`server.rs`) 中添加新消息投递

```rust
// 在 start_broker 的 match 分支中添加
for sub in &subscribers {
    if let Some(entry) = bg_state.web_subscribers.get(&sub.client_id) {
        let json_msg = serde_json::json!({
            "type": "xxx_event",
            "field1": value1,
            "field2": value2,
        });
        let _ = entry.send(json_msg.to_string());
    }
}
```

### 2. 在前端 `dashboard.js` 的 `handleWsMessage()` 中添加处理

```javascript
function handleWsMessage(msg) {
    switch (msg.type) {
        case 'xxx_event':
            handleXxxEvent(msg);
            break;
        // ... 已有分支
    }
}
```

### 3. 添加从浏览器发送的 WebSocket 消息

在 `handle_ws_session()` (`api.rs`) 中添加新的 `match` 分支：

```rust
"xxx_action" => {
    // 处理前端发来的动作
    let _ = tx.send(serde_json::json!({
        "type": "xxx_response",
        "success": true,
    }).to_string());
}
```

### 4. 前端发送函数

```javascript
function sendXxxAction(data) {
    wsSend({
        type: 'xxx_action',
        ...data
    });
}
```

## 消息格式约定

| 方向 | type 命名 | 说明 |
|------|-----------|------|
| 服务端 → 客户端 | 过去式/名词 | `publish`, `subscribed`, `pong`, `error` |
| 客户端 → 服务端 | 祈使句/动作 | `subscribe`, `unsubscribe`, `ping` |

## 错误处理

服务端出错时始终返回：
```json
{ "type": "error", "message": "描述信息" }
```
