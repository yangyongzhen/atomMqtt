# Web 管理界面模式知识库

## 分层架构

```
┌──────────────────────┐
│  前端 (Static HTML)   │ index.html + dashboard.js + dashboard.css
│  编译进二进制          │ include_dir!("mqtt-web/static")
├──────────────────────┤
│  REST API + WebSocket  │ api.rs (actix-web handlers)
├──────────────────────┤
│  BrokerState 共享状态  │ Arc<BrokerState>
└──────────────────────┘
```

## REST API 认证

**HTTP Basic Auth**，通过 actix-web middleware 实现：

```rust
// main.rs — web_auth_middleware
async fn web_auth_middleware(req, next) {
    if path.starts_with("/api/") && state.config.web_auth_enabled {
        // 跳过 /api/login
        // 验证 Authorization: Basic base64(username:password)
        // 失败 → 401 + WWW-Authenticate header
    }
    next.call(req).await
}
```

- `config.web_auth_enabled` 控制开关
- 默认关闭（兼容无配置首次启动）
- `/api/login` 端点跳过 Basic Auth 检查（使用 JSON POST 登录）

## API 处理器模式

```rust
#[get("/api/xxx")]
pub async fn get_xxx(state: web::Data<BrokerState>) -> impl Responder {
    // 1. 从 state 读取数据
    // 2. 构造 JSON 响应
    HttpResponse::Ok().json(serde_json::json!({ ... }))
}
```

**响应格式约定**：

```json
// 成功
{ "success": true, ... }

// 失败
{ "success": false, "error": "描述信息" }
```

## WebSocket 架构

### 连接生命周期

```
浏览器 ws://host:port/ws/subscribe → actix_web upgrade
    ↓
生成 uuid 作为 subscriber_id
    ↓
创建 mpsc::unbounded_channel<String>
    ↓
存入 web_subscribers DashMap
    ↓
主循环 select! { ws_msg | channel_msg }
    ↓
断开 → 从 web_subscribers 移除 → 取消所有订阅
```

### 消息协议 (JSON)

**客户端 → 服务端**：

| type | 字段 | 说明 |
|------|------|------|
| `subscribe` | `topic_filter`, `qos` | 订阅主题 |
| `unsubscribe` | `topic_filter` | 取消订阅 |
| `unsubscribe_all` | — | 取消所有订阅 |
| `ping` | — | 心跳 |

**服务端 → 客户端**：

| type | 字段 | 说明 |
|------|------|------|
| `subscribed` | `topic_filter`, `qos` | 订阅确认 |
| `unsubscribed` | `topic_filter` | 取消订阅确认 |
| `publish` | `topic`, `payload`, `qos`, `source_client`, `timestamp` | 收到消息 |
| `pong` | — | 心跳回复 |
| `error` | `message` | 错误通知 |

## 前端数据流

```
apiFetch('/api/metrics') ──► 定时刷新 (每 5 秒)
    │
    ▼
refreshDashboard() ──► update DOM (innerHTML)
    │
    ▼
WebSocket 实时消息 ──► handleWsMessage() ──► addMessageRow() / toast
```

## 认证流程

```
Dashboard 页面加载
    │
checkAuth() → 检查 sessionStorage 中的 auth 信息
    │
    ├── 有认证信息 → 正常加载
    └── 无认证信息 → 向后端调任意 API
        ├── 401 → 跳转 login.html
        └── 200 → 无需认证（web_auth_enabled = false）
```

## 自动刷新机制

```javascript
function startAutoRefresh() {
    setInterval(() => {
        if (currentPage === 'dashboard') refreshDashboard();
        if (currentPage === 'clients') refreshClients();
        // ...
    }, 5000); // 5 秒刷新
}
```

- WebSocket 心跳：每 30 秒发送 `ping`
- WebSocket 断开自动重连（指数退避 + 最大 30 秒间隔）
- API 返回 401 时立即跳转登录页

## 嵌入资源

```rust
static STATIC_DIR: Dir<'_> = include_dir!("mqtt-web/static");

async fn serve_embedded_file(req: actix_web::HttpRequest) -> HttpResponse {
    let path = req.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    match STATIC_DIR.get_file(path) {
        Some(file) => HttpResponse::Ok()
            .content_type(mime_guess::from_path(path).first_or_octet_stream().to_string())
            .body(file.contents()),
        None => HttpResponse::NotFound().body("404 Not Found"),
    }
}
```
