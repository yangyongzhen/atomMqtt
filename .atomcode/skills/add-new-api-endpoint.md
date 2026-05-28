# 新增 REST API 端点

## 步骤

### 1. 在 `api.rs` 中编写处理器函数

```rust
/// 获取某项数据
#[get("/api/xxx")]
pub async fn get_xxx(state: web::Data<BrokerState>) -> impl Responder {
    // 从 state 中读取数据
    // 使用 state.metrics.lock().unwrap() 访问指标
    // 使用 state.xxx.iter() 访问 DashMap
    
    HttpResponse::Ok().json(serde_json::json!({
        "field1": value1,
        "field2": value2,
    }))
}
```

**CRUD 操作模式：**

| 操作 | HTTP 方法 | 响应 |
|------|-----------|------|
| 读取 | GET | `200 + JSON` |
| 创建 | POST | `200/201 + JSON { success: true }` |
| 删除 | DELETE | `200 + JSON { success: true }` |
| 出错 | — | `4xx + JSON { success: false, error: "..." }` |

### 2. 在 `main.rs` 中注册路由

```rust
.service(api::get_xxx)           // 带 #[get] 宏的路由
// 或
.route("/api/xxx", web::get().to(api::get_xxx))  // 手动路由
```

### 3. 更新 Web API 文档 (`Doc/web-api.md`)

在对应章节添加端点描述、请求格式、响应格式。

### 4. 前端消费（如需）

在 `dashboard.js` 中添加：
```javascript
async function refreshXxx() {
    const resp = await apiFetch('/api/xxx');
    const data = await resp.json();
    // 更新 UI
}
```

在 `index.html` 中添加对应的 HTML 容器元素。
```

