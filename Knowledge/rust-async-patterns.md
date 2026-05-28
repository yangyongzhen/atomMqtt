# Rust 异步编程模式知识库

## 组合通信模型

本项目同时使用三种通信机制：

| 机制 | 用途 | 示例 |
|------|------|------|
| `Arc<T>` | 共享只读/可变状态 | `Arc<BrokerState>` 传递给所有 task |
| `mpsc::UnboundedChannel` | 一对多消息分发 | 后台路由器 → N 个连接 task |
| `DashMap<K, Sender>` | 按 key 路由消息 | `connections: DashMap<String, UnboundedSender<Vec<u8>>>` |

## tokio::select! 模式

每个 TCP 连接的主循环使用 `select!` 同时等待多个来源：

```rust
loop {
    tokio::select! {
        // 来源 1: TCP 数据
        result = framed.next() => {
            // 解码并处理 MQTT 包
        }
        // 来源 2: 内部转发消息
        Some(bytes) = conn_rx.recv() => {
            stream.write_all(&bytes).await?;
        }
    }
}
```

**关键点**：
- `select!` 公平轮询所有分支
- 连接 task 在单循环中处理双向流量，无需额外锁
- 断连时两路都会关闭，`select!` 退出

## 启动与优雅关闭

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 初始化日志、配置
    // 2. 创建 Persistence + BrokerState
    // 3. 从 DB 恢复状态
    // 4. 启动后台路由器（spawn）
    // 5. 启动 TCP Server
    // 6. 启动 Web Server
    // 7. tokio::select! 等待所有服务
    
    // 优雅关闭:
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        p.shutdown().await;  // flush 所有待写入事件
        std::process::exit(0);
    });
    
    tokio::select! {
        result = web_fut => { /* Web 服务结束 */ }
    }
    Ok(())
}
```

## Error 处理策略

| 层级 | 策略 | 类型 |
|------|------|------|
| 应用入口 | `anyhow::Result` | `anyhow::Error` |
| 模块函数 | `Result<T, E>` | 自定义错误枚举 |
| 连接处理 | `?` 传播 | 断连时直接 return |
| 持久化错误 | 记录 `error!` 日志 | 不传播（防止崩溃） |

## 通道发送失败自动清理

```rust
// 后台路由器投递
if let Some(tx) = bg_state.connections.get(&sub.client_id) {
    let _ = tx.send(encoded.to_vec());
    // 如果 channel 已关闭（对端断连），send 返回 Err，静默忽略
}

// 心跳/超时检测：1.5 × keep_alive 无数据则断开
```

## Share State 模式

```rust
// 创建
let state = Arc::new(BrokerState::new(config.clone(), persistence_arc.clone()));

// 传递给 TCP server（不同的闭包捕获同一个 Arc）
let tcp_state = state.clone();
tokio::spawn(async move { tcp_listener.accept_loop(tcp_state).await });

// 传递给 Web server
let web_state = state.clone();
let web_fut = start_web_server(web_state);
```

Web 层（actix-web）通过 `web::Data<T>` 包裹 Arc：

```rust
let state_data: web::Data<BrokerState> = web::Data::from(state);
// web::Data 内部保存 Arc，App 中 clone 也是 Arc 的 clone
```

## #[deny(unsafe_code)]

本项目禁止 unsafe 代码。所有并发通过安全抽象（DashMap、Mutex、mpsc）实现。
