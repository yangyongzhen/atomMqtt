# SQLite 持久化模式知识库

## 架构概览

```
内存数据结构 ← 主流程操作 (零等待)
    │ PersistEvent (mpsc::UnboundedSender)
    ▼
后台写入任务 (bg_writer)
    │ 触发: 50 个事件 或 100ms 定时器
    ▼
BEGIN TRANSACTION → 批量执行 SQL → COMMIT
    ▼
broker.db (WAL 模式)
```

## 数据库表结构

```sql
CREATE TABLE IF NOT EXISTS sessions (
    client_id TEXT PRIMARY KEY,
    protocol_version INTEGER NOT NULL,
    clean_session INTEGER NOT NULL,
    keep_alive INTEGER NOT NULL,
    username TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS subscriptions (
    client_id TEXT NOT NULL,
    filter TEXT NOT NULL,
    qos INTEGER NOT NULL,
    PRIMARY KEY (client_id, filter)
) STRICT;

CREATE TABLE IF NOT EXISTS retained_messages (
    topic TEXT PRIMARY KEY,
    payload BLOB NOT NULL,
    qos INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS will_messages (
    client_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload BLOB NOT NULL,
    qos INTEGER NOT NULL,
    retain INTEGER NOT NULL,
    delay_interval INTEGER NOT NULL
) STRICT;
```

- 所有表使用 `STRICT` 模式（SQLite 3.37+），禁止类型宽松
- `INSERT OR REPLACE` 处理 upsert

## PersistEvent 枚举

```rust
pub enum PersistEvent {
    SaveSession { client_id, protocol_version, clean_session, keep_alive, username },
    RemoveSession(String),
    SaveSubscription { client_id, filter, qos },
    RemoveSubscription { client_id, filter },
    RemoveClientSubscriptions(String),
    SaveRetained { topic, payload, qos },
    RemoveRetained(String),
    SaveWill { client_id, topic, payload, qos, retain, delay_interval },
    RemoveWill(String),
    Shutdown,
}
```

## 批量写入实现

```rust
async fn bg_writer(db: Arc<Mutex<Connection>>, mut rx: mpsc::UnboundedReceiver<PersistEvent>) {
    let mut batch = Vec::new();
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    
    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                if matches!(event, PersistEvent::Shutdown) {
                    flush_all(&db, &batch).ok();
                    return;
                }
                batch.push(event);
                if batch.len() >= 50 {
                    flush_all(&db, &batch);
                    batch.clear();
                }
            }
            _ = timer.tick() => {
                if !batch.is_empty() {
                    flush_all(&db, &batch);
                    batch.clear();
                }
            }
        }
    }
}
```

## 启动恢复流程

```
Persistence::open() → 创建/打开 DB
    ↓
Persistence::load_sessions()     → state.sessions 恢复
Persistence::load_subscriptions() → state.subscriptions 恢复
Persistence::load_retained()     → state.retained 恢复
Persistence::load_wills()        → state.wills 恢复
    ↓
Startup cleanup:
  1. 删除所有 clean_session=true 的已断连客户端（崩溃残留）
  2. 删除所有孤儿订阅（session 已删除但订阅残留在 DB 中）
```

## 设计原则

1. **主流程不等待磁盘**：所有持久化通过 mpsc 通道异步发送
2. **事件溯源**：每个状态变更对应一个 `PersistEvent`，可审计可重放
3. **批量减少 I/O**：50 条或 100ms 一次事务提交
4. **降级不崩溃**：持久化失败只记录 `error!` 日志，不阻塞主流程
5. **WAL 模式**：读不阻塞写，写不阻塞读
