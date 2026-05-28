# 新增持久化事件

## 步骤

### 1. 在 `persistence.rs` 的 `PersistEvent` 枚举中添加变体

```rust
pub enum PersistEvent {
    // ... 已有变体
    /// 新类型的持久化操作。
    SaveNewXxx { /* 字段 */ },
    RemoveXxx(String),
}
```

### 2. 创建 SQL 表（在 `Persistence::open()` 的 `CREATE TABLE` 部分）

```sql
CREATE TABLE IF NOT EXISTS xxx (
    -- 字段定义
    PRIMARY KEY (xxx)
) STRICT;
```

### 3. 在 `flush_all()` 中添加处理分支

```rust
PersistEvent::SaveNewXxx { .. } => {
    // 使用 INSERT OR REPLACE
    db.execute("INSERT OR REPLACE INTO xxx (...) VALUES (?1, ?2, ...)", params![])?;
}
PersistEvent::RemoveXxx(val) => {
    db.execute("DELETE FROM xxx WHERE key = ?1", params![val])?;
}
```

### 4. 添加加载方法（启动恢复用）

```rust
pub fn load_xxx(&self) -> Vec<XxxMessage> {
    let db = self.db.lock().unwrap();
    // prepare → query_map → collect
}
```

### 5. 在 `main.rs` 的启动恢复流程中调用

```rust
for xxx in persistence_arc.load_xxx() {
    state.xxx.insert(xxx.key.clone(), xxx);
}
```

## 设计约束

- 所有 `PersistEvent` 必须在 `flush_all()` 中有 `match` 分支
- 使用 `INSERT OR REPLACE` 避免主键冲突
- 启动时 `load_*` 方法按类型独立加载，不跨表关联
- 加载错误应记录 `error!` 日志并返回空 Vec（不阻止启动）
