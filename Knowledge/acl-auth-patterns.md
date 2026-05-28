# ACL 与认证模式知识库

## 认证模型

```rust
pub enum AuthMethod {
    None,                    // 无认证（默认）
    File { path: String },   // 从文件读取用户名:密码
}
```

### 认证流程

```rust
pub fn authenticate(&self, allow_anonymous: bool, username, password) -> AuthResult {
    match &self.method {
        AuthMethod::None => {
            if allow_anonymous → Success(username or "anonymous")
            else if username provided → Success
            else → Denied(AnonymousDisallowed)
        }
        AuthMethod::File { .. } => {
            if username + password match file → Success
            else if allow_anonymous → Success("anonymous")
            else → Denied(BadUsernameOrPassword)
        }
    }
}
```

## ACL 模型

```rust
pub enum AclMethod {
    None,                    // 无 ACL 检查（默认）
    File { path: String },   // 从文件读取 ACL 规则
}
```

### ACL 文件格式 (acl.conf)

```
# 格式: user topic publish|subscribe|readwrite
# 默认拒绝，首条匹配规则生效

admin # readwrite              # 管理员所有主题
sensor temperature/# publish   # 传感器只写
client house/+/temp subscribe  # 客户端订阅特定主题
* test/# subscribe              # 所有用户可订阅 test/ 下主题
```

### 匹配逻辑

1. 按用户名匹配规则块
2. 在规则块内按顺序匹配主题（支持 `+`/`#` 通配符）
3. 首条匹配的 Allow/Deny 立即生效
4. 无匹配 → **默认拒绝**

### 主题通配符匹配算法

```rust
fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    let topic_segments = topic.split('/');
    let filter_segments = filter.split('/');
    
    // 双指针逐段匹配
    // # = 匹配剩余所有层级
    // + = 匹配单层任意值
    // 其他 = 精确匹配
}
```

## API 层认证（Web Basic Auth）

```
config.web_auth_enabled 控制开关
config.web_auth_username / password 凭据

Middleware 对所有 /api/* 路径拦截:
  - /api/login 放行（使用 JSON POST 登录）
  - Authorization: Basic base64(user:pass) 验证
  - 失败 → 401 + WWW-Authenticate Basic realm="AtomMQTT"
```

## 错误码映射 (MQTT Auth)

| AuthErrorKind | V3 ReturnCode | V5 ReasonCode |
|---------------|---------------|---------------|
| BadUsernameOrPassword | ConnectionRefused(0x05) | BadUserNameOrPassword(0x86) |
| NotAuthorized | ConnectionRefused(0x05) | NotAuthorized(0x87) |
| AnonymousDisallowed | ConnectionRefused(0x05) | NotAuthorized(0x87) |
| ServerUnavailable | ServerUnavailable(0x03) | ServerUnavailable(0x89) |

## 设计要点

- 认证和 ACL 是独立的两个维度：认证通过 ≠ 有权限发布/订阅
- ACL 默认拒绝：没有显式允许就是拒绝
- 匿名认证 + ACL 可组合使用：允许匿名连接，但限制主题访问
- 非认证模式（AuthMethod::None + allow_anonymous=true）没有安全保护，仅测试用途
