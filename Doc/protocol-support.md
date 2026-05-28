# MQTT 协议支持

> **版本**: 0.1.0  
> **更新**: 2025-01-15

---

## 1. 支持的协议版本

| 版本 | 标识符 | 支持状态 | 说明 |
|------|--------|----------|------|
| MQTT 3.1.1 | v311 / Protocol Level 4 | ✅ 完整支持 | 最广泛兼容的 MQTT 版本 |
| MQTT 5.0 | v5 / Protocol Level 5 | ✅ 完整支持 | 含属性 (Properties) 机制 |
| MQTT 3.1 | v3 / Protocol Level 3 | ❌ 不支持 | 已弃用 |

---

## 2. 协议自动检测

Broker 在第一个数据包自动检测客户端使用的协议版本：

```rust
fn decode_first_packet(buf: &mut BytesMut, config: &BrokerConfig) -> Result<Option<MqttPacket>> {
    // 读取固定头、剩余长度
    // 检查 protocol_level (第 7 字节)
    match protocol_level {
        4 => decode_v311_packet(buf),    // MQTT 3.1.1
        5 => decode_v5_packet(buf),      // MQTT 5.0
        _ => Err(UnsupportedVersion),     // 拒绝
    }
}
```

检测后，连接全程使用该协议版本进行编解码。

---

## 3. 支持的数据包类型

### MQTT 3.1.1 (v311)

| 包类型 | 方向 | 支持 |
|--------|------|------|
| CONNECT | Client → Server | ✅ |
| CONNACK | Server → Client | ✅ |
| PUBLISH | 双向 | ✅ |
| PUBACK | 双向 | ✅ (QoS 1) |
| PUBREC | 双向 | ✅ (QoS 2) |
| PUBREL | 双向 | ✅ (QoS 2) |
| PUBCOMP | 双向 | ✅ (QoS 2) |
| SUBSCRIBE | Client → Server | ✅ |
| SUBACK | Server → Client | ✅ |
| UNSUBSCRIBE | Client → Server | ✅ |
| UNSUBACK | Server → Client | ✅ |
| PINGREQ | Client → Server | ✅ |
| PINGRESP | Server → Client | ✅ |
| DISCONNECT | 双向 | ✅ |
| AUTH | 双向 | ❌ (MQTT 5.0 独有) |

### MQTT 5.0 (v5)

MQTT 5.0 所有数据包类型均支持，含以下 5.0 特有特性：

| 特性 | 支持 | 实现细节 |
|------|------|----------|
| 会话过期 (Session Expiry) | ✅ | `session_expiry_interval` 配置 |
| 消息过期 (Message Expiry) | ✅ | 5.0 属性中的 `message_expiry_interval` |
| 原因码 (Reason Codes) | ✅ | 完整的原因码枚举 |
| 用户属性 (User Properties) | ✅ | UTF-8 键值对 |
| 订阅标识符 (Subscription Identifier) | ✅ | 用于订阅标识 |
| 内容类型 (Content Type) | ✅ | 字符串描述 |
| 响应主题 (Response Topic) | ✅ | 请求/响应模式 |
| 对比数据 (Correlation Data) | ✅ | 请求/响应关联 |
| 最大包大小 (Maximum Packet Size) | ✅ | 连接协商 |
| 主题别名 (Topic Alias) | ❌ | 未实现 |
| 订阅选项 (Subscription Options) | ⚠️ | 基础实现 |
| 服务器重定向 (Server Redirect) | ❌ | 未实现 |
| 认证数据 (Auth Data) | ❌ | AUTH 包未实现 |

---

## 4. QoS 支持

| 级别 | 名称 | 支持 | 说明 |
|------|------|------|------|
| 0 | At Most Once | ✅ | 最多一次，无确认 |
| 1 | At Least Once | ✅ | 至少一次，需要 PUBACK |
| 2 | Exactly Once | ✅ | 恰好一次，2 次握手 (PUBREC/PUBREL/PUBCOMP) |

**QoS 降级**：当发布者的 QoS 级别高于订阅者请求的 QoS 时，服务端应降级投递。当前实现中，后台路由统一使用 `QoS::AtMostOnce` 转发，后续将根据订阅者的请求 QoS 进行降级投递。

---

## 5. 主题过滤器语法

支持标准 MQTT 主题过滤器和通配符：

| 语法 | 含义 | 示例 |
|------|------|------|
| `sensor/temp` | 精确匹配 | 仅匹配 `sensor/temp` |
| `sensor/+/temp` | 单级通配符 | 匹配 `sensor/room1/temp`、`sensor/room2/temp` |
| `sensor/#` | 多级通配符 | 匹配 `sensor/temp`、`sensor/room1/temp`、`sensor/floor1/room1/temp` |
| `+` | 单层通配 | 匹配 `a`、`b`，不匹配 `a/b` |
| `#` | 多层通配 | 匹配所有主题 |

**规则**：
- `#` 只能在过滤器末尾使用：`sensor/#` ✅，`sensor/#/temp` ❌
- `+` 可以在任意层级使用：`+/+/temp` ✅
- 空段不合法：`sensor//temp` ❌
- 系统主题 `$SYS/#` 有特殊层级匹配

---

## 6. 编码/解码实现

### 6.1 固定头 (Fixed Header)

```
  Bit:  7  6  5  4  3  2  1  0
        ┌──┬──┬──┬──┬──┬──┬──┬──┐
Byte 1: │       │     DUP      │
        │ MQTT  │  QoS  │Flags│
        │ 包类型 │       │     │
        └──┴──┴──┴──┴──┴──┴──┴──┘
Byte 2+: 剩余长度 (Variable Byte Integer)
```

### 6.2 剩余长度编码

使用 MQTT 标准的 Variable Byte Integer 编码：

```rust
// 编码
fn encode_remaining_length(length: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut len = length;
    loop {
        let mut byte = (len & 0x7F) as u8;
        len >>= 7;
        if len > 0 { byte |= 0x80; }
        bytes.push(byte);
        if len == 0 { break; }
    }
    bytes
}

// 解码
fn decode_remaining_length(data: &[u8]) -> Result<(usize, usize), Error> {
    let mut value = 0usize;
    let mut multiplier = 1;
    for (i, &byte) in data.iter().enumerate() {
        value += (byte as usize & 0x7F) * multiplier;
        if multiplier > 128 * 128 * 128 { return Err(Error::MalformedRemainingLength); }
        multiplier *= 128;
        if byte & 0x80 == 0 { return Ok((value, i + 1)); }
    }
    Err(Error::MalformedRemainingLength)
}
```

### 6.3 UTF-8 字符串编码

所有 MQTT 字符串使用 `长度前缀(2字节) + UTF-8 内容` 格式：

```rust
fn encode_string(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let len = bytes.len() as u16;
    len.to_be_bytes().iter().chain(bytes).copied().collect()
}

fn decode_string(data: &[u8]) -> Result<(&str, &[u8]), Error> {
    if data.len() < 2 { return Err(Error::PacketTooShort); }
    let len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + len { return Err(Error::PacketTooShort); }
    let s = std::str::from_utf8(&data[2..2+len])
        .map_err(|_| Error::InvalidUtf8)?;
    Ok((s, &data[2+len..]))
}
```

---

## 7. 会话管理

### 7.1 会话生命周期

```
CONNECT (CleanSession=false)
  → 创建/恢复 SessionState
  → 发送 CONNACK (session_present=true/false)

CONNECT (CleanSession=true)
  → 创建新 SessionState
  → 丢弃旧会话数据
  → 发送 CONNACK (session_present=false)

DISCONNECT (正常断开)
  → CleanSession=true: 删除会话
  → CleanSession=false: 保留会话（含未投递消息）

TCP 断开 (异常断开)
  → 发送 Will Message (如有)
  → CleanSession=true: 删除会话和订阅
  → CleanSession=false: 保留会话，等待重连
```

### 7.2 Keep Alive 机制

```
Client ─── PINGREQ ──► Server
Server ─── PINGRESP ──► Client

超时计算:
  timeout = keep_alive × 1.5  (MQTT 标准)
  
如果超过 timeout 未收到任何包:
  → 判定连接为 stale
  → 断开连接
  → 发送 Will Message
```

---

## 8. 认证支持

### 8.1 匿名模式

默认模式，接受所有连接：
```rust
AuthMethod::None
```

### 8.2 文件密码模式

从文件中读取用户名:密码对：
```rust
AuthMethod::File { path: "users.txt" }
```

文件格式：
```
admin:123456
user1:pass1
sensor:token123
```

### 8.3 授权 — ACL 文件访问控制

基于 ACL 规则文件的 topic 级别访问控制，默认拒绝，首条匹配规则生效。

```rust
fn authorize(username: &str, topic: &str, action: &str) -> bool {
    let rules = load_acl("acl.conf");
    for rule in rules {
        if rule.matches(username, topic, action) {
            return rule.action == "allow";
        }
    }
    false // 默认拒绝
}
```

**ACL 规则文件 `acl.conf` 格式**：

每行格式：`user topic publish|subscribe|readwrite`

- `user` — 用户名，`*` 表示匹配所有用户
- `topic` — 主题过滤器，支持 `+` 和 `#` 通配符
- `publish` — 允许发布到该主题
- `subscribe` — 允许订阅该主题
- `readwrite` — 允许发布和订阅

```
# acl.conf — ACL 规则文件
# 格式: user topic publish|subscribe|readwrite
# 默认拒绝，第一条匹配规则生效

admin # readwrite        # 管理员可发布和订阅所有主题
sensor temperature/# publish   # sensor 用户可发布 temperature 下的所有子主题
client house/+/temp subscribe  # client 用户可订阅 house/任意房间/temp
* test/# subscribe       # 所有用户可订阅 test/ 下的主题
```

**匹配规则**：
1. 逐条从上至下匹配
2. 用户名和主题均匹配时，按指定动作（publish/subscribe/readwrite）判定
3. 首条匹配规则生效（allow 或 deny）
4. 无匹配规则时默认拒绝
5. 动作不匹配视为拒绝（如规则为 publish，尝试 subscribe 则拒绝）
