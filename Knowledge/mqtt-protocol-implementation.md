# MQTT 协议实现知识库

## MQTT 包编码结构

```
Byte 1:   [包类型(4b)] [DUP(1b)] [QoS(2b)] [Retain(1b)]
Byte 2+:  剩余长度 (Variable Byte Integer)
Bytes+:   可变头部 + 载荷 (取决于包类型)
```

## 剩余长度编码 (Variable Byte Integer)

```rust
// 编码：每个字节低7位是数据，最高位=1表示还有后续字节
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
        if multiplier > 128 * 128 * 128 { return Err(MalformedRemainingLength); }
        multiplier *= 128;
        if byte & 0x80 == 0 { return Ok((value, i + 1)); }
    }
    Err(MalformedRemainingLength)
}
```

## UTF-8 字符串编码（所有 MQTT 字符串通用）

```rust
// 编码: 2字节长度前缀 (Big Endian) + UTF-8 内容
fn encode_string(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let len = bytes.len() as u16;
    len.to_be_bytes().iter().chain(bytes).copied().collect()
}

// 解码
fn decode_string(data: &[u8]) -> Result<(&str, &[u8]), Error> {
    if data.len() < 2 { return Err(PacketTooShort); }
    let len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + len { return Err(PacketTooShort); }
    let s = std::str::from_utf8(&data[2..2+len]).map_err(|_| InvalidUtf8)?;
    Ok((s, &data[2+len..]))
}
```

## 协议版本自动检测

首次数据包第 7 字节为 `protocol_level`：

| 值 | 版本 |
|----|------|
| 4 | MQTT 3.1.1 |
| 5 | MQTT 5.0 |

```rust
fn decode_first_packet(buf: &mut BytesMut, config: &BrokerConfig) -> Result<Option<MqttPacket>> {
    match protocol_level {
        4 => decode_v311_packet(buf),
        5 => decode_v5_packet(buf),
        _ => Err(UnsupportedVersion),
    }
}
```

## QoS 处理流程

| QoS | 级别 | 握手次数 | ACK 包 |
|-----|------|----------|--------|
| 0 | At Most Once | 0 | 无 |
| 1 | At Least Once | 1 | PUBACK |
| 2 | Exactly Once | 2 | PUBREC → PUBREL → PUBCOMP |

**QoS 降级**：投递时路由器固定使用 `QoS::AtMostOnce`，后续需按订阅者请求 QoS 降级。

## 主题过滤与匹配

### 订阅树 (Trie) 结构

```
TopicNode {
    children: Vec<(String, TopicNode)>,
    subscriptions: Vec<Subscription>,
}
```

- `+` 子节点：匹配该层任意主题段
- `#` 子节点：匹配剩余所有层级（必须是过滤器最后一段）

### lookup 算法

```rust
fn collect_matching(node, segments, results, seen) {
    // 1. 检查本节点的 '#' 子节点（多级通配符）
    if node has child "#" → collect all subscriptions
    
    // 2. 到达 topic 末端 → 收集本节点订阅
    if segments.is_empty() → collect node.subscriptions
    
    // 3. 精确匹配
    if node has child matching segments[0] → recurse(rest)
    
    // 4. 单级通配符 '+'
    if node has child "+" → recurse(rest)
}
```

## 会话生命周期

```
CONNECT (CleanSession=false) → 创建/恢复 SessionState → CONNACK (session_present)
CONNECT (CleanSession=true)  → 创建新会话，丢弃旧的 → CONNACK (session_present=false)
DISCONNECT                   → 正常断开，按 clean_session 保留/删除
TCP 异常断开                 → 发送遗嘱消息，按 clean_session 处理
```

## Keep Alive

```
timeout = keep_alive × 1.5
PINGREQ <→ PINGRESP 心跳维持
超过 timeout 无数据 → 断开 → 遗嘱消息
```

## MQTT 5.0 特有特性

| 特性 | 实现注意 |
|------|----------|
| 会话过期 | 5.0 CONNECT 包的 `session_expiry_interval` 属性 |
| 消息过期 | PUBLISH 的 `message_expiry_interval` 属性 |
| 原因码 | 所有响应包包含 `ReasonCode` 枚举 |
| 用户属性 | UTF-8 键值对列表 |
| 订阅标识符 | 多个订阅可共享同一标识符 |
