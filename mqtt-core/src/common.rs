//! Common types shared across MQTT 3.1.1 and 5.0.

use std::fmt;

/// MQTT protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolVersion {
    /// MQTT 3.1.1 (v4)
    V311,
    /// MQTT 5.0 (v5)
    V5,
}

impl ProtocolVersion {
    /// Protocol level byte used in CONNECT packet.
    pub fn level_byte(self) -> u8 {
        match self {
            ProtocolVersion::V311 => 4,
            ProtocolVersion::V5 => 5,
        }
    }

    /// Name string used in CONNECT packet.
    pub fn name_str(self) -> &'static str {
        match self {
            ProtocolVersion::V311 => "MQTT",
            ProtocolVersion::V5 => "MQTT",
        }
    }
}

/// Quality of Service level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QoS {
    /// At most once delivery (fire and forget).
    AtMostOnce = 0,
    /// At least once delivery (acknowledged).
    AtLeastOnce = 1,
    /// Exactly once delivery (handshake).
    ExactlyOnce = 2,
}

impl QoS {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(QoS::AtMostOnce),
            1 => Some(QoS::AtLeastOnce),
            2 => Some(QoS::ExactlyOnce),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QoS::AtMostOnce => write!(f, "QoS0"),
            QoS::AtLeastOnce => write!(f, "QoS1"),
            QoS::ExactlyOnce => write!(f, "QoS2"),
        }
    }
}

/// MQTT packet types (common across versions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5,
    PubRel = 6,
    PubComp = 7,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
    Auth = 15,
}

impl PacketType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value >> 4 {
            1 => Some(PacketType::Connect),
            2 => Some(PacketType::ConnAck),
            3 => Some(PacketType::Publish),
            4 => Some(PacketType::PubAck),
            5 => Some(PacketType::PubRec),
            6 => Some(PacketType::PubRel),
            7 => Some(PacketType::PubComp),
            8 => Some(PacketType::Subscribe),
            9 => Some(PacketType::SubAck),
            10 => Some(PacketType::Unsubscribe),
            11 => Some(PacketType::UnsubAck),
            12 => Some(PacketType::PingReq),
            13 => Some(PacketType::PingResp),
            14 => Some(PacketType::Disconnect),
            15 => Some(PacketType::Auth),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        (self as u8) << 4
    }

    /// Whether this packet type expects a packet ID field.
    pub fn has_packet_id(self) -> bool {
        matches!(
            self,
            PacketType::PubAck
                | PacketType::PubRec
                | PacketType::PubRel
                | PacketType::PubComp
                | PacketType::Subscribe
                | PacketType::SubAck
                | PacketType::Unsubscribe
                | PacketType::UnsubAck
        )
    }

    /// Whether this packet type is a "control" packet (not publish).
    pub fn is_control(self) -> bool {
        !matches!(self, PacketType::Publish)
    }
}

/// Topic filter for subscriptions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicFilter {
    /// Raw filter string (e.g., "sensor/+/temperature").
    pub filter: String,
    /// Pre-split segments for matching.
    segments: Vec<String>,
    /// Whether the filter ends with '#' (multi-level wildcard).
    has_multiwild: bool,
}

impl TopicFilter {
    pub fn new(filter: &str) -> Self {
        let filter = filter.trim().to_string();
        let segments: Vec<String> = filter.split('/').map(|s| s.to_string()).collect();
        let has_multiwild = segments.last().map(|s| s == "#").unwrap_or(false);
        TopicFilter { filter, segments, has_multiwild }
    }

    /// Check if a topic matches this filter.
    pub fn matches(&self, topic: &str) -> bool {
        let topic_segs: Vec<&str> = topic.split('/').collect();
        let mut ti = 0; // topic segment index

        for (_si, seg) in self.segments.iter().enumerate() {
            if seg == "#" {
                // '#' matches any remaining segments including zero
                return true;
            }

            if ti >= topic_segs.len() {
                return false;
            }

            if seg == "+" {
                // '+' matches exactly one segment
                ti += 1;
                continue;
            }

            if seg != topic_segs[ti] {
                return false;
            }
            ti += 1;
        }

        // All filter segments consumed; topic should also be fully consumed
        ti == topic_segs.len()
    }

    pub fn as_str(&self) -> &str {
        &self.filter
    }
}

/// A topic name (for publishing).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicName {
    pub name: String,
}

impl TopicName {
    pub fn new(name: &str) -> Self {
        TopicName { name: name.to_string() }
    }

    pub fn as_str(&self) -> &str {
        &self.name
    }

    /// Validate MQTT topic name rules.
    pub fn is_valid(&self) -> bool {
        if self.name.is_empty() {
            return false;
        }
        // Topic name must not contain wildcard characters
        if self.name.contains('+') || self.name.contains('#') {
            return false;
        }
        true
    }
}

/// Error type for MQTT operations.
#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("Invalid packet: {0}")]
    InvalidPacket(String),

    #[error("Protocol violation: {0}")]
    ProtocolViolation(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("Malformed remaining length")]
    MalformedRemainingLength,

    #[error("Packet too large: {0} bytes")]
    PacketTooLarge(usize),

    #[error("Invalid UTF-8 string")]
    InvalidUtf8,

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Authentication failed: {0}")]
    AuthFailed(String),
}

/// Result type for MQTT operations.
pub type MqttResult<T> = Result<T, MqttError>;

/// Maximum remaining length bytes (4 bytes, 28 bits)
pub const MAX_REMAINING_LENGTH_BYTES: usize = 4;
/// Maximum packet size (256 MB)
pub const MAX_PACKET_SIZE: usize = 256 * 1024 * 1024;
/// Default max packet size for broker
pub const DEFAULT_MAX_PACKET_SIZE: usize = 10 * 1024 * 1024;

/// Encode remaining length using MQTT variable-length encoding.
pub fn encode_remaining_length(mut length: usize) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4);
    loop {
        let mut byte = (length & 0x7F) as u8;
        length >>= 7;
        if length > 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if length == 0 {
            break;
        }
    }
    encoded
}

/// Decode remaining length from MQTT variable-length encoding.
/// Returns (value, bytes_consumed).
pub fn decode_remaining_length(buf: &[u8]) -> MqttResult<(usize, usize)> {
    let mut value: usize = 0;
    let mut multiplier: usize = 1;
    let mut consumed;

    for (i, &byte) in buf.iter().enumerate() {
        consumed = i + 1;
        if consumed > MAX_REMAINING_LENGTH_BYTES {
            return Err(MqttError::MalformedRemainingLength);
        }
        value += (byte as usize & 0x7F) * multiplier;
        multiplier *= 128;
        if byte & 0x80 == 0 {
            return Ok((value, consumed));
        }
    }

    Err(MqttError::MalformedRemainingLength)
}

/// Encode a UTF-8 string with length prefix (MQTT format).
pub fn encode_string(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut out = Vec::with_capacity(2 + len);
    out.push((len >> 8) as u8);
    out.push((len & 0xFF) as u8);
    out.extend_from_slice(bytes);
    out
}

/// Decode a length-prefixed UTF-8 string.
pub fn decode_string(buf: &[u8]) -> MqttResult<(&str, usize)> {
    if buf.len() < 2 {
        return Err(MqttError::InvalidPacket("Insufficient data for string length".into()));
    }
    let len = (buf[0] as usize) << 8 | (buf[1] as usize);
    if buf.len() < 2 + len {
        return Err(MqttError::InvalidPacket("Insufficient data for string".into()));
    }
    match std::str::from_utf8(&buf[2..2 + len]) {
        Ok(s) => Ok((s, 2 + len)),
        Err(_) => Err(MqttError::InvalidUtf8),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remaining_length_encoding() {
        let cases = vec![
            (0, vec![0x00]),
            (127, vec![0x7F]),
            (128, vec![0x80, 0x01]),
            (16383, vec![0xFF, 0x7F]),
            (2097151, vec![0xFF, 0xFF, 0x7F]),
            (268435455, vec![0xFF, 0xFF, 0xFF, 0x7F]),
        ];

        for (input, expected) in cases {
            let encoded = encode_remaining_length(input);
            assert_eq!(encoded, expected, "Encoding failed for {input}");
            let (decoded, _) = decode_remaining_length(&encoded).unwrap();
            assert_eq!(decoded, input, "Decoding failed for {input}");
        }
    }

    #[test]
    fn test_topic_filter() {
        let filter = TopicFilter::new("sensor/+/temperature");
        assert!(filter.matches("sensor/room1/temperature"));
        assert!(filter.matches("sensor/room2/temperature"));
        assert!(!filter.matches("sensor/room1/humidity"));
        assert!(!filter.matches("sensor/room1/temperature/extra"));

        let multi = TopicFilter::new("sensor/#");
        assert!(multi.matches("sensor/room1/temperature"));
        assert!(multi.matches("sensor/room1"));
        assert!(!multi.matches("actuator/light"));

        let exact = TopicFilter::new("test/topic");
        assert!(exact.matches("test/topic"));
        assert!(!exact.matches("test/topic/extra"));
    }

    #[test]
    fn test_string_encoding() {
        let s = "hello";
        let encoded = encode_string(s);
        assert_eq!(encoded.len(), 2 + 5);
        let (decoded, _) = decode_string(&encoded).unwrap();
        assert_eq!(decoded, s);
    }
}
