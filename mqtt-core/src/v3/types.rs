//! MQTT 3.1.1 packet type definitions.

use crate::common::QoS;

/// Unique packet identifier (used for QoS 1/2 flows).
pub type PacketId = u16;

/// ===== CONNECT Packet =====

#[derive(Debug, Clone)]
pub struct ConnectPacket {
    /// Client identifier (unique per client).
    pub client_id: String,
    /// Whether to clean session on connect.
    pub clean_session: bool,
    /// Keep alive interval in seconds.
    pub keep_alive: u16,
    /// Optional will message.
    pub will: Option<Will>,
    /// Optional username.
    pub username: Option<String>,
    /// Optional password.
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Will {
    /// Will topic.
    pub topic: String,
    /// Will message payload.
    pub message: Vec<u8>,
    /// Will QoS.
    pub qos: QoS,
    /// Will retain flag.
    pub retain: bool,
}

/// ===== CONNACK Packet =====

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectReturnCode {
    Accepted = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUsernameOrPassword = 4,
    NotAuthorized = 5,
}

impl ConnectReturnCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(ConnectReturnCode::Accepted),
            1 => Some(ConnectReturnCode::UnacceptableProtocolVersion),
            2 => Some(ConnectReturnCode::IdentifierRejected),
            3 => Some(ConnectReturnCode::ServerUnavailable),
            4 => Some(ConnectReturnCode::BadUsernameOrPassword),
            5 => Some(ConnectReturnCode::NotAuthorized),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone)]
pub struct ConnAckPacket {
    /// Whether the server has stored the session.
    pub session_present: bool,
    /// Return code.
    pub return_code: ConnectReturnCode,
}

/// ===== PUBLISH Packet =====

#[derive(Debug, Clone)]
pub struct PublishPacket {
    /// Topic name.
    pub topic: String,
    /// Payload.
    pub payload: Vec<u8>,
    /// QoS level.
    pub qos: QoS,
    /// Retain flag.
    pub retain: bool,
    /// Packet identifier (only for QoS > 0).
    pub packet_id: Option<PacketId>,
}

/// ===== PUBACK / PUBREC / PUBREL / PUBCOMP =====

#[derive(Debug, Clone)]
pub struct PubAckPacket {
    pub packet_id: PacketId,
}

#[derive(Debug, Clone)]
pub struct PubRecPacket {
    pub packet_id: PacketId,
}

#[derive(Debug, Clone)]
pub struct PubRelPacket {
    pub packet_id: PacketId,
}

#[derive(Debug, Clone)]
pub struct PubCompPacket {
    pub packet_id: PacketId,
}

/// ===== SUBSCRIBE Packet =====

#[derive(Debug, Clone)]
pub struct SubscribePacket {
    pub packet_id: PacketId,
    pub filters: Vec<SubscribeFilter>,
}

#[derive(Debug, Clone)]
pub struct SubscribeFilter {
    pub path: String,
    pub qos: QoS,
}

/// ===== SUBACK Packet =====

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubAckReturnCode {
    SuccessQoS0 = 0,
    SuccessQoS1 = 1,
    SuccessQoS2 = 2,
    Failure = 0x80,
}

impl SubAckReturnCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(SubAckReturnCode::SuccessQoS0),
            1 => Some(SubAckReturnCode::SuccessQoS1),
            2 => Some(SubAckReturnCode::SuccessQoS2),
            0x80 => Some(SubAckReturnCode::Failure),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubAckPacket {
    pub packet_id: PacketId,
    pub return_codes: Vec<SubAckReturnCode>,
}

/// ===== UNSUBSCRIBE Packet =====

#[derive(Debug, Clone)]
pub struct UnsubscribePacket {
    pub packet_id: PacketId,
    pub filters: Vec<String>,
}

/// ===== UNSUBACK Packet =====

#[derive(Debug, Clone)]
pub struct UnsubAckPacket {
    pub packet_id: PacketId,
}

/// ===== PINGREQ / PINGRESP / DISCONNECT =====

#[derive(Debug, Clone)]
pub struct PingReqPacket;

#[derive(Debug, Clone)]
pub struct PingRespPacket;

#[derive(Debug, Clone)]
pub struct DisconnectPacket;

/// ===== Enum of all MQTT 3.1.1 packets =====

#[derive(Debug, Clone)]
pub enum MqttPacketV3 {
    Connect(ConnectPacket),
    ConnAck(ConnAckPacket),
    Publish(PublishPacket),
    PubAck(PubAckPacket),
    PubRec(PubRecPacket),
    PubRel(PubRelPacket),
    PubComp(PubCompPacket),
    Subscribe(SubscribePacket),
    SubAck(SubAckPacket),
    Unsubscribe(UnsubscribePacket),
    UnsubAck(UnsubAckPacket),
    PingReq(PingReqPacket),
    PingResp(PingRespPacket),
    Disconnect(DisconnectPacket),
}
