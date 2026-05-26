//! MQTT 5.0 packet types.

use crate::common::QoS;
use super::properties::Properties;

/// Packet ID type
pub type PacketId = u16;

/// MQTT 5.0 reason codes.
/// Note: Reason codes are contextual - the same byte value may have different
/// semantic meanings depending on the packet type. We use unique discriminant
/// values with a `to_u8()` mapping for the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReasonCode {
    Success = 0,
    GrantedQoS1 = 1,
    GrantedQoS2 = 2,
    DisconnectWithWillMessage = 4,
    NoMatchingSubscribers = 16,
    NoSubscriptionExisted = 17,
    ContinueAuthentication = 24,
    ReAuthenticate = 25,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUserNameOrPassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    ServerShuttingDown = 139,
    BadAuthenticationMethod = 140,
    KeepAliveTimeout = 141,
    SessionTakenOver = 142,
    TopicFilterInvalid = 143,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    PacketIdentifierNotFound = 146,
    ReceiveMaximumExceeded = 147,
    TopicAliasInvalid = 148,
    PacketTooLarge = 149,
    MessageRateTooHigh = 150,
    QuotaExceeded = 151,
    AdministrativeAction = 152,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QoSNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    SharedSubscriptionsNotSupported = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

/// Convenience aliases for reason code byte values that share the same wire format.
impl ReasonCode {
    /// Reason code 0x00 for CONNACK success / SUBACK GrantedQoS0.
    pub const fn normal_disconnection() -> Self { ReasonCode::Success }
    /// Reason code 0x00 for SUBACK.
    pub const fn granted_qos0() -> Self { ReasonCode::Success }
}

impl ReasonCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => ReasonCode::Success,
            1 => ReasonCode::GrantedQoS1,
            2 => ReasonCode::GrantedQoS2,
            4 => ReasonCode::DisconnectWithWillMessage,
            16 => ReasonCode::NoMatchingSubscribers,
            17 => ReasonCode::NoSubscriptionExisted,
            24 => ReasonCode::ContinueAuthentication,
            25 => ReasonCode::ReAuthenticate,
            128 => ReasonCode::UnspecifiedError,
            129 => ReasonCode::MalformedPacket,
            130 => ReasonCode::ProtocolError,
            131 => ReasonCode::ImplementationSpecificError,
            132 => ReasonCode::UnsupportedProtocolVersion,
            133 => ReasonCode::ClientIdentifierNotValid,
            134 => ReasonCode::BadUserNameOrPassword,
            135 => ReasonCode::NotAuthorized,
            136 => ReasonCode::ServerUnavailable,
            137 => ReasonCode::ServerBusy,
            138 => ReasonCode::Banned,
            139 => ReasonCode::ServerShuttingDown,
            140 => ReasonCode::BadAuthenticationMethod,
            141 => ReasonCode::KeepAliveTimeout,
            142 => ReasonCode::SessionTakenOver,
            143 => ReasonCode::TopicFilterInvalid,
            144 => ReasonCode::TopicNameInvalid,
            145 => ReasonCode::PacketIdentifierInUse,
            146 => ReasonCode::PacketIdentifierNotFound,
            147 => ReasonCode::ReceiveMaximumExceeded,
            148 => ReasonCode::TopicAliasInvalid,
            149 => ReasonCode::PacketTooLarge,
            150 => ReasonCode::MessageRateTooHigh,
            151 => ReasonCode::QuotaExceeded,
            152 => ReasonCode::AdministrativeAction,
            153 => ReasonCode::PayloadFormatInvalid,
            154 => ReasonCode::RetainNotSupported,
            155 => ReasonCode::QoSNotSupported,
            156 => ReasonCode::UseAnotherServer,
            157 => ReasonCode::ServerMoved,
            158 => ReasonCode::SharedSubscriptionsNotSupported,
            159 => ReasonCode::ConnectionRateExceeded,
            160 => ReasonCode::MaximumConnectTime,
            161 => ReasonCode::SubscriptionIdentifiersNotSupported,
            162 => ReasonCode::WildcardSubscriptionsNotSupported,
            _ => return None,
        })
    }

    pub fn is_success(&self) -> bool {
        matches!(self, ReasonCode::Success | ReasonCode::GrantedQoS1 | ReasonCode::GrantedQoS2)
    }
}

// ===== CONNECT =====

#[derive(Debug, Clone)]
pub struct ConnectPacket {
    pub client_id: String,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub properties: Properties,
    pub will: Option<Will>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    pub properties: Properties,
    pub delay_interval: u32,
}

// ===== CONNACK =====

#[derive(Debug, Clone)]
pub struct ConnAckPacket {
    pub session_present: bool,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

// ===== PUBLISH =====

#[derive(Debug, Clone)]
pub struct PublishPacket {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    pub packet_id: Option<PacketId>,
    pub properties: Properties,
}

// ===== PUBACK / PUBREC / PUBREL / PUBCOMP =====

#[derive(Debug, Clone)]
pub struct PubAckPacket {
    pub packet_id: PacketId,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

#[derive(Debug, Clone)]
pub struct PubRecPacket {
    pub packet_id: PacketId,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

#[derive(Debug, Clone)]
pub struct PubRelPacket {
    pub packet_id: PacketId,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

#[derive(Debug, Clone)]
pub struct PubCompPacket {
    pub packet_id: PacketId,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

// ===== SUBSCRIBE =====

#[derive(Debug, Clone)]
pub struct SubscribePacket {
    pub packet_id: PacketId,
    pub filters: Vec<SubscribeFilter>,
    pub properties: Properties,
}

#[derive(Debug, Clone)]
pub struct SubscribeFilter {
    pub path: String,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainHandling {
    /// Send retained messages at subscribe time.
    Send = 0,
    /// Send retained only if subscription did not exist.
    SendIfNew = 1,
    /// Do not send retained messages.
    DoNotSend = 2,
}

impl RetainHandling {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v & 0x03 {
            0 => Some(RetainHandling::Send),
            1 => Some(RetainHandling::SendIfNew),
            2 => Some(RetainHandling::DoNotSend),
            _ => None,
        }
    }
}

// ===== SUBACK =====

#[derive(Debug, Clone)]
pub struct SubAckPacket {
    pub packet_id: PacketId,
    pub reason_codes: Vec<ReasonCode>,
    pub properties: Properties,
}

// ===== UNSUBSCRIBE =====

#[derive(Debug, Clone)]
pub struct UnsubscribePacket {
    pub packet_id: PacketId,
    pub filters: Vec<String>,
    pub properties: Properties,
}

// ===== UNSUBACK =====

#[derive(Debug, Clone)]
pub struct UnsubAckPacket {
    pub packet_id: PacketId,
    pub reason_codes: Vec<ReasonCode>,
    pub properties: Properties,
}

// ===== PINGREQ / PINGRESP =====

#[derive(Debug, Clone)]
pub struct PingReqPacket;

#[derive(Debug, Clone)]
pub struct PingRespPacket;

// ===== DISCONNECT =====

#[derive(Debug, Clone)]
pub struct DisconnectPacket {
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

// ===== AUTH =====

#[derive(Debug, Clone)]
pub struct AuthPacket {
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

// ===== Enum of all MQTT 5.0 packets =====

#[derive(Debug, Clone)]
pub enum MqttPacketV5 {
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
    Auth(AuthPacket),
}
