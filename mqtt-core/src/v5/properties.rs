//! MQTT 5.0 Properties.

use crate::common::MqttResult;
use crate::common::MqttError;

/// MQTT 5.0 Property IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyId {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    ReasonString = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQoS = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

impl PropertyId {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(PropertyId::PayloadFormatIndicator),
            2 => Some(PropertyId::MessageExpiryInterval),
            3 => Some(PropertyId::ContentType),
            8 => Some(PropertyId::ResponseTopic),
            9 => Some(PropertyId::CorrelationData),
            11 => Some(PropertyId::SubscriptionIdentifier),
            17 => Some(PropertyId::SessionExpiryInterval),
            18 => Some(PropertyId::AssignedClientIdentifier),
            19 => Some(PropertyId::ServerKeepAlive),
            21 => Some(PropertyId::AuthenticationMethod),
            22 => Some(PropertyId::AuthenticationData),
            23 => Some(PropertyId::RequestProblemInformation),
            24 => Some(PropertyId::WillDelayInterval),
            25 => Some(PropertyId::RequestResponseInformation),
            26 => Some(PropertyId::ResponseInformation),
            28 => Some(PropertyId::ServerReference),
            31 => Some(PropertyId::ReasonString),
            33 => Some(PropertyId::ReceiveMaximum),
            34 => Some(PropertyId::TopicAliasMaximum),
            35 => Some(PropertyId::TopicAlias),
            36 => Some(PropertyId::MaximumQoS),
            37 => Some(PropertyId::RetainAvailable),
            38 => Some(PropertyId::UserProperty),
            39 => Some(PropertyId::MaximumPacketSize),
            40 => Some(PropertyId::WildcardSubscriptionAvailable),
            41 => Some(PropertyId::SubscriptionIdentifierAvailable),
            42 => Some(PropertyId::SharedSubscriptionAvailable),
            _ => None,
        }
    }
}

/// A single MQTT 5.0 property value.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Byte(u8),
    TwoByteInteger(u16),
    FourByteInteger(u32),
    VariableByteInteger(u32),
    BinaryData(Vec<u8>),
    UTF8String(String),
    Pair(String, String), // User property
}

/// Collection of MQTT 5.0 properties.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Properties {
    pub inner: Vec<(PropertyId, PropertyValue)>,
}

impl Properties {
    pub fn new() -> Self {
        Properties { inner: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Properties { inner: Vec::with_capacity(cap) }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn add(&mut self, id: PropertyId, value: PropertyValue) {
        self.inner.push((id, value));
    }

    pub fn get(&self, id: PropertyId) -> Vec<&PropertyValue> {
        self.inner.iter()
            .filter(|(k, _)| *k == id)
            .map(|(_, v)| v)
            .collect()
    }

    pub fn get_first(&self, id: PropertyId) -> Option<&PropertyValue> {
        self.inner.iter()
            .find(|(k, _)| *k == id)
            .map(|(_, v)| v)
    }

    /// Encode properties to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (id, value) in &self.inner {
            buf.push(*id as u8);
            match value {
                PropertyValue::Byte(v) => buf.push(*v),
                PropertyValue::TwoByteInteger(v) => buf.extend_from_slice(&v.to_be_bytes()),
                PropertyValue::FourByteInteger(v) => buf.extend_from_slice(&v.to_be_bytes()),
                PropertyValue::VariableByteInteger(v) => {
                    buf.extend_from_slice(&crate::common::encode_remaining_length(*v as usize));
                }
                PropertyValue::BinaryData(v) => {
                    buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
                    buf.extend_from_slice(v);
                }
                PropertyValue::UTF8String(s) => {
                    buf.extend_from_slice(&crate::common::encode_string(s));
                }
                PropertyValue::Pair(key, val) => {
                    buf.extend_from_slice(&crate::common::encode_string(key));
                    buf.extend_from_slice(&crate::common::encode_string(val));
                }
            }
        }
        // Prepend length
        let len = buf.len();
        let mut result = crate::common::encode_remaining_length(len);
        result.extend_from_slice(&buf);
        result
    }

    /// Decode properties from bytes.
    pub fn decode(data: &[u8]) -> MqttResult<(Self, usize)> {
        if data.is_empty() {
            return Ok((Properties::new(), 0));
        }

        let (props_len, len_bytes) = crate::common::decode_remaining_length(data)?;
        if props_len == 0 {
            return Ok((Properties::new(), len_bytes));
        }

        if data.len() < len_bytes + props_len {
            return Err(MqttError::InvalidPacket("Properties data truncated".into()));
        }

        let mut props = Properties::new();
        let mut pos = len_bytes;
        let end = len_bytes + props_len;

        while pos < end {
            let id_byte = data[pos];
            pos += 1;
            let id = PropertyId::from_u8(id_byte)
                .ok_or_else(|| MqttError::InvalidPacket(format!("Unknown property ID: {}", id_byte)))?;

            match id {
                PropertyId::PayloadFormatIndicator
                | PropertyId::RequestProblemInformation
                | PropertyId::RequestResponseInformation
                | PropertyId::MaximumQoS
                | PropertyId::RetainAvailable
                | PropertyId::WildcardSubscriptionAvailable
                | PropertyId::SubscriptionIdentifierAvailable
                | PropertyId::SharedSubscriptionAvailable => {
                    if pos >= end {
                        return Err(MqttError::InvalidPacket("Property data truncated".into()));
                    }
                    props.add(id, PropertyValue::Byte(data[pos]));
                    pos += 1;
                }
                PropertyId::ServerKeepAlive
                | PropertyId::ReceiveMaximum
                | PropertyId::TopicAliasMaximum
                | PropertyId::TopicAlias => {
                    if pos + 2 > end {
                        return Err(MqttError::InvalidPacket("Property data truncated".into()));
                    }
                    let v = u16::from_be_bytes([data[pos], data[pos + 1]]);
                    props.add(id, PropertyValue::TwoByteInteger(v));
                    pos += 2;
                }
                PropertyId::MessageExpiryInterval
                | PropertyId::SessionExpiryInterval
                | PropertyId::WillDelayInterval
                | PropertyId::MaximumPacketSize => {
                    if pos + 4 > end {
                        return Err(MqttError::InvalidPacket("Property data truncated".into()));
                    }
                    let v = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                    props.add(id, PropertyValue::FourByteInteger(v));
                    pos += 4;
                }
                PropertyId::SubscriptionIdentifier => {
                    let (v, consumed) = crate::common::decode_remaining_length(&data[pos..])?;
                    props.add(id, PropertyValue::VariableByteInteger(v as u32));
                    pos += consumed;
                }
                PropertyId::CorrelationData
                | PropertyId::AuthenticationData => {
                    if pos + 2 > end {
                        return Err(MqttError::InvalidPacket("Property data truncated".into()));
                    }
                    let len = (data[pos] as usize) << 8 | (data[pos + 1] as usize);
                    pos += 2;
                    if pos + len > end {
                        return Err(MqttError::InvalidPacket("Binary property truncated".into()));
                    }
                    props.add(id, PropertyValue::BinaryData(data[pos..pos + len].to_vec()));
                    pos += len;
                }
                PropertyId::ContentType
                | PropertyId::ResponseTopic
                | PropertyId::AssignedClientIdentifier
                | PropertyId::AuthenticationMethod
                | PropertyId::ResponseInformation
                | PropertyId::ServerReference
                | PropertyId::ReasonString => {
                    let (s, consumed) = crate::common::decode_string(&data[pos..])?;
                    props.add(id, PropertyValue::UTF8String(s.to_string()));
                    pos += consumed;
                }
                PropertyId::UserProperty => {
                    let (key, consumed) = crate::common::decode_string(&data[pos..])?;
                    pos += consumed;
                    let (val, consumed) = crate::common::decode_string(&data[pos..])?;
                    pos += consumed;
                    props.add(id, PropertyValue::Pair(key.to_string(), val.to_string()));
                }
            }
        }

        Ok((props, end))
    }
}

// Convenience helpers for common properties
impl Properties {
    pub fn session_expiry_interval(&self) -> Option<u32> {
        if let Some(PropertyValue::FourByteInteger(v)) = self.get_first(PropertyId::SessionExpiryInterval) {
            Some(*v)
        } else {
            None
        }
    }

    pub fn receive_maximum(&self) -> Option<u16> {
        if let Some(PropertyValue::TwoByteInteger(v)) = self.get_first(PropertyId::ReceiveMaximum) {
            Some(*v)
        } else {
            None
        }
    }

    pub fn maximum_packet_size(&self) -> Option<u32> {
        if let Some(PropertyValue::FourByteInteger(v)) = self.get_first(PropertyId::MaximumPacketSize) {
            Some(*v)
        } else {
            None
        }
    }

    pub fn topic_alias_maximum(&self) -> Option<u16> {
        if let Some(PropertyValue::TwoByteInteger(v)) = self.get_first(PropertyId::TopicAliasMaximum) {
            Some(*v)
        } else {
            None
        }
    }

    pub fn reason_string(&self) -> Option<&str> {
        if let Some(PropertyValue::UTF8String(s)) = self.get_first(PropertyId::ReasonString) {
            Some(s.as_str())
        } else {
            None
        }
    }

    pub fn will_delay_interval(&self) -> Option<u32> {
        if let Some(PropertyValue::FourByteInteger(v)) = self.get_first(PropertyId::WillDelayInterval) {
            Some(*v)
        } else {
            None
        }
    }
}
