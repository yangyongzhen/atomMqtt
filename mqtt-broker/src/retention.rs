//! Retained message management.

use mqtt_core::common::QoS;

/// A retained message stored for a topic.
#[derive(Debug, Clone)]
pub struct RetainedMessage {
    /// Topic name.
    pub topic: String,
    /// Message payload.
    pub payload: Vec<u8>,
    /// QoS level of the published message.
    pub qos: QoS,
    /// Timestamp when the message was retained.
    pub timestamp: std::time::Instant,
}

impl RetainedMessage {
    pub fn new(topic: String, payload: Vec<u8>, qos: QoS) -> Self {
        RetainedMessage {
            topic,
            payload,
            qos,
            timestamp: std::time::Instant::now(),
        }
    }
}
