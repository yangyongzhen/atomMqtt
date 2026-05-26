//! Will message management.

use mqtt_core::common::QoS;

/// A will message that should be published when a client disconnects unexpectedly.
#[derive(Debug, Clone)]
pub struct WillMessage {
    /// Client that set this will.
    pub client_id: String,
    /// Will topic.
    pub topic: String,
    /// Will payload.
    pub payload: Vec<u8>,
    /// Will QoS.
    pub qos: QoS,
    /// Will retain flag.
    pub retain: bool,
    /// Delay interval (MQTT 5.0).
    pub delay_interval: u32,
    /// Timestamp when the will was set.
    pub created_at: std::time::Instant,
}
