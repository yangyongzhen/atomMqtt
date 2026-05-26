//! API response models.

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ClientInfo {
    pub client_id: String,
    pub protocol_version: String,
    pub connected: bool,
    pub keep_alive: u16,
    pub username: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionInfo {
    pub client_id: String,
    pub filter: String,
    pub qos: String,
}

#[derive(Debug, Serialize)]
pub struct RetainedMessageInfo {
    pub topic: String,
    pub qos: String,
    pub payload_size: usize,
    pub payload_preview: String,
}

#[derive(Debug, Serialize)]
pub struct ConfigInfo {
    pub tcp_host: String,
    pub tcp_port: u16,
    pub web_host: String,
    pub web_port: u16,
    pub max_packet_size: usize,
    pub allow_anonymous: bool,
    pub session_expiry_interval: u32,
}

#[derive(Debug, Serialize)]
pub struct BrokerInfoResponse {
    pub version: String,
    pub name: String,
    pub uptime_seconds: u64,
    pub config: ConfigInfo,
    pub protocol_versions: Vec<String>,
}
