//! Broker configuration.

/// Broker configuration.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// TCP bind address.
    pub tcp_host: String,
    /// TCP port.
    pub tcp_port: u16,
    /// Web management interface bind address.
    pub web_host: String,
    /// Web management interface port.
    pub web_port: u16,
    /// Maximum packet size (bytes).
    pub max_packet_size: usize,
    /// Maximum QoS level supported.
    pub max_qos: crate::QoS,
    /// Whether anonymous connections are allowed.
    pub allow_anonymous: bool,
    /// Authentication method.
    pub auth_method: AuthMethod,
    /// Session expiry interval (seconds), 0 = no expiry.
    pub session_expiry_interval: u32,
    /// Persistence directory path. None = disable persistence.
    pub persistence_path: Option<String>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            tcp_host: "0.0.0.0".to_string(),
            tcp_port: 1883,
            web_host: "0.0.0.0".to_string(),
            web_port: 8080,
            max_packet_size: 10 * 1024 * 1024, // 10 MB
            max_qos: crate::QoS::ExactlyOnce,
            allow_anonymous: true,
            auth_method: AuthMethod::None,
            session_expiry_interval: 3600,
            persistence_path: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum AuthMethod {
    None,
    File { path: String },
}

/// Authentication credentials.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}
