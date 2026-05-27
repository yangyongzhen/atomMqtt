//! Broker configuration.

use serde::Deserialize;

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

/// Configuration file name.
const CONFIG_FILE: &str = "config.toml";

/// Default config.toml content (written when no config file exists).
fn default_config_toml() -> String {
    String::from(
        r###"# ==========================================
# AtomMQTT Broker Configuration
#
# If this file is deleted, it will be regenerated with defaults.
# ==========================================

# MQTT TCP listener settings
[tcp]
host = "0.0.0.0"
port = 1883

# Web management interface settings
[web]
host = "0.0.0.0"
port = 8080

# Broker engine settings
[broker]
# Maximum incoming packet size in bytes (default: 10 MB)
max_packet_size = 10485760
# Maximum QoS level: 0 = AtMostOnce, 1 = AtLeastOnce, 2 = ExactlyOnce
max_qos = 2
# Allow anonymous (unauthenticated) connections
allow_anonymous = true
# Session expiry interval in seconds (0 = never expire)
session_expiry_interval = 3600

# Authentication settings
[auth]
# Authentication method: "none" or "file"
method = "none"
# Path to password file (used when method = "file")
# auth_file = "passwd"

# Persistence (SQLite) settings
[persistence]
# Database file path. Leave empty for default "broker.db"
# db_path = "broker.db"
"###,
    )
}

/// Load configuration from `config.toml`, or create a default config file if none exists.
pub fn load_config() -> BrokerConfig {
    let path = std::path::Path::new(CONFIG_FILE);

    if path.exists() {
        let content = std::fs::read_to_string(path).unwrap_or_else(|e| {
            eprintln!("[config] Warning: cannot read {}, using defaults: {}", CONFIG_FILE, e);
            String::new()
        });

        if content.trim().is_empty() {
            return BrokerConfig::default();
        }

        match toml::from_str::<TomlConfig>(&content) {
            Ok(toml_cfg) => {
                let mut cfg = BrokerConfig::default();
                cfg.tcp_host = toml_cfg.tcp.host;
                cfg.tcp_port = toml_cfg.tcp.port;
                cfg.web_host = toml_cfg.web.host;
                cfg.web_port = toml_cfg.web.port;
                if let Some(v) = toml_cfg.broker.max_packet_size {
                    cfg.max_packet_size = v;
                }
                if let Some(v) = toml_cfg.broker.max_qos {
                    cfg.max_qos = crate::QoS::from_u8(v).unwrap_or(crate::QoS::ExactlyOnce);
                }
                if let Some(v) = toml_cfg.broker.allow_anonymous {
                    cfg.allow_anonymous = v;
                }
                if let Some(v) = toml_cfg.broker.session_expiry_interval {
                    cfg.session_expiry_interval = v;
                }
                cfg.auth_method = match toml_cfg.auth.method.as_str() {
                    "file" => AuthMethod::File {
                        path: toml_cfg.auth.auth_file.unwrap_or_else(|| "passwd".to_string()),
                    },
                    _ => AuthMethod::None,
                };
                cfg.persistence_path = toml_cfg.persistence.db_path;
                cfg
            }
            Err(e) => {
                eprintln!("[config] Error parsing {}: {}", CONFIG_FILE, e);
                eprintln!("[config] Falling back to default configuration.");
                BrokerConfig::default()
            }
        }
    } else {
        // Write default config file
        let content = default_config_toml();
        match std::fs::write(path, &content) {
            Ok(_) => println!("[config] Created default configuration file: {}", CONFIG_FILE),
            Err(e) => eprintln!("[config] Warning: could not create {}: {}", CONFIG_FILE, e),
        }
        BrokerConfig::default()
    }
}

// ---------------------------------------------------------------------------
// TOML config structure (private, mirrors BrokerConfig with flat sections)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TomlConfig {
    #[serde(default)]
    tcp: TcpSection,
    #[serde(default)]
    web: WebSection,
    #[serde(default)]
    broker: BrokerSection,
    #[serde(default)]
    auth: AuthSection,
    #[serde(default)]
    persistence: PersistenceSection,
}

#[derive(Debug, Deserialize)]
struct TcpSection {
    #[serde(default = "default_tcp_host")]
    host: String,
    #[serde(default = "default_tcp_port")]
    port: u16,
}

impl Default for TcpSection {
    fn default() -> Self {
        TcpSection {
            host: default_tcp_host(),
            port: default_tcp_port(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct WebSection {
    #[serde(default = "default_web_host")]
    host: String,
    #[serde(default = "default_web_port")]
    port: u16,
}

impl Default for WebSection {
    fn default() -> Self {
        WebSection {
            host: default_web_host(),
            port: default_web_port(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct BrokerSection {
    max_packet_size: Option<usize>,
    max_qos: Option<u8>,
    allow_anonymous: Option<bool>,
    session_expiry_interval: Option<u32>,
}

impl Default for BrokerSection {
    fn default() -> Self {
        BrokerSection {
            max_packet_size: None,
            max_qos: None,
            allow_anonymous: None,
            session_expiry_interval: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AuthSection {
    #[serde(default = "default_auth_method")]
    method: String,
    auth_file: Option<String>,
}

impl Default for AuthSection {
    fn default() -> Self {
        AuthSection {
            method: default_auth_method(),
            auth_file: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PersistenceSection {
    db_path: Option<String>,
}

impl Default for PersistenceSection {
    fn default() -> Self {
        PersistenceSection { db_path: None }
    }
}

fn default_tcp_host() -> String {
    "0.0.0.0".to_string()
}
fn default_tcp_port() -> u16 {
    1883
}
fn default_web_host() -> String {
    "0.0.0.0".to_string()
}
fn default_web_port() -> u16 {
    8080
}
fn default_auth_method() -> String {
    "none".to_string()
}
