//! MQTT Broker Engine.
//!
//! Core broker logic: connection management, session state, subscription tree,
//! message routing, will messages, retained messages, and metrics.

#![deny(unsafe_code)]

pub mod config;
pub mod session;
pub mod subscription;
pub mod retention;
pub mod will;
pub mod server;
pub mod metrics;
pub mod auth;
pub mod persistence;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::str;
use dashmap::DashMap;
use tokio::sync::mpsc;
use mqtt_core::common::QoS;

/// Broker identifier (UUID v4).
pub const BROKER_ID: &str = env!("CARGO_PKG_NAME");

/// Shared state accessible by all connection handlers.
pub struct BrokerState {
    /// Broker configuration.
    pub config: config::BrokerConfig,
    /// Active sessions, keyed by client_id.
    pub sessions: DashMap<String, session::SessionState>,
    /// Subscription tree for topic matching.
    pub subscriptions: Mutex<subscription::SubscriptionTree>,
    /// Retained messages, keyed by topic.
    pub retained: DashMap<String, retention::RetainedMessage>,
    /// Will messages for disconnected clients.
    pub wills: DashMap<String, will::WillMessage>,
    /// Broker metrics.
    pub metrics: Mutex<metrics::BrokerMetrics>,
    /// Client ID counter for auto-generated IDs.
    pub next_client_id: AtomicU64,
    /// Channel to communicate with the background message router.
    pub broker_handle: std::sync::Mutex<Option<BrokerHandle>>,
    /// Active connections: client_id → sender for forwarding PUBLISH packets.
    pub connections: DashMap<String, tokio::sync::mpsc::UnboundedSender<Vec<u8>>>,
    /// Web subscribers: subscriber_id → sender for forwarding JSON messages.
    pub web_subscribers: DashMap<String, tokio::sync::mpsc::UnboundedSender<String>>,
    /// Persistence layer for sessions, subscriptions, and retained messages.
    pub persistence: Arc<crate::persistence::Persistence>,
    /// Authenticator for MQTT client authentication.
    pub authenticator: crate::auth::Authenticator,
    /// ACL checker for topic-level authorization.
    pub acl: crate::auth::AclChecker,
}

impl BrokerState {
    /// Create a new broker state with the given config and persistence.
    pub fn new(config: config::BrokerConfig, persistence: Arc<crate::persistence::Persistence>) -> Self {
        let auth_method = config.auth_method.clone();
        let acl_path = match &config.acl_method {
            config::AclMethod::File { path } => path.clone(),
            config::AclMethod::None => String::new(),
        };
        BrokerState {
            config,
            sessions: DashMap::new(),
            subscriptions: Mutex::new(subscription::SubscriptionTree::new()),
            retained: DashMap::new(),
            wills: DashMap::new(),
            metrics: Mutex::new(metrics::BrokerMetrics::new()),
            next_client_id: AtomicU64::new(1),
            broker_handle: std::sync::Mutex::new(None),
            connections: DashMap::new(),
            web_subscribers: DashMap::new(),
            persistence,
            authenticator: crate::auth::Authenticator::new(&auth_method),
            acl: crate::auth::AclChecker::new(&acl_path),
        }
    }

    /// Generate a unique anonymous client ID.
    pub fn generate_client_id(&self) -> String {
        use std::sync::atomic::Ordering;
        let id = self.next_client_id.fetch_add(1, Ordering::SeqCst);
        format!("anonymous_{}", id)
    }
}

/// Channel message types for inter-task communication.
pub enum BrokerMessage {
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
        source_client: String,
    },
    ClientDisconnected {
        client_id: String,
        clean_session: bool,
    },
}

/// A handle to communicate with the broker's background processing loop.
#[derive(Clone)]
pub struct BrokerHandle {
    pub sender: mpsc::UnboundedSender<BrokerMessage>,
}
