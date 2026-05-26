//! Client session state management.

use std::time::Instant;
use mqtt_core::common::ProtocolVersion;

/// Client session state.
#[derive(Debug, Clone)]
pub struct SessionState {
    /// Client identifier.
    pub client_id: String,
    /// Protocol version used.
    pub protocol_version: ProtocolVersion,
    /// Whether session is clean/clean_start.
    pub clean_session: bool,
    /// Keep alive interval in seconds.
    pub keep_alive: u16,
    /// Whether the client is currently connected.
    pub connected: bool,
    /// Time when the session was created.
    pub created_at: Instant,
    /// Last activity time.
    pub last_active: Instant,
    /// Username (if authenticated).
    pub username: Option<String>,
}

impl SessionState {
    /// Create a new session state.
    pub fn new(
        client_id: String,
        protocol_version: ProtocolVersion,
        clean_session: bool,
        keep_alive: u16,
        username: Option<String>,
    ) -> Self {
        let now = Instant::now();
        SessionState {
            client_id,
            protocol_version,
            clean_session,
            keep_alive,
            connected: true,
            created_at: now,
            last_active: now,
            username,
        }
    }

    /// Update the last active timestamp.
    pub fn touch(&mut self) {
        self.last_active = Instant::now();
    }

    /// Check if the connection is stale (keep alive timeout).
    pub fn is_stale(&self) -> bool {
        if self.keep_alive == 0 {
            return false;
        }
        // Use 1.5x keep alive as timeout
        let timeout = self.keep_alive as f64 * 1.5;
        self.last_active.elapsed().as_secs_f64() > timeout
    }
}
