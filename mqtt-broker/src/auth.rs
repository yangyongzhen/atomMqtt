//! Authentication and authorization.

use crate::config::AuthMethod;
use mqtt_core::v3;
use mqtt_core::v5;

/// Authentication result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthResult {
    /// Authentication successful, with username.
    Success { username: String },
    /// Authentication denied.
    Denied { reason: AuthErrorKind },
}

/// Reason for authentication failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthErrorKind {
    /// Invalid username or password (used with file-based auth).
    BadUsernameOrPassword,
    /// Anonymous connections are not allowed.
    AnonymousDisallowed,
    /// Client not authorized (generic).
    NotAuthorized,
}

impl AuthErrorKind {
    /// Return the MQTT 3.1.1 CONNACK return code for this error.
    pub fn to_v3_return_code(&self) -> v3::types::ConnectReturnCode {
        match self {
            AuthErrorKind::BadUsernameOrPassword => v3::types::ConnectReturnCode::BadUsernameOrPassword,
            AuthErrorKind::AnonymousDisallowed => v3::types::ConnectReturnCode::NotAuthorized,
            AuthErrorKind::NotAuthorized => v3::types::ConnectReturnCode::NotAuthorized,
        }
    }

    /// Return the MQTT 5.0 reason code for this error.
    pub fn to_v5_reason_code(&self) -> v5::types::ReasonCode {
        match self {
            AuthErrorKind::BadUsernameOrPassword => v5::types::ReasonCode::BadUserNameOrPassword,
            AuthErrorKind::AnonymousDisallowed => v5::types::ReasonCode::NotAuthorized,
            AuthErrorKind::NotAuthorized => v5::types::ReasonCode::NotAuthorized,
        }
    }
}

/// Simple authenticator.
pub struct Authenticator {
    method: AuthMethod,
    /// In-memory user database for file-based auth.
    users: Vec<(String, String)>, // (username, password)
}

impl Authenticator {
    /// Create a new authenticator based on config.
    pub fn new(method: &AuthMethod) -> Self {
        let users = match method {
            AuthMethod::None => Vec::new(),
            AuthMethod::File { path } => {
                // Try to read credentials file
                let content = std::fs::read_to_string(path).unwrap_or_default();
                content.lines()
                    .filter_map(|line| {
                        let parts: Vec<&str> = line.splitn(2, ':').collect();
                        if parts.len() == 2 {
                            Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        };

        Authenticator { method: method.clone(), users }
    }

    /// Authenticate a client.
    ///
    /// Parameters:
    /// - `allow_anonymous`: whether the broker allows anonymous connections globally.
    /// - `username`: optional username from CONNECT.
    /// - `password`: optional password from CONNECT.
    pub fn authenticate(
        &self,
        allow_anonymous: bool,
        username: Option<&str>,
        password: Option<&str>,
    ) -> AuthResult {
        match &self.method {
            AuthMethod::None => {
                if allow_anonymous {
                    // Every connection is allowed
                    let username = username.unwrap_or("anonymous").to_string();
                    AuthResult::Success { username }
                } else if let Some(user) = username {
                    // Even with auth method "none", if allow_anonymous is false,
                    // anonymous connections (no username) are rejected,
                    // but any username is accepted.
                    AuthResult::Success { username: user.to_string() }
                } else {
                    AuthResult::Denied { reason: AuthErrorKind::AnonymousDisallowed }
                }
            }
            AuthMethod::File { .. } => {
                if let Some(user) = username {
                    let pass = password.unwrap_or("");
                    if self.users.iter().any(|(u, p)| u == user && p == pass) {
                        AuthResult::Success { username: user.to_string() }
                    } else {
                        AuthResult::Denied { reason: AuthErrorKind::BadUsernameOrPassword }
                    }
                } else if allow_anonymous {
                    // File-based auth but anonymous allowed: skip authentication
                    AuthResult::Success { username: "anonymous".to_string() }
                } else {
                    AuthResult::Denied { reason: AuthErrorKind::AnonymousDisallowed }
                }
            }
        }
    }

    /// Check if a client is authorized to publish to a topic.
    pub fn authorize_publish(&self, _username: &str, _topic: &str) -> bool {
        // Default: allow all. Extend for ACL support.
        true
    }

    /// Check if a client is authorized to subscribe to a topic.
    pub fn authorize_subscribe(&self, _username: &str, _topic: &str) -> bool {
        // Default: allow all. Extend for ACL support.
        true
    }
}
