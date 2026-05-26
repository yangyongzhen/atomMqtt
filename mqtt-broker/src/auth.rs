//! Authentication and authorization.

use crate::config::AuthMethod;
use crate::config::Credentials;

/// Authentication result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthResult {
    /// Authentication successful, with optional username.
    Success { username: String },
    /// Authentication failed.
    Denied,
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
    pub fn authenticate(&self, credentials: Option<&Credentials>) -> AuthResult {
        match &self.method {
            AuthMethod::None => {
                // Allow any connection
                let username = credentials.map(|c| c.username.clone()).unwrap_or_else(|| "anonymous".to_string());
                AuthResult::Success { username }
            }
            AuthMethod::File { .. } => {
                if let Some(creds) = credentials {
                    if self.users.iter().any(|(u, p)| u == &creds.username && p == &creds.password) {
                        AuthResult::Success { username: creds.username.clone() }
                    } else {
                        AuthResult::Denied
                    }
                } else {
                    // Credentials required
                    AuthResult::Denied
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
