//! Authentication and authorization (including ACL topic-level access control).
//!
//! # ACL File Format
//!
//! Each non-empty, non-comment (`#`) line defines one rule:
//!
//! ```text
//! user <username> topic <publish|subscribe|readwrite> <topic_filter>
//! ```
//!
//! Examples:
//! ```text
//! user admin topic readwrite #
//! user sensor01 topic publish sensor/#
//! user mobile_app topic subscribe notifications/#
//! ```
//!
//! Rules are evaluated **in order**; the first matching rule decides access.
//! If no rule matches, access is **denied** (default-deny).

use crate::config::AuthMethod;
use mqtt_core::v3;
use mqtt_core::v5;
use tracing::{info, warn};

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

/// Simple authenticator (stage 1: authentication).
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
    pub fn authenticate(
        &self,
        allow_anonymous: bool,
        username: Option<&str>,
        password: Option<&str>,
    ) -> AuthResult {
        match &self.method {
            AuthMethod::None => {
                if allow_anonymous {
                    let username = username.unwrap_or("anonymous").to_string();
                    AuthResult::Success { username }
                } else if let Some(user) = username {
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
                    AuthResult::Success { username: "anonymous".to_string() }
                } else {
                    AuthResult::Denied { reason: AuthErrorKind::AnonymousDisallowed }
                }
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// ACL (Access Control List) — stage 3: topic-level authorization
// ──────────────────────────────────────────────────────────────────────────

/// ACL operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclAccess {
    /// Client can publish to matching topics.
    Publish,
    /// Client can subscribe to matching topics.
    Subscribe,
    /// Client can both publish and subscribe.
    ReadWrite,
}

/// A single ACL rule.
#[derive(Debug, Clone)]
struct AclEntry {
    /// The username this rule applies to (literal match).
    username: String,
    /// Topic filter (may contain `+` / `#` wildcards).
    filter: String,
    /// Access type granted.
    access: AclAccess,
}

/// ACL checker: loaded from a file, applies topic-level authorization.
pub struct AclChecker {
    /// ACL entries, in file order.
    entries: Vec<AclEntry>,
    /// Whether ACL is enabled.
    enabled: bool,
}

impl AclChecker {
    /// Create a new ACL checker from an ACL file path.
    /// If `path` is empty or file doesn't exist, ACL is disabled (allow all).
    pub fn new(path: &str) -> Self {
        if path.is_empty() {
            return AclChecker { entries: Vec::new(), enabled: false };
        }
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => {
                eprintln!("[acl] Warning: cannot read ACL file '{}', ACL disabled (allow all)", path);
                return AclChecker { entries: Vec::new(), enabled: false };
            }
        };

        let entries: Vec<AclEntry> = content.lines()
            .filter_map(|line| parse_acl_line(line))
            .collect();

        let enabled = !entries.is_empty();
        if enabled {
            info!("[acl] Loaded {} ACL rules from '{}'", entries.len(), path);
        } else {
            info!("[acl] No ACL rules in '{}', ACL disabled (allow all)", path);
        }

        AclChecker { entries, enabled }
    }

    /// Check if a user is authorised to publish on a topic.
    ///
    /// Rules are evaluated in file order; the **first** matching rule decides.
    /// If no rule matches, access is **denied** (default-deny).
    pub fn authorize_publish(&self, username: &str, topic: &str) -> bool {
        if !self.enabled {
            return true;
        }
        for entry in &self.entries {
            if entry.username == username {
                let allowed = match entry.access {
                    AclAccess::Publish | AclAccess::ReadWrite => true,
                    AclAccess::Subscribe => false,
                };
                if allowed && topic_matches_filter(topic, &entry.filter) {
                    return true;
                }
                // If username matches but access is wrong type, continue (don't deny yet
                // because later rules with same user might grant it)
            }
        }
        // Default-deny
        warn!("[acl] PUBLISH denied: user={}, topic={}", username, topic);
        false
    }

    /// Check if a user is authorised to subscribe to a topic filter.
    pub fn authorize_subscribe(&self, username: &str, topic_filter: &str) -> bool {
        if !self.enabled {
            return true;
        }
        for entry in &self.entries {
            if entry.username == username {
                let allowed = match entry.access {
                    AclAccess::Subscribe | AclAccess::ReadWrite => true,
                    AclAccess::Publish => false,
                };
                if allowed && topic_matches_filter(topic_filter, &entry.filter) {
                    return true;
                }
            }
        }
        warn!("[acl] SUBSCRIBE denied: user={}, filter={}", username, topic_filter);
        false
    }
}

/// Parse a single ACL file line.
/// Format: `user <username> topic <publish|subscribe|readwrite> <topic_filter>`
fn parse_acl_line(line: &str) -> Option<AclEntry> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    let parts: Vec<&str> = line.splitn(5, ' ').collect();
    if parts.len() < 5 || parts[0] != "user" || parts[2] != "topic" {
        eprintln!("[acl] Skipping malformed ACL line: {}", line);
        return None;
    }

    let username = parts[1].to_string();
    let access = match parts[3] {
        "publish" => AclAccess::Publish,
        "subscribe" => AclAccess::Subscribe,
        "readwrite" => AclAccess::ReadWrite,
        other => {
            eprintln!("[acl] Unknown access type '{}' in line: {}", other, line);
            return None;
        }
    };
    let filter = parts[4].to_string();

    Some(AclEntry { username, filter, access })
}

/// Check whether `topic` matches a `filter` that may contain `+` / `#`.
///
/// This re-uses the MQTT standard matching semantics already implemented in
/// `mqtt_core::common::TopicFilter`. We re-implement a simple version here
/// for the ACL checker to avoid a dependency on mqtt_core internals and to
/// keep the logic self-contained.
fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    if filter == "#" {
        return true;
    }

    let topic_segments: Vec<&str> = topic.split('/').collect();
    let filter_segments: Vec<&str> = filter.split('/').collect();

    let mut ti = 0usize;
    let mut fi = 0usize;

    while fi < filter_segments.len() {
        if filter_segments[fi] == "#" {
            // '#' must be the last segment in the filter
            return true;
        }

        if ti >= topic_segments.len() {
            return false;
        }

        if filter_segments[fi] != "+" && filter_segments[fi] != topic_segments[ti] {
            return false;
        }

        ti += 1;
        fi += 1;
    }

    // All filter segments consumed; topic segments must match exactly
    ti == topic_segments.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_matches_filter() {
        assert!(topic_matches_filter("sensor/temp", "sensor/temp"));
        assert!(topic_matches_filter("sensor/temp", "sensor/+"));
        assert!(topic_matches_filter("sensor/room1/temp", "sensor/+/temp"));
        assert!(topic_matches_filter("a/b/c", "#"));
        assert!(topic_matches_filter("anything", "#"));
        assert!(!topic_matches_filter("sensor/temp", "sensor/humidity"));
        assert!(!topic_matches_filter("sensor/temp", "home/#"));
        assert!(topic_matches_filter("sensor/floor1/room1/temp", "sensor/#"));
        assert!(topic_matches_filter("a", "+"));
        assert!(!topic_matches_filter("a/b", "+"));
    }

    #[test]
    fn test_parse_acl_line() {
        let entry = parse_acl_line("user admin topic readwrite #").unwrap();
        assert_eq!(entry.username, "admin");
        assert_eq!(entry.access, AclAccess::ReadWrite);
        assert_eq!(entry.filter, "#");

        let entry = parse_acl_line("user sensor01 topic publish sensor/#").unwrap();
        assert_eq!(entry.username, "sensor01");
        assert_eq!(entry.access, AclAccess::Publish);
        assert_eq!(entry.filter, "sensor/#");

        assert!(parse_acl_line("").is_none());
        assert!(parse_acl_line("# comment").is_none());
        assert!(parse_acl_line("user x topic unknown #").is_none());
    }

    #[test]
    fn test_acl_checker_publish() {
        let checker = AclChecker {
            entries: vec![
                AclEntry { username: "admin".into(), filter: "#".into(), access: AclAccess::ReadWrite },
                AclEntry { username: "sensor".into(), filter: "sensor/#".into(), access: AclAccess::Publish },
                AclEntry { username: "app".into(), filter: "notifications/#".into(), access: AclAccess::Subscribe },
            ],
            enabled: true,
        };

        assert!(checker.authorize_publish("admin", "any/topic"));
        assert!(checker.authorize_publish("sensor", "sensor/temp"));
        assert!(!checker.authorize_publish("sensor", "home/temp")); // not in sensor/#
        assert!(!checker.authorize_publish("app", "notifications/1")); // subscribe only
        assert!(!checker.authorize_publish("unknown", "any/topic")); // no rule
    }

    #[test]
    fn test_acl_checker_subscribe() {
        let checker = AclChecker {
            entries: vec![
                AclEntry { username: "admin".into(), filter: "#".into(), access: AclAccess::ReadWrite },
                AclEntry { username: "app".into(), filter: "notifications/#".into(), access: AclAccess::Subscribe },
            ],
            enabled: true,
        };

        assert!(checker.authorize_subscribe("admin", "any/topic"));
        assert!(checker.authorize_subscribe("app", "notifications/alerts"));
        assert!(!checker.authorize_subscribe("app", "sensor/data")); // not in notifications/#
        assert!(!checker.authorize_subscribe("unknown", "any/topic"));
    }

    #[test]
    fn test_acl_disabled() {
        let checker = AclChecker { entries: vec![], enabled: false };
        assert!(checker.authorize_publish("anyone", "any/topic"));
        assert!(checker.authorize_subscribe("anyone", "any/topic"));
    }
}
