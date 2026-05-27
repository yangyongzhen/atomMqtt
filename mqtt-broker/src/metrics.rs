//! Broker metrics collection.

use serde::Serialize;

/// Broker metrics for monitoring and management UI.
#[derive(Debug, Default, Serialize, Clone)]
pub struct BrokerMetrics {
    /// Total bytes received.
    pub bytes_received: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
    /// Total messages published.
    pub messages_published: u64,
    /// Total messages received (from clients).
    pub messages_received: u64,
    /// Total subscriptions active.
    pub subscriptions_active: u64,
    /// Total clients connected currently.
    pub clients_connected: u64,
    /// Total clients connected (cumulative).
    pub clients_total: u64,
    /// Total packets received.
    pub packets_received: u64,
    /// Total packets sent.
    pub packets_sent: u64,
    /// Number of rejected connections.
    pub rejected_connections: u64,
    /// Uptime seconds.
    pub uptime_seconds: u64,
}

impl BrokerMetrics {
    pub fn new() -> Self {
        BrokerMetrics::default()
    }

    pub fn increment_bytes_received(&mut self, n: u64) { self.bytes_received += n; }
    pub fn increment_bytes_sent(&mut self, n: u64) { self.bytes_sent += n; }
    pub fn increment_messages_published(&mut self) { self.messages_published += 1; }
    pub fn increment_messages_received(&mut self) { self.messages_received += 1; }
    pub fn increment_clients_connected(&mut self) { self.clients_connected += 1; self.clients_total += 1; }
    pub fn decrement_clients_connected(&mut self) { self.clients_connected = self.clients_connected.saturating_sub(1); }
    pub fn increment_packets_received(&mut self) { self.packets_received += 1; }
    pub fn increment_packets_sent(&mut self) { self.packets_sent += 1; }
    pub fn increment_rejected_connections(&mut self) { self.rejected_connections += 1; }

    /// Snapshot of current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            bytes_received: self.bytes_received,
            bytes_sent: self.bytes_sent,
            messages_published: self.messages_published,
            messages_received: self.messages_received,
            subscriptions_active: self.subscriptions_active,
            clients_connected: self.clients_connected,
            clients_total: self.clients_total,
            packets_received: self.packets_received,
            packets_sent: self.packets_sent,
            rejected_connections: self.rejected_connections,
            uptime_seconds: self.uptime_seconds,
        }
    }
}

/// A point-in-time snapshot of broker metrics.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub messages_published: u64,
    pub messages_received: u64,
    pub subscriptions_active: u64,
    pub clients_connected: u64,
    pub clients_total: u64,
    pub packets_received: u64,
    pub packets_sent: u64,
    pub rejected_connections: u64,
    pub uptime_seconds: u64,
}
