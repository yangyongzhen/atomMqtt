//! Subscription management with topic tree (trie-based) for efficient matching.

use std::collections::HashSet;
use mqtt_core::common::{QoS, TopicFilter};

/// A client's subscription to a topic filter.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Client ID.
    pub client_id: String,
    /// Topic filter string.
    pub filter: String,
    /// Maximum QoS requested.
    pub qos: QoS,
}

/// Node in the topic tree.
#[derive(Debug)]
struct TopicNode {
    /// Subscriptions at this exact level.
    /// Wildcard '+' subscriptions at this level.
    subscriptions: Vec<Subscription>,
    /// Child nodes for each topic segment.
    children: Vec<(String, TopicNode)>,
}

impl TopicNode {
    fn new() -> Self {
        TopicNode {
            subscriptions: Vec::new(),
            children: Vec::new(),
        }
    }
}

/// Topic tree for efficient message routing.
/// Supports wildcards: '+' (single level) and '#' (multi level).
#[derive(Debug)]
pub struct SubscriptionTree {
    root: TopicNode,
}

impl SubscriptionTree {
    /// Create a new empty subscription tree.
    pub fn new() -> Self {
        SubscriptionTree { root: TopicNode::new() }
    }

    /// Add a subscription.
    pub fn subscribe(&mut self, client_id: &str, filter: &str, qos: QoS) {
        let segments: Vec<&str> = filter.split('/').collect();
        let sub = Subscription {
            client_id: client_id.to_string(),
            filter: filter.to_string(),
            qos,
        };
        Self::insert_node(&mut self.root, &segments, sub);
    }

    fn insert_node(node: &mut TopicNode, segments: &[&str], subscription: Subscription) {
        if segments.is_empty() {
            // Check if (client_id, filter) already exists — update QoS instead of duplicating
            if let Some(existing) = node.subscriptions.iter_mut().find(|s| {
                s.client_id == subscription.client_id && s.filter == subscription.filter
            }) {
                existing.qos = subscription.qos;
            } else {
                node.subscriptions.push(subscription);
            }
            return;
        }

        let segment = segments[0];
        let rest = &segments[1..];

        // Find or create child node for this segment
        if let Some(pos) = node.children.iter().position(|(s, _)| s == segment) {
            Self::insert_node(&mut node.children[pos].1, rest, subscription);
        } else {
            let mut child = TopicNode::new();
            Self::insert_node(&mut child, rest, subscription);
            node.children.push((segment.to_string(), child));
        }
    }

    /// Unsubscribe a client from a filter.
    pub fn unsubscribe(&mut self, client_id: &str, filter: &str) {
        let segments: Vec<&str> = filter.split('/').collect();
        Self::remove_subscription(&mut self.root, &segments, client_id);
    }

    fn remove_subscription(node: &mut TopicNode, segments: &[&str], client_id: &str) {
        if segments.is_empty() {
            node.subscriptions.retain(|s| s.client_id != client_id);
            return;
        }

        let segment = segments[0];
        let rest = &segments[1..];

        if let Some(pos) = node.children.iter().position(|(s, _)| s == segment) {
            Self::remove_subscription(&mut node.children[pos].1, rest, client_id);
        }
    }

    /// Remove all subscriptions for a client.
    pub fn unsubscribe_all(&mut self, client_id: &str) {
        Self::remove_client(&mut self.root, client_id);
    }

    fn remove_client(node: &mut TopicNode, client_id: &str) {
        node.subscriptions.retain(|s| s.client_id != client_id);
        // Collect indices to avoid borrow conflicts with recursive call
        let child_indices: Vec<usize> = (0..node.children.len()).collect();
        for i in child_indices {
            Self::remove_client(&mut node.children[i].1, client_id);
        }
    }

    /// Find all subscriptions matching a given topic.
    pub fn lookup(&self, topic: &str) -> Vec<Subscription> {
        let segments: Vec<&str> = topic.split('/').collect();
        let mut results = Vec::new();
        let mut seen = HashSet::new(); // Dedup by (client_id, filter)

        self.collect_matching(&self.root, &segments, &mut results, &mut seen);

        results
    }

    fn collect_matching(
        &self,
        node: &TopicNode,
        segments: &[&str],
        results: &mut Vec<Subscription>,
        seen: &mut HashSet<(String, String)>,
    ) {
        // Check subscriptions at this node (wildcard '#')
        // The '#' wildcard in the tree is stored as a child named "#" at each level
        if let Some(wildcard_node) = node.children.iter().find(|(s, _)| s == "#") {
            for sub in &wildcard_node.1.subscriptions {
                let key = (sub.client_id.clone(), sub.filter.clone());
                if seen.insert(key) {
                    results.push(sub.clone());
                }
            }
        }

        if segments.is_empty() {
            // Reached end of topic, collect subscriptions at this node
            for sub in &node.subscriptions {
                let key = (sub.client_id.clone(), sub.filter.clone());
                if seen.insert(key) {
                    results.push(sub.clone());
                }
            }
            return;
        }

        let segment = segments[0];
        let rest = &segments[1..];

        // Exact match
        if let Some(child) = node.children.iter().find(|(s, _)| s == segment) {
            self.collect_matching(&child.1, rest, results, seen);
        }

        // Single-level wildcard '+'
        if let Some(plus_child) = node.children.iter().find(|(s, _)| s == "+") {
            self.collect_matching(&plus_child.1, rest, results, seen);
        }

        // Multi-level wildcard '#' is already handled at the beginning of this function
    }

    /// Get the total number of subscriptions.
    pub fn count(&self) -> usize {
        self.count_node(&self.root)
    }

    fn count_node(&self, node: &TopicNode) -> usize {
        let mut count = node.subscriptions.len();
        for (_, child) in &node.children {
            count += self.count_node(child);
        }
        count
    }

    /// Get all subscriptions (for management API).
    pub fn all_subscriptions(&self) -> Vec<Subscription> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();
        self.collect_all(&self.root, &mut results, &mut seen);
        results
    }

    fn collect_all(&self, node: &TopicNode, results: &mut Vec<Subscription>, seen: &mut HashSet<(String, String)>) {
        for sub in &node.subscriptions {
            let key = (sub.client_id.clone(), sub.filter.clone());
            if seen.insert(key) {
                results.push(sub.clone());
            }
        }
        for (_, child) in &node.children {
            self.collect_all(child, results, seen);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qos0() -> QoS { QoS::AtMostOnce }
    fn qos1() -> QoS { QoS::AtLeastOnce }

    #[test]
    fn test_exact_match() {
        let mut tree = SubscriptionTree::new();
        tree.subscribe("client1", "sensor/temp", qos0());
        tree.subscribe("client2", "sensor/humidity", qos1());

        let results = tree.lookup("sensor/temp");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].client_id, "client1");
    }

    #[test]
    fn test_plus_wildcard() {
        let mut tree = SubscriptionTree::new();
        tree.subscribe("client1", "sensor/+/temp", qos0());

        let results = tree.lookup("sensor/room1/temp");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].client_id, "client1");

        // Should not match different depth
        let results = tree.lookup("sensor/room1/other/temp");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_hash_wildcard() {
        let mut tree = SubscriptionTree::new();
        tree.subscribe("client1", "sensor/#", qos0());

        let results = tree.lookup("sensor/temp");
        assert_eq!(results.len(), 1);

        let results = tree.lookup("sensor/room1/temp");
        assert_eq!(results.len(), 1);

        let results = tree.lookup("other/topic");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_multiple_subscribers() {
        let mut tree = SubscriptionTree::new();
        tree.subscribe("client1", "topic", qos0());
        tree.subscribe("client2", "topic", qos1());

        let results = tree.lookup("topic");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let mut tree = SubscriptionTree::new();
        tree.subscribe("client1", "topic/a", qos0());
        tree.subscribe("client1", "topic/b", qos0());

        tree.unsubscribe("client1", "topic/a");
        let results = tree.lookup("topic/a");
        assert_eq!(results.len(), 0);

        let results = tree.lookup("topic/b");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_unsubscribe_all() {
        let mut tree = SubscriptionTree::new();
        tree.subscribe("client1", "topic/a", qos0());
        tree.subscribe("client1", "topic/b", qos0());
        tree.subscribe("client2", "topic/a", qos0());

        tree.unsubscribe_all("client1");
        assert_eq!(tree.lookup("topic/a").len(), 1);
        assert_eq!(tree.lookup("topic/b").len(), 0);
    }

    #[test]
    fn test_mixed_wildcards() {
        let mut tree = SubscriptionTree::new();
        tree.subscribe("client1", "+/+/temp", qos0());

        let results = tree.lookup("sensor/room1/temp");
        assert_eq!(results.len(), 1);

        // Must match exactly 3 levels
        let results = tree.lookup("sensor/temp");
        assert_eq!(results.len(), 0);
    }
}
