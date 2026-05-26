//! SQLite persistence layer for broker state.
//!
//! Persists sessions, subscriptions, retained messages, and will messages
//! to a SQLite database file. Uses WAL mode for crash safety and concurrent reads.
//! Writes are batched through a background channel worker for minimal hot-path impact.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::Connection;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::config::BrokerConfig;
use crate::retention::RetainedMessage;
use crate::session::SessionState;
use crate::will::WillMessage;
use mqtt_core::common::ProtocolVersion;

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Events that trigger persistence writes.
#[derive(Debug)]
pub enum PersistEvent {
    /// Save/update a session.
    SaveSession {
        client_id: String,
        protocol_version: i32,
        clean_session: bool,
        keep_alive: u16,
        username: Option<String>,
    },
    /// Remove a session.
    RemoveSession(String),
    /// Save or update a subscription (upsert by client_id + filter).
    SaveSubscription {
        client_id: String,
        filter: String,
        qos: i32,
    },
    /// Remove a single subscription.
    RemoveSubscription {
        client_id: String,
        filter: String,
    },
    /// Remove all subscriptions for a client.
    RemoveClientSubscriptions(String),
    /// Save or update a retained message (upsert by topic).
    SaveRetained {
        topic: String,
        payload: Vec<u8>,
        qos: i32,
    },
    /// Delete a retained message by topic.
    RemoveRetained(String),
    /// Save a will message.
    SaveWill {
        client_id: String,
        topic: String,
        payload: Vec<u8>,
        qos: i32,
        retain: bool,
        delay_interval: u32,
    },
    /// Remove a will message by client_id.
    RemoveWill(String),
    /// Graceful shutdown signal.
    Shutdown,
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

/// SQLite persistence engine.
///
/// Load methods are called synchronously at startup.
/// Writes are dispatched via `send_event()` and processed in a background
/// async task with batching (every 100 ms or every 50 events, whichever comes first).
pub struct Persistence {
    db: Arc<Mutex<Connection>>,
    tx: mpsc::UnboundedSender<PersistEvent>,
}

impl Persistence {
    /// Open (or create) the database, run migrations, and start the background writer.
    pub fn open(config: &BrokerConfig) -> Result<Self, rusqlite::Error> {
        let path = config
            .persistence_path
            .as_deref()
            .unwrap_or("broker.db");

        let db = Connection::open(path)?;

        // WAL mode → better concurrency, crash durability
        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )?;

        // Create tables
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS sessions (
                client_id       TEXT PRIMARY KEY,
                protocol_version INTEGER NOT NULL DEFAULT 4,
                clean_session   INTEGER NOT NULL DEFAULT 0,
                keep_alive      INTEGER NOT NULL DEFAULT 60,
                username        TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                last_active     TEXT NOT NULL DEFAULT (datetime('now'))
             );

             CREATE TABLE IF NOT EXISTS subscriptions (
                client_id TEXT NOT NULL,
                filter    TEXT NOT NULL,
                qos       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (client_id, filter)
             );

             CREATE TABLE IF NOT EXISTS retained_messages (
                topic   TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                qos     INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
             );

             CREATE TABLE IF NOT EXISTS will_messages (
                client_id      TEXT PRIMARY KEY,
                topic          TEXT NOT NULL,
                payload        BLOB NOT NULL,
                qos            INTEGER NOT NULL DEFAULT 0,
                retain         INTEGER NOT NULL DEFAULT 0,
                delay_interval INTEGER NOT NULL DEFAULT 0,
                created_at     TEXT NOT NULL DEFAULT (datetime('now'))
             );",
        )?;

        let db = Arc::new(Mutex::new(db));
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn background writer
        let bg_db = Arc::clone(&db);
        tokio::spawn(async move {
            bg_writer(bg_db, rx).await;
        });

        info!("Persistence: database opened at '{}' (WAL mode)", path);
        Ok(Self { db, tx })
    }

    // -----------------------------------------------------------------------
    // Load helpers (called at startup)
    // -----------------------------------------------------------------------

    /// Load all sessions. `connected` is set to `false`; timestamps are reset.
    pub fn load_sessions(&self) -> Vec<SessionState> {
        let db = self.db.lock().unwrap();
        let mut stmt = match db.prepare(
            "SELECT client_id, protocol_version, clean_session, keep_alive, username
             FROM sessions",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("Persistence: failed to prepare load_sessions: {}", e);
                return Vec::new();
            }
        };

        let now = std::time::Instant::now();
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, u16>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .unwrap();

        rows.filter_map(|r| {
            r.ok().map(|(cid, pv, cs, ka, un)| SessionState {
                client_id: cid,
                protocol_version: match pv {
                    5 => ProtocolVersion::V5,
                    _ => ProtocolVersion::V311,
                },
                clean_session: cs,
                keep_alive: ka,
                connected: false,
                created_at: now,
                last_active: now,
                username: un,
            })
        })
        .collect()
    }

    /// Load all subscriptions as `(client_id, filter, qos)` tuples.
    pub fn load_subscriptions(&self) -> Vec<(String, String, crate::QoS)> {
        let db = self.db.lock().unwrap();
        let mut stmt = match db.prepare(
            "SELECT client_id, filter, qos FROM subscriptions",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("Persistence: failed to prepare load_subscriptions: {}", e);
                return Vec::new();
            }
        };

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i32>(2)?,
            ))
        }).unwrap();

        rows.filter_map(|r| {
            r.ok().map(|(cid, f, q)| {
                let qos = match q {
                    2 => crate::QoS::ExactlyOnce,
                    1 => crate::QoS::AtLeastOnce,
                    _ => crate::QoS::AtMostOnce,
                };
                (cid, f, qos)
            })
        })
        .collect()
    }

    /// Load all retained messages.
    pub fn load_retained(&self) -> Vec<RetainedMessage> {
        let db = self.db.lock().unwrap();
        let mut stmt = match db.prepare(
            "SELECT topic, payload, qos FROM retained_messages",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("Persistence: failed to prepare load_retained: {}", e);
                return Vec::new();
            }
        };

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i32>(2)?,
            ))
        }).unwrap();

        rows.filter_map(|r| {
            r.ok().map(|(topic, payload, qos)| {
                let qos = match qos {
                    2 => crate::QoS::ExactlyOnce,
                    1 => crate::QoS::AtLeastOnce,
                    _ => crate::QoS::AtMostOnce,
                };
                RetainedMessage::new(topic, payload, qos)
            })
        })
        .collect()
    }

    /// Load all will messages.
    pub fn load_wills(&self) -> Vec<WillMessage> {
        let db = self.db.lock().unwrap();
        let mut stmt = match db.prepare(
            "SELECT client_id, topic, payload, qos, retain, delay_interval
             FROM will_messages",
        ) {
            Ok(s) => s,
            Err(e) => {
                error!("Persistence: failed to prepare load_wills: {}", e);
                return Vec::new();
            }
        };

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i32>(3)?,
                row.get::<_, bool>(4)?,
                row.get::<_, u32>(5)?,
            ))
        }).unwrap();

        rows.filter_map(|r| {
            r.ok().map(|(cid, topic, payload, qos, retain, di)| {
                let qos = match qos {
                    2 => crate::QoS::ExactlyOnce,
                    1 => crate::QoS::AtLeastOnce,
                    _ => crate::QoS::AtMostOnce,
                };
                WillMessage {
                    client_id: cid,
                    topic,
                    payload,
                    qos,
                    retain,
                    delay_interval: di,
                    created_at: std::time::Instant::now(),
                }
            })
        })
        .collect()
    }

    /// Send an event to the background writer. Non-blocking.
    pub fn send_event(&self, event: PersistEvent) {
        if let Err(e) = self.tx.send(event) {
            error!("Persistence: failed to send event: {}", e);
        }
    }

    /// Signal graceful shutdown and wait for pending writes to finish.
    pub async fn shutdown(&self) {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let _ = self.tx.send(PersistEvent::Shutdown);
        // Give the writer a moment to flush
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(done_tx);
        drop(done_rx);
        info!("Persistence: shut down");
    }
}

// ---------------------------------------------------------------------------
// Background writer
// ---------------------------------------------------------------------------

async fn bg_writer(db: Arc<Mutex<Connection>>, mut rx: mpsc::UnboundedReceiver<PersistEvent>) {
    let mut batch: Vec<PersistEvent> = Vec::with_capacity(64);
    let flush_interval = Duration::from_millis(100);

    loop {
        // Wait for at least one event, or the flush timeout
        let timed_out = {
            let timeout = tokio::time::sleep(flush_interval);
            tokio::pin!(timeout);

            tokio::select! {
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(PersistEvent::Shutdown) | None => {
                            // Drain remaining events and flush before exit
                            while let Ok(e) = rx.try_recv() {
                                match e {
                                    PersistEvent::Shutdown => break,
                                    other => batch.push(other),
                                }
                            }
                            flush_all(&db, &batch);
                            info!("Persistence: background writer stopped");
                            return;
                        }
                        Some(event) => {
                            batch.push(event);
                            false // didn't time out
                        }
                    }
                }
                _ = &mut timeout => {
                    true // timed out
                }
            }
        };

        // Batch up any additional events that arrived during processing
        if !timed_out {
            // After receiving an event, try to collect more without waiting
            while let Ok(event) = rx.try_recv() {
                match event {
                    PersistEvent::Shutdown => {
                        flush_all(&db, &batch);
                        info!("Persistence: background writer stopped");
                        return;
                    }
                    other => {
                        batch.push(other);
                        if batch.len() >= 50 {
                            flush_all(&db, &batch);
                            batch.clear();
                        }
                    }
                }
            }
        }

        // Flush accumulated batch
        if !batch.is_empty() {
            flush_all(&db, &batch);
            batch.clear();
        }
    }
}

/// Execute a batch of events inside a single transaction.
fn flush_all(db: &Mutex<Connection>, batch: &[PersistEvent]) {
    let conn = match db.lock() {
        Ok(c) => c,
        Err(poisoned) => {
            error!("Persistence: DB mutex poisoned, skipping batch of {} events", batch.len());
            return;
        }
    };

    // Use a transaction for atomic batch write
    if let Err(e) = conn.execute("BEGIN IMMEDIATE", []) {
        error!("Persistence: failed to begin transaction: {}", e);
        return;
    }

    let mut count = 0u32;
    for event in batch {
        match event {
            PersistEvent::SaveSession { client_id, protocol_version, clean_session, keep_alive, username } => {
                let sql = "INSERT INTO sessions (client_id, protocol_version, clean_session, keep_alive, username, last_active)
                           VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
                           ON CONFLICT(client_id) DO UPDATE SET
                               protocol_version = excluded.protocol_version,
                               clean_session    = excluded.clean_session,
                               keep_alive       = excluded.keep_alive,
                               username         = excluded.username,
                               last_active      = datetime('now')";
                if let Err(e) = conn.execute(sql, rusqlite::params![client_id, protocol_version, *clean_session as i32, *keep_alive, username]) {
                    error!("Persistence: SaveSession failed for {}: {}", client_id, e);
                }
                count += 1;
            }
            PersistEvent::RemoveSession(client_id) => {
                if let Err(e) = conn.execute("DELETE FROM sessions WHERE client_id = ?1", rusqlite::params![client_id]) {
                    error!("Persistence: RemoveSession failed for {}: {}", client_id, e);
                }
                count += 1;
            }
            PersistEvent::SaveSubscription { client_id, filter, qos } => {
                let sql = "INSERT INTO subscriptions (client_id, filter, qos)
                           VALUES (?1, ?2, ?3)
                           ON CONFLICT(client_id, filter) DO UPDATE SET qos = excluded.qos";
                if let Err(e) = conn.execute(sql, rusqlite::params![client_id, filter, qos]) {
                    error!("Persistence: SaveSubscription failed for {} / {}: {}", client_id, filter, e);
                }
                count += 1;
            }
            PersistEvent::RemoveSubscription { client_id, filter } => {
                if let Err(e) = conn.execute("DELETE FROM subscriptions WHERE client_id = ?1 AND filter = ?2", rusqlite::params![client_id, filter]) {
                    error!("Persistence: RemoveSubscription failed for {} / {}: {}", client_id, filter, e);
                }
                count += 1;
            }
            PersistEvent::RemoveClientSubscriptions(client_id) => {
                if let Err(e) = conn.execute("DELETE FROM subscriptions WHERE client_id = ?1", rusqlite::params![client_id]) {
                    error!("Persistence: RemoveClientSubscriptions failed for {}: {}", client_id, e);
                }
                count += 1;
            }
            PersistEvent::SaveRetained { topic, payload, qos } => {
                let sql = "INSERT INTO retained_messages (topic, payload, qos, created_at)
                           VALUES (?1, ?2, ?3, datetime('now'))
                           ON CONFLICT(topic) DO UPDATE SET
                               payload    = excluded.payload,
                               qos        = excluded.qos,
                               created_at = datetime('now')";
                if let Err(e) = conn.execute(sql, rusqlite::params![topic, payload, qos]) {
                    error!("Persistence: SaveRetained failed for {}: {}", topic, e);
                }
                count += 1;
            }
            PersistEvent::RemoveRetained(topic) => {
                if let Err(e) = conn.execute("DELETE FROM retained_messages WHERE topic = ?1", rusqlite::params![topic]) {
                    error!("Persistence: RemoveRetained failed for {}: {}", topic, e);
                }
                count += 1;
            }
            PersistEvent::SaveWill { client_id, topic, payload, qos, retain, delay_interval } => {
                let sql = "INSERT INTO will_messages (client_id, topic, payload, qos, retain, delay_interval, created_at)
                           VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))
                           ON CONFLICT(client_id) DO UPDATE SET
                               topic          = excluded.topic,
                               payload        = excluded.payload,
                               qos            = excluded.qos,
                               retain         = excluded.retain,
                               delay_interval = excluded.delay_interval,
                               created_at     = datetime('now')";
                if let Err(e) = conn.execute(sql, rusqlite::params![client_id, topic, payload, qos, *retain as i32, delay_interval]) {
                    error!("Persistence: SaveWill failed for {}: {}", client_id, e);
                }
                count += 1;
            }
            PersistEvent::RemoveWill(client_id) => {
                if let Err(e) = conn.execute("DELETE FROM will_messages WHERE client_id = ?1", rusqlite::params![client_id]) {
                    error!("Persistence: RemoveWill failed for {}: {}", client_id, e);
                }
                count += 1;
            }
            PersistEvent::Shutdown => continue,
        }
    }

    if let Err(e) = conn.execute("COMMIT", []) {
        error!("Persistence: failed to commit transaction: {}", e);
        // Attempt rollback
        let _ = conn.execute("ROLLBACK", []);
    } else {
        debug!("Persistence: flushed {} events", count);
    }
}
