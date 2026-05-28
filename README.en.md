# AtomMQTT Broker

> A high-performance MQTT Broker implemented in Rust, supporting MQTT 3.1.1 (v311) and MQTT 5.0 protocols, with a built-in Web management dashboard and CLI test client.

![Language](https://img.shields.io/badge/language-Rust-orange)
![MQTT](https://img.shields.io/badge/MQTT-3.1.1%20%7C%205.0-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Features

- ✅ **SQLite Persistence** — Sessions, subscriptions, retained messages, and will messages are automatically persisted to a local database and restored on restart
- ✅ **Dual Protocol Support** — Both MQTT 3.1.1 and MQTT 5.0 simultaneously
- ✅ **Topic Subscription Tree** — Efficient topic matching via Trie (supports `+` / `#` wildcards)
- ✅ **Message Routing** — PUBLISH messages are automatically forwarded to all matching subscribers
- ✅ **QoS 0/1/2** — Full quality-of-service support
- ✅ **Retained Messages** — Store and distribute retained messages
- ✅ **Will Messages** — Auto-publish on unexpected disconnect
- ✅ **Web Management Dashboard** — Built-in Actix-Web dashboard for real-time broker monitoring
- ✅ **WebSocket Subscriptions** — Subscribe to MQTT topics directly from the browser and receive real-time messages
- ✅ **REST API** — Full HTTP API for publishing messages and managing clients
- ✅ **Anonymous / File-based Authentication** — Support for no-auth and password-file based authentication
- ✅ **ACL Topic Access Control** — File-based publish/subscribe/readwrite permission management
- ✅ **Web UI Authentication** — HTTP Basic Auth + JSON login page dual authentication
- ✅ **CLI Client** — Built-in `mqtt-client` tool supporting publish, subscribe, and interactive shell modes
- ✅ **Performance Metrics** — Built-in counters (connections, messages, bytes, packets, etc.)

---

## Project Structure

```
rust_mqtt_broker/
├── Cargo.toml                 # Workspace configuration
├── mqtt-core/                 # MQTT protocol core
│   ├── src/
│   │   ├── common.rs          #   Common types (QoS, TopicFilter, ProtocolVersion)
│   │   ├── codec.rs           #   Codec public interface
│   │   ├── v3/                #   MQTT 3.1.1 implementation
│   │   │   ├── types.rs       #     Packet type definitions
│   │   │   └── codec.rs       #     Encoder/decoder
│   │   └── v5/                #   MQTT 5.0 implementation
│   │       ├── types.rs       #     Packet type definitions (incl. properties)
│   │       ├── codec.rs       #     Encoder/decoder
│   │       └── properties.rs  #     Property definitions
│   └── Cargo.toml
├── mqtt-broker/               # Broker engine
│   ├── src/
│   │   ├── persistence.rs    #   SQLite persistence (async batch writes)
│   │   ├── lib.rs             #   BrokerState, BrokerMessage, BrokerHandle
│   │   ├── server.rs          #   TCP listener, connection handling, message routing
│   │   ├── config.rs          #   Configuration structures
│   │   ├── session.rs         #   Session state management
│   │   ├── subscription.rs    #   Topic subscription tree (Trie implementation)
│   │   ├── retention.rs       #   Retained message storage
│   │   ├── will.rs            #   Will message management
│   │   ├── metrics.rs         #   Performance metrics collection
│   │   └── auth.rs            #   Authentication & authorization
│   └── Cargo.toml
├── mqtt-web/                  # Web management UI
│   ├── src/
│   │   ├── main.rs            #   Entrypoint: start Broker + Web server
│   │   ├── api.rs             #   REST API + WebSocket endpoints
│   │   └── models.rs          #   Response models
│   ├── static/                #   Frontend static files
│   │   ├── index.html         #   Main page
│   │   ├── login.html         #   Login page
│   │   ├── css/dashboard.css  #   Styles
│   │   └── js/dashboard.js    #   Client logic
│   └── Cargo.toml
├── mqtt-client/               # CLI test client
│   ├── src/main.rs            # Publish / Subscribe / Shell modes
│   └── Cargo.toml
├── Doc/                       # Documentation
│   ├── architecture.md        # Architecture design
│   ├── article.md             # Theory & implementation
│   ├── message-routing.md     # Message routing mechanism
│   ├── protocol-support.md    # MQTT protocol support
│   └── web-api.md             # Web API docs
├── Knowledge/                 # Knowledge base
│   ├── architecture-decisions.md
│   ├── rust-async-patterns.md
│   ├── mqtt-protocol-implementation.md
│   ├── persistence-patterns.md
│   ├── web-management-patterns.md
│   └── acl-auth-patterns.md
├── config.toml                # Broker configuration file
├── passwd                     # Password file (authentication)
├── acl.conf                   # ACL rules file
└── CHANGELOG.md               # Changelog
```

---

## Quick Start

### Prerequisites

- Rust 1.70+ (recommend installing via [rustup](https://rustup.rs/))
- OS: Windows / Linux / macOS

### Build

```bash
# Clone the repository
git clone <repo-url>
cd rust_mqtt_broker

# Build all crates
cargo build --release

# Build only the Web Broker (includes frontend)
cargo build -p mqtt-web --release
```

### Start the Broker

```bash
# Start MQTT Broker + Web dashboard (default ports)
cargo run -p mqtt-web

# Or use release mode
cargo run -p mqtt-web --release
```

After startup:
- MQTT TCP listener: `tcp://0.0.0.0:1883`
- Web dashboard: `http://localhost:8081`
- Database file: `broker.db` (auto-created in the working directory)

> **Note**: The `broker.db` database file is created automatically on first startup, using WAL mode for improved concurrent performance.

### Test with the CLI Client

```bash
# Subscribe to a topic
cargo run -p mqtt-client -- sub 127.0.0.1:1883 "test/#" --client-id sub1

# Publish a message
cargo run -p mqtt-client -- pub 127.0.0.1:1883 "test/hello" "Hello MQTT!" --client-id pub1 --qos 1

# Interactive Shell mode
cargo run -p mqtt-client -- shell 127.0.0.1:1883 --client-id my-shell
```

---

## Web Dashboard

Open `http://localhost:8081` in your browser. The login page appears first:

- **Default Username**: `admin`
- **Default Password**: `admin`

After logging in, the following pages are available:

| Page | Function |
|------|----------|
| 📊 Dashboard | Real-time monitoring: online clients, active subscriptions, message stats, network traffic |
| 👥 Clients | View online client details, manually disconnect clients |
| 📋 Subscriptions | View all active subscriptions (Client ID / Topic filter / QoS) |
| 💾 Retained Messages | View all retained messages |
| 📤 Publish | Publish messages to any topic via HTTP API |
| 📡 Subscribe | **Receive messages in real-time via WebSocket** |
| ℹ️ Server Info | Broker configuration and running status |

> **Embedded Frontend**: All frontend static files (HTML/CSS/JS) are compiled into the binary at build time via the `include_dir!` macro — no disk reads at runtime.
> Deploy a single `.exe` file with zero extra dependencies. Fully cross-platform (Windows / macOS / Linux).

---

## API Reference

### REST API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/login` | User login (JSON) |
| `GET` | `/api/metrics` | Get broker metrics snapshot |
| `GET` | `/api/broker/info` | Get broker config & version info |
| `GET` | `/api/clients` | List all online clients |
| `GET` | `/api/clients/{client_id}` | Get single client details |
| `GET` | `/api/subscriptions` | List all active subscriptions |
| `GET` | `/api/retained` | List all retained messages |
| `DELETE` | `/api/retained/{topic}` | Delete a retained message |
| `POST` | `/api/publish` | Publish a message to a topic |
| `POST` | `/api/clients/{client_id}/disconnect` | Force-disconnect a client |

### WebSocket

| Path | Protocol | Description |
|------|----------|-------------|
| `ws://host:8081/ws/subscribe` | JSON | Real-time MQTT topic subscription |
| `ws://host:8081/mqtt` | Binary MQTT packets | Native WebSocket-MQTT bridge |

> **Authentication**: All `/api/` routes are protected by HTTP Basic Auth. The frontend authenticates via the login page, and subsequent requests carry the credentials automatically. `POST /api/login` is exempt from authentication.

#### WebSocket JSON Commands

**Subscribe to a topic**:
```json
{"type": "subscribe", "topic_filter": "test/#", "qos": 1}
```

**Unsubscribe**:
```json
{"type": "unsubscribe", "topic_filter": "test/#"}
```

**Heartbeat**:
```json
{"type": "ping"}
```

**Receive a message**:
```json
{
  "type": "publish",
  "topic": "test/hello",
  "payload": "Hello MQTT!",
  "qos": 1,
  "source_client": "pub1",
  "timestamp": "2025-01-15T10:30:00+08:00"
}
```

---

## Configuration

The Broker reads settings from a `config.toml` configuration file. A default configuration is auto-generated on first startup. Example:

```toml
[tcp]
host = "0.0.0.0"
port = 1883

[web]
host = "0.0.0.0"
port = 8081

[broker]
max_packet_size = 10485760    # 10 MB
max_qos = 2                    # ExactlyOnce
allow_anonymous = false
session_expiry_interval = 3600

[auth]
method = "file"                # "none" or "file"
auth_file = "passwd"

[web_auth]
enabled = true
username = "admin"
password = "admin"

[acl]
method = "file"                # "none" or "file"
acl_file = "acl.conf"
```

### Persistence

The Broker automatically persists the following data to a SQLite database:

| Data | Table | Recovery Point |
|------|-------|----------------|
| Session info | `sessions` | Broker startup |
| Topic subscriptions | `subscriptions` | Broker startup |
| Retained messages | `retained_messages` | Broker startup |
| Will messages | `will_messages` | Broker startup |

Persistence uses an **asynchronous batch-write** strategy:
- Events are sent via an mpsc channel to a dedicated background writer task
- A batch transaction is triggered every 100ms or when 50 events accumulate
- All pending events are flushed on graceful shutdown

---

## Development

### Running Tests

```bash
# Run all unit tests
cargo test

# Run tests for a specific crate
cargo test -p mqtt-broker
cargo test -p mqtt-core
```

### Debug Mode

```bash
# Enable verbose logging
RUST_LOG=mqtt_broker=debug,mqtt_web=debug cargo run -p mqtt-web
```

---

## License

[MIT](./LICENSE)

Copyright (c) 2025 AtomMQTT
