//! REST API endpoints for the broker management interface.

use std::sync::Arc;
use actix_web::{web, get, post, HttpResponse, Responder};
use bytes::Buf;
use serde::Deserialize;

use mqtt_broker::BrokerState;
use mqtt_core::common::QoS;

use crate::models::*;

/// GET /api/metrics - Broker metrics snapshot.
#[get("/api/metrics")]
pub async fn get_metrics(state: web::Data<Arc<BrokerState>>) -> impl Responder {
    let snapshot = state.metrics.lock().unwrap().snapshot();
    HttpResponse::Ok().json(snapshot)
}

/// GET /api/broker/info - Broker general information.
#[get("/api/broker/info")]
pub async fn get_broker_info(state: web::Data<Arc<BrokerState>>) -> impl Responder {
    let metrics = state.metrics.lock().unwrap();
    let info = BrokerInfoResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        name: "AtomMQTT Broker".to_string(),
        uptime_seconds: metrics.uptime_seconds,
        config: ConfigInfo {
            tcp_host: state.config.tcp_host.clone(),
            tcp_port: state.config.tcp_port,
            web_host: state.config.web_host.clone(),
            web_port: state.config.web_port,
            max_packet_size: state.config.max_packet_size,
            allow_anonymous: state.config.allow_anonymous,
            session_expiry_interval: state.config.session_expiry_interval,
        },
        protocol_versions: vec!["MQTT 3.1.1".to_string(), "MQTT 5.0".to_string()],
    };
    HttpResponse::Ok().json(info)
}

/// GET /api/clients - List all connected clients.
#[get("/api/clients")]
pub async fn get_clients(state: web::Data<Arc<BrokerState>>) -> impl Responder {
    let clients: Vec<ClientInfo> = state.sessions.iter()
        .filter(|entry| entry.connected)
        .map(|entry| {
            ClientInfo {
                client_id: entry.client_id.clone(),
                protocol_version: format!("{:?}", entry.protocol_version),
                connected: entry.connected,
                keep_alive: entry.keep_alive,
                username: entry.username.clone().unwrap_or_default(),
                created_at: format!("{:?}", entry.created_at),
            }
        })
        .collect();
    HttpResponse::Ok().json(clients)
}

/// GET /api/clients/{client_id} - Detailed client info.
#[get("/api/clients/{client_id}")]
pub async fn get_client_detail(
    state: web::Data<Arc<BrokerState>>,
    path: web::Path<String>,
) -> impl Responder {
    let client_id = path.into_inner();
    match state.sessions.get(&client_id) {
        Some(session) => {
            HttpResponse::Ok().json(ClientInfo {
                client_id: session.client_id.clone(),
                protocol_version: format!("{:?}", session.protocol_version),
                connected: session.connected,
                keep_alive: session.keep_alive,
                username: session.username.clone().unwrap_or_default(),
                created_at: format!("{:?}", session.created_at),
            })
        }
        None => HttpResponse::NotFound().body(format!("Client '{}' not found", client_id)),
    }
}

/// GET /api/subscriptions - List all subscriptions.
#[get("/api/subscriptions")]
pub async fn get_subscriptions(state: web::Data<Arc<BrokerState>>) -> impl Responder {
    let subs: Vec<SubscriptionInfo> = state.subscriptions.lock().unwrap().all_subscriptions().into_iter().map(|s| {
        SubscriptionInfo {
            client_id: s.client_id,
            filter: s.filter,
            qos: s.qos.to_string(),
        }
    }).collect();
    HttpResponse::Ok().json(subs)
}

/// GET /api/retained - List all retained messages.
#[get("/api/retained")]
pub async fn get_retained_messages(state: web::Data<Arc<BrokerState>>) -> impl Responder {
    let messages: Vec<RetainedMessageInfo> = state.retained.iter().map(|entry| {
        RetainedMessageInfo {
            topic: entry.topic.clone(),
            qos: entry.qos.to_string(),
            payload_size: entry.payload.len(),
            payload_preview: String::from_utf8_lossy(&entry.payload[..entry.payload.len().min(50)]).to_string(),
        }
    }).collect();
    HttpResponse::Ok().json(messages)
}

/// POST /api/publish - Publish a test message via the API.
#[derive(Deserialize)]
pub struct PublishRequest {
    pub topic: String,
    pub payload: String,
    #[serde(default = "default_qos")]
    pub qos: u8,
    #[serde(default)]
    pub retain: bool,
}

fn default_qos() -> u8 { 0 }

#[post("/api/publish")]
pub async fn publish_message(
    state: web::Data<Arc<BrokerState>>,
    body: web::Json<PublishRequest>,
) -> impl Responder {
    let qos = QoS::from_u8(body.qos).unwrap_or(QoS::AtMostOnce);
    let payload = body.payload.as_bytes().to_vec();

    // Send message via broker's routing system so it reaches all subscribers
    if let Some(ref handle) = *state.broker_handle.lock().unwrap() {
        let _ = handle.sender.send(mqtt_broker::BrokerMessage::Publish {
            topic: body.topic.clone(),
            payload: payload.clone(),
            qos,
            retain: body.retain,
            source_client: "web-ui".to_string(),
        });
    }

    // Handle retained messages
    if body.retain && !body.payload.is_empty() {
        state.retained.insert(body.topic.clone(), mqtt_broker::retention::RetainedMessage::new(
            body.topic.clone(), payload.clone(), qos,
        ));
    }

    state.metrics.lock().unwrap().increment_messages_published();

    // Count subscribers for feedback
    let subscriber_count = {
        let subs = state.subscriptions.lock().unwrap();
        subs.lookup(&body.topic).len()
    };

    HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "topic": body.topic,
        "subscriber_count": subscriber_count,
    }))
}

/// GET /ws/subscribe - WebSocket endpoint for subscribing to MQTT topics.
pub async fn ws_subscribe(
    req: actix_web::HttpRequest,
    body: actix_web::web::Payload,
    state: actix_web::web::Data<std::sync::Arc<mqtt_broker::BrokerState>>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let (response, session, msg_stream) = actix_ws::handle(&req, body)?;

    let state = state.get_ref().clone();

    // Use actix_web::rt::spawn (the officially recommended pattern for WS handlers)
    actix_web::rt::spawn(handle_ws_session(state, session, msg_stream));

    Ok(response)
}

/// Handle WebSocket session: subscribe/unsubscribe commands and forward publishes.
async fn handle_ws_session(
    state: std::sync::Arc<mqtt_broker::BrokerState>,
    mut session: actix_ws::Session,
    mut msg_stream: actix_ws::MessageStream,
) {
    use futures_util::StreamExt as _;

    let subscriber_id = format!("webui-sub-{}", uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("x"));
    let mut active_subscriptions: Vec<String> = Vec::new();
    let (fwd_tx, mut fwd_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    loop {
        tokio::select! {
            Some(Ok(msg)) = msg_stream.next() => {
                match msg {
                    actix_ws::Message::Text(text) => {
                        let cmd: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => {
                                let _ = session.text(r#"{"type":"error","message":"invalid JSON"}"#).await;
                                continue;
                            }
                        };

                        let cmd_type = cmd["type"].as_str().unwrap_or("");

                        match cmd_type {
                            "subscribe" => {
                                let topic_filter = cmd["topic_filter"].as_str().unwrap_or("");
                                let qos_val = cmd["qos"].as_u64().unwrap_or(0);
                                let qos = match qos_val {
                                    0 => mqtt_core::common::QoS::AtMostOnce,
                                    1 => mqtt_core::common::QoS::AtLeastOnce,
                                    2 => mqtt_core::common::QoS::ExactlyOnce,
                                    _ => mqtt_core::common::QoS::AtMostOnce,
                                };

                                if topic_filter.is_empty() {
                                    let _ = session.text(r#"{"type":"error","message":"topic_filter required"}"#).await;
                                    continue;
                                }

                                state.subscriptions.lock().unwrap()
                                    .subscribe(&subscriber_id, topic_filter, qos);

                                if !state.web_subscribers.contains_key(&subscriber_id) {
                                    state.web_subscribers.insert(subscriber_id.clone(), fwd_tx.clone());
                                }

                                if !active_subscriptions.contains(&topic_filter.to_string()) {
                                    active_subscriptions.push(topic_filter.to_string());
                                }

                                let resp = serde_json::json!({
                                    "type": "subscribed",
                                    "topic_filter": topic_filter,
                                    "qos": qos_val,
                                });
                                let _ = session.text(resp.to_string()).await;
                                tracing::info!("Web subscribed: {} to {}", subscriber_id, topic_filter);
                            }
                            "unsubscribe" => {
                                let topic_filter = cmd["topic_filter"].as_str().unwrap_or("");
                                if !topic_filter.is_empty() {
                                    state.subscriptions.lock().unwrap()
                                        .unsubscribe(&subscriber_id, topic_filter);
                                    active_subscriptions.retain(|f| f != topic_filter);

                                    let resp = serde_json::json!({
                                        "type": "unsubscribed",
                                        "topic_filter": topic_filter,
                                    });
                                    let _ = session.text(resp.to_string()).await;
                                    tracing::info!("Web unsubscribed: {} from {}", subscriber_id, topic_filter);
                                }
                            }
                            "ping" => {
                                let _ = session.text(r#"{"type":"pong"}"#).await;
                            }
                            _ => {
                                let _ = session.text(r#"{"type":"error","message":"unknown command"}"#).await;
                            }
                        }
                    }
                    actix_ws::Message::Ping(bytes) => {
                        let _ = session.pong(&bytes).await;
                    }
                    actix_ws::Message::Close(_) => break,
                    _ => {}
                }
            }
            Some(json_msg) = fwd_rx.recv() => {
                if session.text(json_msg).await.is_err() {
                    break;
                }
            }
            else => break,
        }
    }

    // Cleanup
    for filter in &active_subscriptions {
        state.subscriptions.lock().unwrap().unsubscribe(&subscriber_id, filter);
    }
    state.web_subscribers.remove(&subscriber_id);
    tracing::info!("Web subscriber disconnected: {}", subscriber_id);
}

/// POST /api/clients/{client_id}/disconnect - Force disconnect a client.
#[post("/api/clients/{client_id}/disconnect")]
pub async fn disconnect_client(
    state: web::Data<Arc<BrokerState>>,
    path: web::Path<String>,
) -> impl Responder {
    let client_id = path.into_inner();
    match state.sessions.get_mut(&client_id) {
        Some(mut session) => {
            session.connected = false;
            HttpResponse::Ok().json(serde_json::json!({
                "success": true,
                "message": format!("Client '{}' will be disconnected", client_id),
            }))
        }
        None => HttpResponse::NotFound().body(format!("Client '{}' not found", client_id)),
    }
}

/// GET /mqtt - Standard MQTT-over-WebSocket endpoint.
///
/// Allows standard MQTT clients (MQTT.js, Paho, mosquitto_sub -W, etc.)
/// to connect via WebSocket transport. The endpoint speaks raw binary MQTT
/// packets over WebSocket binary frames, matching the established convention
/// used by major public brokers.
pub async fn ws_mqtt(
    req: actix_web::HttpRequest,
    body: actix_web::web::Payload,
    state: actix_web::web::Data<std::sync::Arc<mqtt_broker::BrokerState>>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let (response, session, msg_stream) = actix_ws::handle(&req, body)?;
    let state = state.get_ref().clone();

    actix_web::rt::spawn(async move {
        if let Err(e) = handle_ws_mqtt_session(state, session, msg_stream).await {
            tracing::error!("WS MQTT session error: {}", e);
        }
    });

    Ok(response)
}

/// Handle a standard MQTT-over-WebSocket session.
///
/// Mirrors the TCP connection handler (`handle_connection` in mqtt-broker)
/// but reads/writes binary MQTT packets through WebSocket frames instead
/// of a TcpStream.
async fn handle_ws_mqtt_session(
    state: std::sync::Arc<mqtt_broker::BrokerState>,
    mut session: actix_ws::Session,
    mut msg_stream: actix_ws::MessageStream,
) -> anyhow::Result<()> {
    use futures_util::StreamExt as _;
    use tokio::sync::mpsc;

    // Buffer for accumulating MQTT packet bytes from WebSocket frames
    let mut buf = bytes::BytesMut::with_capacity(4096);

    tracing::info!("WS MQTT: handler started, waiting for first frame");

    // Get broker handle for sending messages to the router
    let broker_handle = state.broker_handle.lock().unwrap().clone()
        .ok_or_else(|| anyhow::anyhow!("Broker handle not available"))?;

    // ── Step 1: Read binary frames until we have a complete CONNECT packet ──
    loop {
        match msg_stream.next().await {
            Some(Ok(msg)) => {
                tracing::info!("WS MQTT: received frame: {:?}", msg);
                match msg {
                    actix_ws::Message::Binary(data) => {
                        buf.extend_from_slice(&data);
                        // Try to decode CONNECT; if incomplete, wait for more frames
                        match mqtt_broker::server::decode_first_packet(&mut buf, &state.config) {
                            Ok(Some(connect_packet)) => {
                                // Complete CONNECT received.
                                // Don't split buf — the advance step in
                                // handle_connect_after_decode will consume it.
                                return handle_connect_after_decode(
                                    state, session, msg_stream, buf, broker_handle, connect_packet
                                ).await;
                            }
                            Ok(None) => {
                                // Need more data, continue loop
                                tracing::info!("WS MQTT: awaiting more frames for CONNECT (have {} bytes)", buf.len());
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!("WS MQTT: CONNECT decode error: {}", e);
                                return Ok(());
                            }
                        }
                    }
                    actix_ws::Message::Close(_) => return Ok(()),
                    actix_ws::Message::Ping(bytes) => { let _ = session.pong(&bytes).await; }
                    actix_ws::Message::Pong(_) => {}
                    _ => continue,
                }
            }
            Some(Err(e)) => {
                tracing::warn!("WS MQTT: frame error: {}", e);
                return Ok(());
            }
            None => return Ok(()),
        }
    }
}

/// Process MQTT packets after the CONNECT has been fully decoded.
async fn handle_connect_after_decode(
    state: std::sync::Arc<mqtt_broker::BrokerState>,
    mut session: actix_ws::Session,
    mut msg_stream: actix_ws::MessageStream,
    mut buf: bytes::BytesMut,
    broker_handle: mqtt_broker::BrokerHandle,
    packet: mqtt_core::codec::MqttPacket,
) -> anyhow::Result<()> {
    use futures_util::StreamExt as _;
    use tokio::sync::mpsc;

    // ── Step 3: Extract CONNECT fields ──
    let (client_id, version, keep_alive, clean_session, username) = match &packet {
        mqtt_core::codec::MqttPacket::V311(mqtt_core::v3::types::MqttPacketV3::Connect(c)) => {
            (c.client_id.clone(), mqtt_core::common::ProtocolVersion::V311,
             c.keep_alive, c.clean_session, c.username.clone())
        }
        mqtt_core::codec::MqttPacket::V5(mqtt_core::v5::types::MqttPacketV5::Connect(c)) => {
            (c.client_id.clone(), mqtt_core::common::ProtocolVersion::V5,
             c.keep_alive, c.clean_start, c.username.clone())
        }
        _ => {
            tracing::warn!("WS MQTT: first packet is not CONNECT");
            return Ok(());
        }
    };

    // Handle empty client ID (server-assigned)
    let client_id = if client_id.is_empty() {
        let id = state.generate_client_id();
        tracing::info!("WS MQTT assigned client ID: {}", id);
        id
    } else {
        client_id
    };

    // Force-disconnect any existing session with the same ID
    if let Some(mut existing) = state.sessions.get_mut(&client_id) {
        existing.connected = false;
    }

    // Create session entry
    let session_state = mqtt_broker::session::SessionState::new(
        client_id.clone(),
        version,
        clean_session,
        keep_alive,
        username.clone(),
    );
    state.sessions.insert(client_id.clone(), session_state);
    state.metrics.lock().unwrap().increment_clients_connected();

    // Save session to persistence
    let proto_ver = match version {
        mqtt_core::common::ProtocolVersion::V5 => 5i32,
        _ => 4i32,
    };
    state.persistence.send_event(mqtt_broker::persistence::PersistEvent::SaveSession {
        client_id: client_id.clone(),
        protocol_version: proto_ver,
        clean_session,
        keep_alive,
        username: username.clone(),
    });

    // ── Step 4: Send CONNACK ──
    let connack = match version {
        mqtt_core::common::ProtocolVersion::V311 => {
            mqtt_core::codec::MqttPacket::V311(mqtt_core::v3::types::MqttPacketV3::ConnAck(
                mqtt_core::v3::types::ConnAckPacket {
                    session_present: false,
                    return_code: mqtt_core::v3::types::ConnectReturnCode::Accepted,
                }
            ))
        }
        mqtt_core::common::ProtocolVersion::V5 => {
            mqtt_core::codec::MqttPacket::V5(mqtt_core::v5::types::MqttPacketV5::ConnAck(
                mqtt_core::v5::types::ConnAckPacket {
                    session_present: false,
                    reason_code: mqtt_core::v5::types::ReasonCode::Success,
                    properties: mqtt_core::v5::properties::Properties::new(),
                }
            ))
        }
    };
    let encoded = mqtt_core::codec::encode_packet(&connack)?;
    session.binary(encoded.to_vec()).await
        .map_err(|e| anyhow::anyhow!("Failed to send CONNACK: {}", e))?;
    state.metrics.lock().unwrap().increment_packets_sent();
    state.metrics.lock().unwrap().increment_bytes_sent(encoded.len() as u64);

    tracing::info!("WS MQTT client connected: {}, version={:?}", client_id, version);

    // ── Step 5: Advance buffer past the consumed CONNECT packet ──
    // decode_first_packet does NOT consume, so we manually advance.
    let consumed = match version {
        mqtt_core::common::ProtocolVersion::V311 => {
            let (_, sz) = mqtt_core::v3::codec::decode_packet(&buf)?
                .ok_or_else(|| anyhow::anyhow!("Incomplete CONNECT after decode"))?;
            sz
        }
        mqtt_core::common::ProtocolVersion::V5 => {
            let (_, sz) = mqtt_core::v5::codec::decode_packet(&buf)?
                .ok_or_else(|| anyhow::anyhow!("Incomplete CONNECT after decode"))?;
            sz
        }
    };
    buf.advance(consumed);

    // ── Step 6: Create per-connection channel for publish forwarding ──
    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    state.connections.insert(client_id.clone(), conn_tx);

    // ── Step 7: Main packet processing loop ──
    loop {
        // Try to decode a complete MQTT packet from the buffer
        match mqtt_broker::server::decode_packet_by_version(&mut buf, version) {
            Ok(Some((mqtt_packet, size))) => {
                buf.advance(size);
                state.metrics.lock().unwrap().increment_packets_received();

                // Process the packet (subscribe, publish, ping, etc.)
                let response = mqtt_broker::server::process_packet(
                    &mqtt_packet,
                    &client_id,
                    &state,
                    &broker_handle,
                ).await
                .map_err(|e| anyhow::anyhow!("Packet processing error: {}", e))?;

                // Send response packet back through WebSocket if any
                if let Some(resp_packet) = response {
                    let encoded = mqtt_core::codec::encode_packet(&resp_packet)?;
                    session.binary(encoded.to_vec()).await
                        .map_err(|e| anyhow::anyhow!("Failed to send response: {}", e))?;
                    state.metrics.lock().unwrap().increment_packets_sent();
                    state.metrics.lock().unwrap().increment_bytes_sent(encoded.len() as u64);
                }
            }
            Ok(None) => {
                // Need more data — wait for a WS frame OR a forwarded publish
                tokio::select! {
                    Some(Ok(msg)) = msg_stream.next() => {
                        match msg {
                            actix_ws::Message::Binary(data) => {
                                buf.extend_from_slice(&data);
                            }
                            actix_ws::Message::Close(_) => break,
                            actix_ws::Message::Ping(bytes) => {
                                let _ = session.pong(&bytes).await;
                            }
                            actix_ws::Message::Pong(_) => {}
                            _ => {} // Ignore text frames in binary MQTT mode
                        }
                    }
                    Some(data) = conn_rx.recv() => {
                        // Forwarded PUBLISH from the background router
                        if session.binary(data).await.is_err() {
                            tracing::warn!("Failed to forward publish to WS client {}", client_id);
                            break;
                        }
                        state.metrics.lock().unwrap().increment_packets_sent();
                    }
                    else => break,
                }
            }
            Err(e) => {
                tracing::warn!("WS MQTT decode error from {}: {}", client_id, e);
                break;
            }
        }
    }

    // ── Step 8: Cleanup on disconnect ──
    state.connections.remove(&client_id);
    state.metrics.lock().unwrap().decrement_clients_connected();
    if let Some(mut s) = state.sessions.get_mut(&client_id) {
        s.connected = false;
    }
    let _ = broker_handle.sender.send(mqtt_broker::BrokerMessage::ClientDisconnected {
        client_id: client_id.clone(),
        clean_session,
    });

    tracing::info!("WS MQTT client disconnected: {}", client_id);
    Ok(())
}
