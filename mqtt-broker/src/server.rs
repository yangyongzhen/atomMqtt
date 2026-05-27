//! MQTT TCP server and connection handler.

use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};

use mqtt_core::common::*;
use mqtt_core::v3;
use mqtt_core::v5;
use mqtt_core::codec::{MqttFramedCodec, MqttPacket};

use crate::config::BrokerConfig;
use crate::retention::RetainedMessage;
use crate::session::SessionState;
use crate::will::WillMessage;
use crate::persistence::PersistEvent;
use crate::BrokerState;
use crate::BrokerHandle;

/// Start the MQTT broker server.
pub async fn start_broker(state: Arc<BrokerState>) -> anyhow::Result<BrokerHandle> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<crate::BrokerMessage>();
    let handle = BrokerHandle { sender: msg_tx.clone() };

    // Spawn background message router
    let bg_state = state.clone();
    tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                crate::BrokerMessage::Publish { topic, payload, qos, retain, source_client } => {
                    // Find subscribers
                    let subscribers = bg_state.subscriptions.lock().unwrap().lookup(&topic);
                    bg_state.metrics.lock().unwrap().increment_messages_published();
                    info!("Publish: topic={}, qos={:?}, subscribers={}", topic, qos, subscribers.len());

                    // Forward to each subscriber via their connection channel
                    for sub in &subscribers {
                        if let Some(tx) = bg_state.connections.get(&sub.client_id) {
                            // Create PUBLISH packet for forwarding (use V311 as universal format)
                            let publish_pkt = MqttPacket::V311(v3::types::MqttPacketV3::Publish(
                                v3::types::PublishPacket {
                                    topic: topic.clone(),
                                    payload: payload.clone(),
                                    qos: mqtt_core::common::QoS::AtMostOnce,
                                    retain: false,
                                    packet_id: None,
                                }
                            ));
                            if let Ok(encoded) = mqtt_core::codec::encode_packet(&publish_pkt) {
                                let _ = tx.send(encoded.to_vec());
                                debug!("Forwarded publish to subscriber={}", sub.client_id);
                            }
                        }
                    }

                    // Forward to web subscriber channels (JSON messages)
                    for sub in &subscribers {
                        if let Some(entry) = bg_state.web_subscribers.get(&sub.client_id) {
                            let payload_str = String::from_utf8_lossy(&payload);
                            let json_msg = serde_json::json!({
                                "type": "publish",
                                "topic": topic,
                                "payload": payload_str,
                                "qos": qos as u8,
                                "source_client": source_client,
                                "timestamp": chrono::Utc::now().to_rfc3339(),
                            });
                            let _ = entry.send(json_msg.to_string());
                        }
                    }

                    // Handle retained messages
                    if retain {
                        bg_state.retained.insert(topic.clone(), RetainedMessage::new(
                            topic.clone(), payload.clone(), qos,
                        ));
                    }
                }
                crate::BrokerMessage::ClientDisconnected { client_id, clean_session } => {
                    // Handle will message: publish it to subscribers
                    if let Some((_, will)) = bg_state.wills.remove(&client_id) {
                        info!("Delivering will message for client={}: topic={}", client_id, will.topic);

                        // Find subscribers for the will topic
                        let subscribers = bg_state.subscriptions.lock().unwrap().lookup(&will.topic);
                        for sub in &subscribers {
                            if let Some(tx) = bg_state.connections.get(&sub.client_id) {
                                let publish_pkt = MqttPacket::V311(v3::types::MqttPacketV3::Publish(
                                    v3::types::PublishPacket {
                                        topic: will.topic.clone(),
                                        payload: will.payload.clone(),
                                        qos: mqtt_core::common::QoS::AtMostOnce,
                                        retain: will.retain,
                                        packet_id: None,
                                    }
                                ));
                                if let Ok(encoded) = mqtt_core::codec::encode_packet(&publish_pkt) {
                                    let _ = tx.send(encoded.to_vec());
                                }
                            }
                        }

                        // Also forward to web subscribers
                        for sub in &subscribers {
                            if let Some(entry) = bg_state.web_subscribers.get(&sub.client_id) {
                                let payload_str = String::from_utf8_lossy(&will.payload);
                                let json_msg = serde_json::json!({
                                    "type": "will",
                                    "topic": will.topic,
                                    "payload": payload_str,
                                    "qos": will.qos as u8,
                                    "source_client": client_id,
                                    "timestamp": chrono::Utc::now().to_rfc3339(),
                                });
                                let _ = entry.send(json_msg.to_string());
                            }
                        }

                        // Handle retain
                        if will.retain {
                            bg_state.retained.insert(will.topic.clone(), RetainedMessage::new(
                                will.topic.clone(), will.payload.clone(), will.qos,
                            ));
                        }

                        bg_state.persistence.send_event(PersistEvent::RemoveWill(client_id.clone()));
                    }

                    if clean_session {
                        // Clean session: remove all state for this client
                        bg_state.subscriptions.lock().unwrap().unsubscribe_all(&client_id);
                        bg_state.persistence.send_event(PersistEvent::RemoveClientSubscriptions(client_id.clone()));
                        bg_state.sessions.remove(&client_id);
                        bg_state.persistence.send_event(PersistEvent::RemoveSession(client_id.clone()));
                        bg_state.metrics.lock().unwrap().subscriptions_active = bg_state.subscriptions.lock().unwrap().count() as u64;
                    } else {
                        // Non-clean session: mark session as disconnected but keep subscriptions
                        if let Some(mut session) = bg_state.sessions.get_mut(&client_id) {
                            session.connected = false;
                        }
                    }
                }
            }
        }
    });

    // Spawn TCP listener
    let addr = format!("{}:{}", state.config.tcp_host, state.config.tcp_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("MQTT Broker listening on tcp://{}", addr);

    let listener_state = state.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    info!("New connection from: {}", peer);
                    let state = listener_state.clone();
                    let handle = BrokerHandle { sender: msg_tx.clone() };
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state, handle).await {
                            error!("Connection error from {}: {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    });

    Ok(handle)
}

/// Handle a single MQTT connection.
async fn handle_connection(
    stream: TcpStream,
    state: Arc<BrokerState>,
    broker_handle: BrokerHandle,
) -> anyhow::Result<()> {
    let (mut read_half, mut write_half) = stream.into_split();
    let _codec = MqttFramedCodec::new(state.config.max_packet_size);

    // Read first packet (should be CONNECT)
    let mut read_buf = BytesMut::with_capacity(4096);
    let _write_buf = BytesMut::with_capacity(4096);

    // We'll use tokio::io::BufReader/BufWriter-like approach
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    // Read initial data
    let n = read_half.read_buf(&mut read_buf).await?;
    if n == 0 {
        return Ok(());
    }

    // Try to decode CONNECT
    let packet = match decode_first_packet(&mut read_buf, &state.config)? {
        Some(p) => p,
        None => {
            warn!("Failed to decode CONNECT packet");
            return Ok(());
        }
    };

    // Process CONNECT and get version
    let (client_id, version, keep_alive, clean_session, username, password, will_opt) = match packet {
        MqttPacket::V311(v3::types::MqttPacketV3::Connect(connect)) => {
            let will = connect.will.as_ref().map(|w| WillMessage {
                client_id: String::new(), // filled below
                topic: w.topic.clone(),
                payload: w.message.clone(),
                qos: w.qos,
                retain: w.retain,
                delay_interval: 0,
                created_at: std::time::Instant::now(),
            });
            (connect.client_id, ProtocolVersion::V311, connect.keep_alive, connect.clean_session, connect.username, connect.password, will)
        }
        MqttPacket::V5(v5::types::MqttPacketV5::Connect(connect)) => {
            let will = connect.will.as_ref().map(|w| WillMessage {
                client_id: String::new(), // filled below
                topic: w.topic.clone(),
                payload: w.payload.clone(),
                qos: w.qos,
                retain: w.retain,
                delay_interval: w.delay_interval,
                created_at: std::time::Instant::now(),
            });
            (connect.client_id, ProtocolVersion::V5, connect.keep_alive, connect.clean_start, connect.username, connect.password, will)
        }
        _ => {
            warn!("First packet is not CONNECT");
            return Ok(());
        }
    };

    // ── Authentication check ──
    let username = match state.authenticator.authenticate(
        state.config.allow_anonymous,
        username.as_deref(),
        password.as_deref(),
    ) {
        crate::auth::AuthResult::Success { username } => Some(username),
        crate::auth::AuthResult::Denied { reason } => {
            warn!("Authentication failed for {}: {:?}", client_id, reason);
            let connack = match version {
                ProtocolVersion::V311 => {
                    MqttPacket::V311(v3::types::MqttPacketV3::ConnAck(v3::types::ConnAckPacket {
                        session_present: false,
                        return_code: reason.to_v3_return_code(),
                    }))
                }
                ProtocolVersion::V5 => {
                    MqttPacket::V5(v5::types::MqttPacketV5::ConnAck(v5::types::ConnAckPacket {
                        session_present: false,
                        reason_code: reason.to_v5_reason_code(),
                        properties: mqtt_core::v5::properties::Properties::new(),
                    }))
                }
            };
            let encoded = mqtt_core::codec::encode_packet(&connack)?;
            write_half.writable().await?;
            write_half.write(&encoded).await?;
            state.metrics.lock().unwrap().increment_packets_sent();
            state.metrics.lock().unwrap().increment_bytes_sent(encoded.len() as u64);
            return Ok(());
        }
    };

    // Handle empty client ID
    let client_id = if client_id.is_empty() {
        let id = state.generate_client_id();
        info!("Assigned client ID: {}", id);
        id
    } else {
        client_id
    };

    // Check existing session (force disconnect)
    if let Some(mut existing) = state.sessions.get_mut(&client_id) {
        existing.connected = false;
        // In production, send disconnect to old session
    }

    // Create session
    let session = SessionState::new(
        client_id.clone(),
        version,
        clean_session,
        keep_alive,
        username.clone(),
    );
    state.sessions.insert(client_id.clone(), session);
    state.metrics.lock().unwrap().increment_clients_connected();

    // Store will message if provided in CONNECT
    if let Some(mut will) = will_opt {
        will.client_id = client_id.clone();
        state.wills.insert(client_id.clone(), will.clone());
        state.persistence.send_event(PersistEvent::SaveWill {
            client_id: client_id.clone(),
            topic: will.topic,
            payload: will.payload,
            qos: will.qos as i32,
            retain: will.retain,
            delay_interval: will.delay_interval,
        });
    }

    // Save session to persistence
    let proto_ver = match version {
        ProtocolVersion::V5 => 5i32,
        _ => 4i32,
    };
    state.persistence.send_event(PersistEvent::SaveSession {
        client_id: client_id.clone(),
        protocol_version: proto_ver,
        clean_session,
        keep_alive,
        username: username.clone(),
    });

    // Send CONNACK
    let connack = match version {
        ProtocolVersion::V311 => {
            MqttPacket::V311(v3::types::MqttPacketV3::ConnAck(v3::types::ConnAckPacket {
                session_present: false,
                return_code: v3::types::ConnectReturnCode::Accepted,
            }))
        }
        ProtocolVersion::V5 => {
            MqttPacket::V5(v5::types::MqttPacketV5::ConnAck(v5::types::ConnAckPacket {
                session_present: false,
                reason_code: v5::types::ReasonCode::Success,
                properties: v5::properties::Properties::new(),
            }))
        }
    };

    let encoded = mqtt_core::codec::encode_packet(&connack)?;
    write_half.writable().await?;
    write_half.write(&encoded).await?;
    state.metrics.lock().unwrap().increment_packets_sent();
    state.metrics.lock().unwrap().increment_bytes_sent(encoded.len() as u64);

    info!("Client connected: {}, version={:?}", client_id, version);

    // Create per-connection channel for forwarding publishes from the background router
    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    state.connections.insert(client_id.clone(), conn_tx);

    // Main loop: read and process packets
    let mut framed_buf = BytesMut::new();
    framed_buf.extend_from_slice(&read_buf); // Remaining data after CONNECT
    // Advance read_buf past the CONNECT packet so framed_buf only has future data
    // (decode_first_packet does NOT consume)
    {
        let (_, consumed) = v3::codec::decode_packet(&read_buf)
            .map_err(|e| anyhow::anyhow!("CONNECT decode error: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("Incomplete CONNECT"))?;
        let _ = read_buf.split_to(consumed);
        framed_buf = read_buf.clone();
    }

    loop {
        // Try to decode a packet first
        match decode_packet_by_version(&mut framed_buf, version) {
            Ok(Some((mqtt_packet, size))) => {
                framed_buf.advance(size);
                state.metrics.lock().unwrap().increment_packets_received();

                // Process the packet
                let response = process_packet(
                    &mqtt_packet,
                    &client_id,
                    username.as_deref().unwrap_or("anonymous"),
                    &state,
                    &broker_handle,
                ).await?;

                // Send response if any
                if let Some(resp) = response {
                    let encoded = mqtt_core::codec::encode_packet(&resp)?;
                    write_half.writable().await?;
                    write_half.write_all(&encoded).await?;
                    state.metrics.lock().unwrap().increment_packets_sent();
                    state.metrics.lock().unwrap().increment_bytes_sent(encoded.len() as u64);
                }
            }
            Ok(None) => {
                // Need more data — wait for TCP data OR forwarded publish
                tokio::select! {
                    result = read_half.read_buf(&mut framed_buf) => {
                        let n = result?;
                        if n == 0 {
                            // Connection closed
                            break;
                        }
                    }
                    Some(data) = conn_rx.recv() => {
                        if let Err(e) = write_half.write_all(&data).await {
                            warn!("Failed to write forwarded publish to {}: {}", client_id, e);
                            break;
                        }
                        state.metrics.lock().unwrap().increment_packets_sent();
                        state.metrics.lock().unwrap().increment_bytes_sent(data.len() as u64);
                    }
                }
            }
            Err(e) => {
                warn!("Packet decode error from {}: {}", client_id, e);
                break;
            }
        }
    }

    // Cleanup on disconnect
    state.connections.remove(&client_id);
    state.metrics.lock().unwrap().decrement_clients_connected();
    if let Some(mut session) = state.sessions.get_mut(&client_id) {
        session.connected = false;
    }

    // Fire disconnect message
    let _ = broker_handle.sender.send(crate::BrokerMessage::ClientDisconnected {
        client_id: client_id.clone(),
        clean_session,
    });

    info!("Client disconnected: {}", client_id);
    Ok(())
}

/// Decode the first packet (CONNECT) to determine version.
pub fn decode_first_packet(buf: &mut BytesMut, config: &BrokerConfig) -> Result<Option<MqttPacket>, MqttError> {
    let _codec = MqttFramedCodec::new(config.max_packet_size);
    // We need to decode manually since MqttFramedCodec splits the buffer
    if buf.len() < 2 {
        return Ok(None);
    }

    let (remaining_len, len_bytes) = decode_remaining_length(&buf[1..])?;
    let total_len = 1 + len_bytes + remaining_len;
    if buf.len() < total_len {
        return Ok(None);
    }

    // Check protocol version
    let hdr = &buf[1 + len_bytes..];
    if hdr.len() < 8 {
        return Err(MqttError::InvalidPacket("CONNECT packet too short".into()));
    }

    let protocol_level = hdr[6]; // After: name_len(2) + "MQTT"(4)
    match protocol_level {
        4 => {
            let (packet, _) = v3::codec::decode_packet(buf)?
                .ok_or_else(|| MqttError::InvalidPacket("Failed to decode CONNECT".into()))?;
            Ok(Some(MqttPacket::V311(packet)))
        }
        5 => {
            let (packet, _) = v5::codec::decode_packet(buf)?
                .ok_or_else(|| MqttError::InvalidPacket("Failed to decode CONNECT".into()))?;
            Ok(Some(MqttPacket::V5(packet)))
        }
        _ => Err(MqttError::UnsupportedVersion(protocol_level)),
    }
}

/// Decode a packet using the known protocol version.
pub fn decode_packet_by_version(buf: &mut BytesMut, version: ProtocolVersion) -> Result<Option<(MqttPacket, usize)>, MqttError> {
    if buf.is_empty() {
        return Ok(None);
    }

    match version {
        ProtocolVersion::V311 => {
            v3::codec::decode_packet(buf).map(|opt| opt.map(|(p, s)| (MqttPacket::V311(p), s)))
        }
        ProtocolVersion::V5 => {
            v5::codec::decode_packet(buf).map(|opt| opt.map(|(p, s)| (MqttPacket::V5(p), s)))
        }
    }
}

/// Process a decoded MQTT packet and return an optional response.
pub async fn process_packet(
    packet: &MqttPacket,
    client_id: &str,
    username: &str,
    state: &Arc<BrokerState>,
    broker_handle: &BrokerHandle,
) -> Result<Option<MqttPacket>, MqttError> {
    match packet {
        MqttPacket::V311(p) => process_v3_packet(p, client_id, username, state, broker_handle).await,
        MqttPacket::V5(p) => process_v5_packet(p, client_id, username, state, broker_handle).await,
    }
}

/// Process MQTT 3.1.1 packet.
async fn process_v3_packet(
    packet: &v3::types::MqttPacketV3,
    client_id: &str,
    username: &str,
    state: &Arc<BrokerState>,
    broker_handle: &BrokerHandle,
) -> Result<Option<MqttPacket>, MqttError> {
    match packet {
        v3::types::MqttPacketV3::Publish(p) => {
            // ── ACL check: PUBLISH ──
            if !state.acl.authorize_publish(username, &p.topic) {
                // For MQTT 3.1.1: silently drop the publish (no PUBACK even for QoS 1)
                warn!("PUBLISH denied by ACL: user={}, topic={}", username, p.topic);
                state.metrics.lock().unwrap().increment_messages_received();
                state.metrics.lock().unwrap().increment_bytes_received(p.payload.len() as u64);
                return Ok(None);
            }

            state.metrics.lock().unwrap().increment_messages_received();
            state.metrics.lock().unwrap().increment_bytes_received(p.payload.len() as u64);

            // Forward to subscribers
            let _ = broker_handle.sender.send(crate::BrokerMessage::Publish {
                topic: p.topic.clone(),
                payload: p.payload.clone(),
                qos: p.qos,
                retain: p.retain,
                source_client: client_id.to_string(),
            });

            // Handle retained messages
            if p.retain {
                if p.payload.is_empty() {
                    state.retained.remove(&p.topic);
                    state.persistence.send_event(PersistEvent::RemoveRetained(p.topic.clone()));
                } else {
                    state.retained.insert(p.topic.clone(), RetainedMessage::new(
                        p.topic.clone(), p.payload.clone(), p.qos,
                    ));
                    state.persistence.send_event(PersistEvent::SaveRetained {
                        topic: p.topic.clone(),
                        payload: p.payload.clone(),
                        qos: p.qos as i32,
                    });
                }
            }

            // Send PUBACK for QoS 1
            if p.qos == QoS::AtLeastOnce {
                if let Some(pid) = p.packet_id {
                    return Ok(Some(MqttPacket::V311(v3::types::MqttPacketV3::PubAck(
                        v3::types::PubAckPacket { packet_id: pid }
                    ))));
                }
            }
            // For QoS 2, send PUBREC
            if p.qos == QoS::ExactlyOnce {
                if let Some(pid) = p.packet_id {
                    return Ok(Some(MqttPacket::V311(v3::types::MqttPacketV3::PubRec(
                        v3::types::PubRecPacket { packet_id: pid }
                    ))));
                }
            }
            Ok(None)
        }

        v3::types::MqttPacketV3::PubAck(p) => {
            debug!("PUBACK from {}: pid={}", client_id, p.packet_id);
            Ok(None)
        }

        v3::types::MqttPacketV3::PubRec(p) => {
            // Respond with PUBREL
            Ok(Some(MqttPacket::V311(v3::types::MqttPacketV3::PubRel(
                v3::types::PubRelPacket { packet_id: p.packet_id }
            ))))
        }

        v3::types::MqttPacketV3::PubRel(p) => {
            // Respond with PUBCOMP
            Ok(Some(MqttPacket::V311(v3::types::MqttPacketV3::PubComp(
                v3::types::PubCompPacket { packet_id: p.packet_id }
            ))))
        }

        v3::types::MqttPacketV3::PubComp(p) => {
            debug!("PUBCOMP from {}: pid={}", client_id, p.packet_id);
            Ok(None)
        }

        v3::types::MqttPacketV3::Subscribe(p) => {
            let mut return_codes = Vec::new();
            for filter in &p.filters {
                // ── ACL check: SUBSCRIBE ──
                if !state.acl.authorize_subscribe(username, &filter.path) {
                    warn!("SUBSCRIBE denied by ACL: user={}, filter={}", username, filter.path);
                    // MQTT 3.1.1: return failure code 0x80 for denied topics
                    return_codes.push(0x80u8);
                    continue;
                }

                // Add subscription to tree
                state.subscriptions.lock().unwrap().subscribe(client_id, &filter.path, filter.qos);
                state.persistence.send_event(PersistEvent::SaveSubscription {
                    client_id: client_id.to_string(),
                    filter: filter.path.clone(),
                    qos: filter.qos as i32,
                });
                return_codes.push(filter.qos as u8);

                // Send retained messages matching this subscription filter (MQTT-3.3.1-10)
                let topic_filter = mqtt_core::common::TopicFilter::new(&filter.path);
                for item in state.retained.iter() {
                    if topic_filter.matches(&item.topic) {
                        // Only send if payload is non-empty (empty = delete marker)
                        if !item.payload.is_empty() {
                            let publish_pkt = MqttPacket::V311(v3::types::MqttPacketV3::Publish(
                                v3::types::PublishPacket {
                                    topic: item.topic.clone(),
                                    payload: item.payload.clone(),
                                    qos: mqtt_core::common::QoS::AtMostOnce,
                                    retain: true,
                                    packet_id: None,
                                }
                            ));
                            if let Ok(encoded) = mqtt_core::codec::encode_packet(&publish_pkt) {
                                if let Some(tx) = state.connections.get(client_id) {
                                    let _ = tx.send(encoded.to_vec());
                                }
                            }
                        }
                    }
                }
            }

            let suback_codes: Vec<v3::types::SubAckReturnCode> = return_codes.iter()
                .filter_map(|&c| v3::types::SubAckReturnCode::from_u8(c))
                .collect();

            state.metrics.lock().unwrap().subscriptions_active = state.subscriptions.lock().unwrap().count() as u64;

            Ok(Some(MqttPacket::V311(v3::types::MqttPacketV3::SubAck(
                v3::types::SubAckPacket {
                    packet_id: p.packet_id,
                    return_codes: suback_codes,
                }
            ))))
        }

        v3::types::MqttPacketV3::Unsubscribe(p) => {
            for filter in &p.filters {
                state.subscriptions.lock().unwrap().unsubscribe(client_id, filter);
                state.persistence.send_event(PersistEvent::RemoveSubscription {
                    client_id: client_id.to_string(),
                    filter: filter.clone(),
                });
            }
            state.metrics.lock().unwrap().subscriptions_active = state.subscriptions.lock().unwrap().count() as u64;

            Ok(Some(MqttPacket::V311(v3::types::MqttPacketV3::UnsubAck(
                v3::types::UnsubAckPacket { packet_id: p.packet_id }
            ))))
        }

        v3::types::MqttPacketV3::PingReq(_) => {
            Ok(Some(MqttPacket::V311(v3::types::MqttPacketV3::PingResp(
                v3::types::PingRespPacket
            ))))
        }

        v3::types::MqttPacketV3::Disconnect(_) => {
            // Client is disconnecting
            if let Some(mut session) = state.sessions.get_mut(client_id) {
                session.connected = false;
            }
            // Remove will message if clean disconnect
            state.wills.remove(client_id);
            state.persistence.send_event(PersistEvent::RemoveWill(client_id.to_string()));
            Ok(None)
        }

        _ => {
            debug!("Unhandled MQTT 3.1.1 packet from {}: {:?}", client_id, packet);
            Ok(None)
        }
    }
}

/// Process MQTT 5.0 packet.
async fn process_v5_packet(
    packet: &v5::types::MqttPacketV5,
    client_id: &str,
    username: &str,
    state: &Arc<BrokerState>,
    broker_handle: &BrokerHandle,
) -> Result<Option<MqttPacket>, MqttError> {
    match packet {
        v5::types::MqttPacketV5::Publish(p) => {
            // ── ACL check: PUBLISH (MQTT 5.0) ──
            if !state.acl.authorize_publish(username, &p.topic) {
                warn!("PUBLISH denied by ACL: user={}, topic={}", username, p.topic);
                state.metrics.lock().unwrap().increment_messages_received();
                state.metrics.lock().unwrap().increment_bytes_received(p.payload.len() as u64);
                // Send PUBACK with NotAuthorized for QoS 1
                if p.qos == QoS::AtLeastOnce {
                    if let Some(pid) = p.packet_id {
                        return Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::PubAck(
                            v5::types::PubAckPacket {
                                packet_id: pid,
                                reason_code: v5::types::ReasonCode::NotAuthorized,
                                properties: v5::properties::Properties::new(),
                            }
                        ))));
                    }
                }
                // For QoS 2, send PUBREC with NotAuthorized
                if p.qos == QoS::ExactlyOnce {
                    if let Some(pid) = p.packet_id {
                        return Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::PubRec(
                            v5::types::PubRecPacket {
                                packet_id: pid,
                                reason_code: v5::types::ReasonCode::NotAuthorized,
                                properties: v5::properties::Properties::new(),
                            }
                        ))));
                    }
                }
                return Ok(None);
            }

            state.metrics.lock().unwrap().increment_messages_received();
            state.metrics.lock().unwrap().increment_bytes_received(p.payload.len() as u64);

            let _ = broker_handle.sender.send(crate::BrokerMessage::Publish {
                topic: p.topic.clone(),
                payload: p.payload.clone(),
                qos: p.qos,
                retain: p.retain,
                source_client: client_id.to_string(),
            });

            if p.retain {
                if p.payload.is_empty() {
                    state.retained.remove(&p.topic);
                    state.persistence.send_event(PersistEvent::RemoveRetained(p.topic.clone()));
                } else {
                    state.retained.insert(p.topic.clone(), RetainedMessage::new(
                        p.topic.clone(), p.payload.clone(), p.qos,
                    ));
                    state.persistence.send_event(PersistEvent::SaveRetained {
                        topic: p.topic.clone(),
                        payload: p.payload.clone(),
                        qos: p.qos as i32,
                    });
                }
            }

            // Send PUBACK for QoS 1
            if p.qos == QoS::AtLeastOnce {
                if let Some(pid) = p.packet_id {
                    return Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::PubAck(
                        v5::types::PubAckPacket {
                            packet_id: pid,
                            reason_code: v5::types::ReasonCode::Success,
                            properties: v5::properties::Properties::new(),
                        }
                    ))));
                }
            }
            // Send PUBREC for QoS 2
            if p.qos == QoS::ExactlyOnce {
                if let Some(pid) = p.packet_id {
                    return Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::PubRec(
                        v5::types::PubRecPacket {
                            packet_id: pid,
                            reason_code: v5::types::ReasonCode::Success,
                            properties: v5::properties::Properties::new(),
                        }
                    ))));
                }
            }
            Ok(None)
        }

        v5::types::MqttPacketV5::PubAck(p) => {
            debug!("PUBACK (v5) from {}: pid={}, reason={:?}", client_id, p.packet_id, p.reason_code);
            Ok(None)
        }

        v5::types::MqttPacketV5::PubRec(p) => {
            Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::PubRel(
                v5::types::PubRelPacket {
                    packet_id: p.packet_id,
                    reason_code: v5::types::ReasonCode::Success,
                    properties: v5::properties::Properties::new(),
                }
            ))))
        }

        v5::types::MqttPacketV5::PubRel(p) => {
            Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::PubComp(
                v5::types::PubCompPacket {
                    packet_id: p.packet_id,
                    reason_code: v5::types::ReasonCode::Success,
                    properties: v5::properties::Properties::new(),
                }
            ))))
        }

        v5::types::MqttPacketV5::PubComp(p) => {
            debug!("PUBCOMP (v5) from {}: pid={}", client_id, p.packet_id);
            Ok(None)
        }

        v5::types::MqttPacketV5::Subscribe(p) => {
            let mut reason_codes = Vec::new();
            for filter in &p.filters {
                // ── ACL check: SUBSCRIBE (MQTT 5.0) ──
                if !state.acl.authorize_subscribe(username, &filter.path) {
                    warn!("SUBSCRIBE denied by ACL: user={}, filter={}", username, filter.path);
                    reason_codes.push(v5::types::ReasonCode::NotAuthorized);
                    continue;
                }

                state.subscriptions.lock().unwrap().subscribe(client_id, &filter.path, filter.qos);
                state.persistence.send_event(PersistEvent::SaveSubscription {
                    client_id: client_id.to_string(),
                    filter: filter.path.clone(),
                    qos: filter.qos as i32,
                });
                reason_codes.push(v5::types::ReasonCode::Success);

                // Send retained messages matching this subscription filter (MQTT-3.3.1-10)
                let topic_filter = mqtt_core::common::TopicFilter::new(&filter.path);
                for item in state.retained.iter() {
                    if topic_filter.matches(&item.topic) {
                        if !item.payload.is_empty() {
                            let publish_pkt = MqttPacket::V311(v3::types::MqttPacketV3::Publish(
                                v3::types::PublishPacket {
                                    topic: item.topic.clone(),
                                    payload: item.payload.clone(),
                                    qos: mqtt_core::common::QoS::AtMostOnce,
                                    retain: true,
                                    packet_id: None,
                                }
                            ));
                            if let Ok(encoded) = mqtt_core::codec::encode_packet(&publish_pkt) {
                                if let Some(tx) = state.connections.get(client_id) {
                                    let _ = tx.send(encoded.to_vec());
                                }
                            }
                        }
                    }
                }
            }
            state.metrics.lock().unwrap().subscriptions_active = state.subscriptions.lock().unwrap().count() as u64;

            Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::SubAck(
                v5::types::SubAckPacket {
                    packet_id: p.packet_id,
                    reason_codes,
                    properties: v5::properties::Properties::new(),
                }
            ))))
        }

        v5::types::MqttPacketV5::Unsubscribe(p) => {
            let mut reason_codes = Vec::new();
            for filter in &p.filters {
                state.subscriptions.lock().unwrap().unsubscribe(client_id, filter);
                state.persistence.send_event(PersistEvent::RemoveSubscription {
                    client_id: client_id.to_string(),
                    filter: filter.clone(),
                });
                reason_codes.push(v5::types::ReasonCode::Success);
            }
            state.metrics.lock().unwrap().subscriptions_active = state.subscriptions.lock().unwrap().count() as u64;

            Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::UnsubAck(
                v5::types::UnsubAckPacket {
                    packet_id: p.packet_id,
                    reason_codes,
                    properties: v5::properties::Properties::new(),
                }
            ))))
        }

        v5::types::MqttPacketV5::PingReq(_) => {
            Ok(Some(MqttPacket::V5(v5::types::MqttPacketV5::PingResp(
                v5::types::PingRespPacket
            ))))
        }

        v5::types::MqttPacketV5::Disconnect(p) => {
            if let Some(mut session) = state.sessions.get_mut(client_id) {
                session.connected = false;
            }
            // Check reason code for will message behaviour
            if p.reason_code != v5::types::ReasonCode::DisconnectWithWillMessage {
                state.wills.remove(client_id);
                state.persistence.send_event(PersistEvent::RemoveWill(client_id.to_string()));
            }
            Ok(None)
        }

        v5::types::MqttPacketV5::Auth(_) => {
            // Extended auth - not fully implemented yet
            debug!("AUTH packet from {}, not fully implemented", client_id);
            Ok(None)
        }

        _ => {
            debug!("Unhandled MQTT 5.0 packet from {}: {:?}", client_id, packet);
            Ok(None)
        }
    }
}
