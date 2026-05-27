//! MQTT 5.0 packet encoder/decoder.

use bytes::{BufMut, BytesMut};
use crate::common::*;
use crate::v5::types::*;
use crate::v5::properties::*;

/// Maximum packet size for MQTT 5.0 (default 10 MB).
const MAX_PACKET_SIZE: usize = 10 * 1024 * 1024;

/// Decode a MQTT 5.0 packet from a byte buffer.
pub fn decode_packet(src: &BytesMut) -> MqttResult<Option<(MqttPacketV5, usize)>> {
    if src.is_empty() {
        return Ok(None);
    }

    let fixed_header = src[0];
    let packet_type = PacketType::from_u8(fixed_header)
        .ok_or_else(|| MqttError::InvalidPacket(format!("Unknown packet type: 0x{:02X}", fixed_header)))?;

    if src.len() < 2 {
        return Ok(None);
    }

    let (remaining_len, len_bytes) = decode_remaining_length(&src[1..])?;
    let total_len = 1 + len_bytes + remaining_len;

    if total_len > MAX_PACKET_SIZE {
        return Err(MqttError::PacketTooLarge(total_len));
    }

    if src.len() < total_len {
        return Ok(None);
    }

    let packet_data = &src[1 + len_bytes..total_len];
    let packet = match packet_type {
        PacketType::Connect => MqttPacketV5::Connect(decode_connect(packet_data, fixed_header)?),
        PacketType::ConnAck => MqttPacketV5::ConnAck(decode_connack(packet_data, fixed_header)?),
        PacketType::Publish => MqttPacketV5::Publish(decode_publish(packet_data, fixed_header)?),
        PacketType::PubAck => MqttPacketV5::PubAck(decode_puback(packet_data)?),
        PacketType::PubRec => MqttPacketV5::PubRec(decode_pubrec(packet_data)?),
        PacketType::PubRel => MqttPacketV5::PubRel(decode_pubrel(packet_data)?),
        PacketType::PubComp => MqttPacketV5::PubComp(decode_pubcomp(packet_data)?),
        PacketType::Subscribe => MqttPacketV5::Subscribe(decode_subscribe(packet_data)?),
        PacketType::SubAck => MqttPacketV5::SubAck(decode_suback(packet_data)?),
        PacketType::Unsubscribe => MqttPacketV5::Unsubscribe(decode_unsubscribe(packet_data)?),
        PacketType::UnsubAck => MqttPacketV5::UnsubAck(decode_unsuback(packet_data)?),
        PacketType::PingReq => MqttPacketV5::PingReq(PingReqPacket),
        PacketType::PingResp => MqttPacketV5::PingResp(PingRespPacket),
        PacketType::Disconnect => MqttPacketV5::Disconnect(decode_disconnect(packet_data)?),
        PacketType::Auth => MqttPacketV5::Auth(decode_auth(packet_data)?),
    };

    Ok(Some((packet, total_len)))
}

/// Encode a MQTT 5.0 packet into bytes.
pub fn encode_packet(packet: &MqttPacketV5) -> MqttResult<BytesMut> {
    match packet {
        MqttPacketV5::Connect(p) => encode_connect(p),
        MqttPacketV5::ConnAck(p) => encode_connack(p),
        MqttPacketV5::Publish(p) => encode_publish(p),
        MqttPacketV5::PubAck(p) => encode_puback(p),
        MqttPacketV5::PubRec(p) => encode_pubrec(p),
        MqttPacketV5::PubRel(p) => encode_pubrel(p),
        MqttPacketV5::PubComp(p) => encode_pubcomp(p),
        MqttPacketV5::Subscribe(p) => encode_subscribe(p),
        MqttPacketV5::SubAck(p) => encode_suback(p),
        MqttPacketV5::Unsubscribe(p) => encode_unsubscribe(p),
        MqttPacketV5::UnsubAck(p) => encode_unsuback(p),
        MqttPacketV5::PingReq(_) => encode_pingreq(),
        MqttPacketV5::PingResp(_) => encode_pingresp(),
        MqttPacketV5::Disconnect(p) => encode_disconnect(p),
        MqttPacketV5::Auth(p) => encode_auth(p),
    }
}

// ==================== CONNECT ====================

fn decode_connect(data: &[u8], _fixed_header: u8) -> MqttResult<ConnectPacket> {
    let mut pos = 0;

    // Protocol name
    let (_protocol_name, consumed) = decode_string(data)?;
    pos += consumed;

    // Protocol level: must be 5 for MQTT 5.0
    if pos >= data.len() {
        return Err(MqttError::InvalidPacket("Missing protocol level".into()));
    }
    let protocol_level = data[pos];
    pos += 1;
    if protocol_level != 5 {
        return Err(MqttError::UnsupportedVersion(protocol_level));
    }

    // Connect flags
    if pos >= data.len() {
        return Err(MqttError::InvalidPacket("Missing connect flags".into()));
    }
    let connect_flags = data[pos];
    pos += 1;

    let clean_start = (connect_flags & 0x02) != 0;
    let will_flag = (connect_flags & 0x04) != 0;
    let will_qos = QoS::from_u8((connect_flags >> 3) & 0x03).unwrap_or(QoS::AtMostOnce);
    let will_retain = (connect_flags & 0x20) != 0;
    let password_flag = (connect_flags & 0x40) != 0;
    let username_flag = (connect_flags & 0x80) != 0;

    // Keep alive
    if pos + 2 > data.len() {
        return Err(MqttError::InvalidPacket("Missing keep alive".into()));
    }
    let keep_alive = u16::from_be_bytes([data[pos], data[pos + 1]]);
    pos += 2;

    // Properties
    let (properties, consumed) = Properties::decode(&data[pos..])?;
    pos += consumed;

    // Client ID
    let (client_id, consumed) = decode_string(&data[pos..])?;
    pos += consumed;

    // Will
    let will = if will_flag {
        let (will_props, consumed) = Properties::decode(&data[pos..])?;
        pos += consumed;
        let delay_interval = will_props.will_delay_interval().unwrap_or(0);

        let (topic, consumed) = decode_string(&data[pos..])?;
        pos += consumed;
        let (payload, _consumed) = {
            if pos + 2 > data.len() {
                return Err(MqttError::InvalidPacket("Missing will payload length".into()));
            }
            let msg_len = (data[pos] as usize) << 8 | (data[pos + 1] as usize);
            pos += 2;
            if pos + msg_len > data.len() {
                return Err(MqttError::InvalidPacket("Will payload truncated".into()));
            }
            let msg = data[pos..pos + msg_len].to_vec();
            pos += msg_len;
            (msg, 2 + msg_len)
        };
        Some(Will {
            topic: topic.to_string(),
            payload,
            qos: will_qos,
            retain: will_retain,
            properties: will_props,
            delay_interval,
        })
    } else {
        None
    };

    // Username
    let username = if username_flag {
        let (u, consumed) = decode_string(&data[pos..])?;
        pos += consumed;
        Some(u.to_string())
    } else {
        None
    };

    // Password
    let password = if password_flag {
        let (p, consumed) = decode_string(&data[pos..])?;
        pos += consumed;
        Some(p.to_string())
    } else {
        None
    };
    let _ = pos;

    Ok(ConnectPacket {
        client_id: client_id.to_string(),
        clean_start,
        keep_alive,
        properties,
        will,
        username,
        password,
    })
}

fn encode_connect(packet: &ConnectPacket) -> MqttResult<BytesMut> {
    let mut variable_header = Vec::new();
    variable_header.extend_from_slice(&[0x00, 0x04]); // "MQTT" length
    variable_header.extend_from_slice(b"MQTT");
    variable_header.push(0x05); // Protocol level for MQTT 5.0

    let mut flags: u8 = 0;
    if packet.clean_start { flags |= 0x02; }
    if let Some(ref will) = packet.will {
        flags |= 0x04;
        flags |= (will.qos as u8) << 3;
        if will.retain { flags |= 0x20; }
    }
    if packet.password.is_some() { flags |= 0x40; }
    if packet.username.is_some() { flags |= 0x80; }
    variable_header.push(flags);

    variable_header.extend_from_slice(&packet.keep_alive.to_be_bytes());
    variable_header.extend_from_slice(&packet.properties.encode());

    let mut payload = Vec::new();
    payload.extend_from_slice(&encode_string(&packet.client_id));

    if let Some(ref will) = packet.will {
        payload.extend_from_slice(&will.properties.encode());
        payload.extend_from_slice(&encode_string(&will.topic));
        let msg_len = will.payload.len();
        payload.extend_from_slice(&(msg_len as u16).to_be_bytes());
        payload.extend_from_slice(&will.payload);
    }

    if let Some(ref username) = packet.username {
        payload.extend_from_slice(&encode_string(username));
    }
    if let Some(ref password) = packet.password {
        payload.extend_from_slice(&encode_string(password));
    }

    let remaining: Vec<u8> = variable_header.iter().copied()
        .chain(payload.iter().copied())
        .collect();
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Connect.to_u8());
    buf.extend_from_slice(&encode_remaining_length(remaining.len()));
    buf.extend_from_slice(&remaining);
    Ok(buf)
}

// ==================== CONNACK ====================

fn decode_connack(data: &[u8], _fixed_header: u8) -> MqttResult<ConnAckPacket> {
    if data.is_empty() {
        return Err(MqttError::InvalidPacket("CONNACK too short".into()));
    }
    let session_present = (data[0] & 0x01) != 0;
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("CONNACK missing reason code".into()));
    }
    let reason_code = ReasonCode::from_u8(data[1])
        .ok_or_else(|| MqttError::InvalidPacket(format!("Unknown reason code: {}", data[1])))?;

    let (properties, _) = Properties::decode(&data[2..])?;

    Ok(ConnAckPacket { session_present, reason_code, properties })
}

fn encode_connack(packet: &ConnAckPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::ConnAck.to_u8());
    let props_encoded = packet.properties.encode();
    let remaining_len = 2 + props_encoded.len();
    buf.extend_from_slice(&encode_remaining_length(remaining_len));
    buf.put_u8(if packet.session_present { 0x01 } else { 0x00 });
    buf.put_u8(packet.reason_code as u8);
    buf.extend_from_slice(&props_encoded);
    Ok(buf)
}

// ==================== PUBLISH ====================

fn decode_publish(data: &[u8], fixed_header: u8) -> MqttResult<PublishPacket> {
    let _dup = (fixed_header & 0x08) != 0;
    let qos = QoS::from_u8((fixed_header >> 1) & 0x03)
        .ok_or_else(|| MqttError::InvalidPacket("Invalid QoS".into()))?;
    let retain = (fixed_header & 0x01) != 0;

    let mut pos = 0;

    let (topic, consumed) = decode_string(data)?;
    pos += consumed;

    // Properties
    let (properties, consumed) = Properties::decode(&data[pos..])?;
    pos += consumed;

    // Packet identifier for QoS > 0
    let packet_id = if qos != QoS::AtMostOnce {
        if pos + 2 > data.len() {
            return Err(MqttError::InvalidPacket("Missing packet ID in PUBLISH".into()));
        }
        let pid = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;
        Some(pid)
    } else {
        None
    };

    let payload = data[pos..].to_vec();

    Ok(PublishPacket {
        topic: topic.to_string(),
        payload,
        qos,
        retain,
        packet_id,
        properties,
    })
}

fn encode_publish(packet: &PublishPacket) -> MqttResult<BytesMut> {
    let mut fixed_byte = PacketType::Publish.to_u8();
    fixed_byte |= (packet.qos as u8) << 1;
    if packet.retain { fixed_byte |= 0x01; }
    // DUP flag set only when re-delivering

    let mut variable = Vec::new();
    variable.extend_from_slice(&encode_string(&packet.topic));
    variable.extend_from_slice(&packet.properties.encode());

    if let Some(pid) = packet.packet_id {
        if packet.qos != QoS::AtMostOnce {
            variable.extend_from_slice(&pid.to_be_bytes());
        }
    }

    let remaining: Vec<u8> = variable.iter().copied()
        .chain(packet.payload.iter().copied())
        .collect();
    let mut buf = BytesMut::new();
    buf.put_u8(fixed_byte);
    buf.extend_from_slice(&encode_remaining_length(remaining.len()));
    buf.extend_from_slice(&remaining);
    Ok(buf)
}

// ==================== PUBACK (v5) ====================

fn decode_puback(data: &[u8]) -> MqttResult<PubAckPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBACK too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let reason_code = if data.len() > 2 {
        ReasonCode::from_u8(data[2]).unwrap_or(ReasonCode::Success)
    } else {
        ReasonCode::Success
    };
    let (properties, _) = if data.len() > 3 {
        Properties::decode(&data[3..])?
    } else {
        (Properties::new(), 0)
    };

    Ok(PubAckPacket { packet_id, reason_code, properties })
}

fn encode_puback(packet: &PubAckPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubAck.to_u8());
    let props_encoded = packet.properties.encode();
    let remaining_len = 2 + 1 + props_encoded.len();
    buf.extend_from_slice(&encode_remaining_length(remaining_len));
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    buf.put_u8(packet.reason_code as u8);
    buf.extend_from_slice(&props_encoded);
    Ok(buf)
}

// ==================== PUBREC (v5) ====================

fn decode_pubrec(data: &[u8]) -> MqttResult<PubRecPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBREC too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let reason_code = if data.len() > 2 {
        ReasonCode::from_u8(data[2]).unwrap_or(ReasonCode::Success)
    } else {
        ReasonCode::Success
    };
    let (properties, _) = if data.len() > 3 {
        Properties::decode(&data[3..])?
    } else {
        (Properties::new(), 0)
    };
    Ok(PubRecPacket { packet_id, reason_code, properties })
}

fn encode_pubrec(packet: &PubRecPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubRec.to_u8());
    let props_encoded = packet.properties.encode();
    let remaining_len = 2 + 1 + props_encoded.len();
    buf.extend_from_slice(&encode_remaining_length(remaining_len));
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    buf.put_u8(packet.reason_code as u8);
    buf.extend_from_slice(&props_encoded);
    Ok(buf)
}

// ==================== PUBREL (v5) ====================

fn decode_pubrel(data: &[u8]) -> MqttResult<PubRelPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBREL too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let reason_code = if data.len() > 2 {
        ReasonCode::from_u8(data[2]).unwrap_or(ReasonCode::Success)
    } else {
        ReasonCode::Success
    };
    let (properties, _) = if data.len() > 3 {
        Properties::decode(&data[3..])?
    } else {
        (Properties::new(), 0)
    };
    Ok(PubRelPacket { packet_id, reason_code, properties })
}

fn encode_pubrel(packet: &PubRelPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubRel.to_u8() | 0x02);
    let props_encoded = packet.properties.encode();
    let remaining_len = 2 + 1 + props_encoded.len();
    buf.extend_from_slice(&encode_remaining_length(remaining_len));
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    buf.put_u8(packet.reason_code as u8);
    buf.extend_from_slice(&props_encoded);
    Ok(buf)
}

// ==================== PUBCOMP (v5) ====================

fn decode_pubcomp(data: &[u8]) -> MqttResult<PubCompPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBCOMP too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let reason_code = if data.len() > 2 {
        ReasonCode::from_u8(data[2]).unwrap_or(ReasonCode::Success)
    } else {
        ReasonCode::Success
    };
    let (properties, _) = if data.len() > 3 {
        Properties::decode(&data[3..])?
    } else {
        (Properties::new(), 0)
    };
    Ok(PubCompPacket { packet_id, reason_code, properties })
}

fn encode_pubcomp(packet: &PubCompPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubComp.to_u8());
    let props_encoded = packet.properties.encode();
    let remaining_len = 2 + 1 + props_encoded.len();
    buf.extend_from_slice(&encode_remaining_length(remaining_len));
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    buf.put_u8(packet.reason_code as u8);
    buf.extend_from_slice(&props_encoded);
    Ok(buf)
}

// ==================== SUBSCRIBE (v5) ====================

fn decode_subscribe(data: &[u8]) -> MqttResult<SubscribePacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("SUBSCRIBE too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let mut pos = 2;

    let (properties, consumed) = Properties::decode(&data[pos..])?;
    pos += consumed;

    let mut filters = Vec::new();
    while pos < data.len() {
        let (topic, consumed) = decode_string(&data[pos..])?;
        pos += consumed;

        if pos >= data.len() {
            return Err(MqttError::InvalidPacket("Missing subscription options".into()));
        }
        let options = data[pos];
        pos += 1;

        let qos = QoS::from_u8(options & 0x03)
            .ok_or_else(|| MqttError::InvalidPacket("Invalid QoS in SUBSCRIBE".into()))?;
        let no_local = (options & 0x04) != 0;
        let retain_as_published = (options & 0x08) != 0;
        let retain_handling = RetainHandling::from_u8((options >> 4) & 0x03)
            .unwrap_or(RetainHandling::Send);

        filters.push(SubscribeFilter {
            path: topic.to_string(),
            qos,
            no_local,
            retain_as_published,
            retain_handling,
        });
    }

    Ok(SubscribePacket { packet_id, filters, properties })
}

fn encode_subscribe(packet: &SubscribePacket) -> MqttResult<BytesMut> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet.packet_id.to_be_bytes());
    payload.extend_from_slice(&packet.properties.encode());

    for filter in &packet.filters {
        payload.extend_from_slice(&encode_string(&filter.path));
        let mut options = filter.qos as u8;
        if filter.no_local { options |= 0x04; }
        if filter.retain_as_published { options |= 0x08; }
        options |= (filter.retain_handling as u8) << 4;
        payload.push(options);
    }

    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Subscribe.to_u8() | 0x02);
    buf.extend_from_slice(&encode_remaining_length(payload.len()));
    buf.extend_from_slice(&payload);
    Ok(buf)
}

// ==================== SUBACK (v5) ====================

fn decode_suback(data: &[u8]) -> MqttResult<SubAckPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("SUBACK too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let mut pos = 2;

    let (properties, consumed) = Properties::decode(&data[pos..])?;
    pos += consumed;

    let reason_codes: Vec<ReasonCode> = data[pos..].iter()
        .filter_map(|&b| ReasonCode::from_u8(b))
        .collect();

    Ok(SubAckPacket { packet_id, reason_codes, properties })
}

fn encode_suback(packet: &SubAckPacket) -> MqttResult<BytesMut> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet.packet_id.to_be_bytes());
    payload.extend_from_slice(&packet.properties.encode());
    for rc in &packet.reason_codes {
        payload.push(*rc as u8);
    }

    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::SubAck.to_u8());
    buf.extend_from_slice(&encode_remaining_length(payload.len()));
    buf.extend_from_slice(&payload);
    Ok(buf)
}

// ==================== UNSUBSCRIBE (v5) ====================

fn decode_unsubscribe(data: &[u8]) -> MqttResult<UnsubscribePacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("UNSUBSCRIBE too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let mut pos = 2;

    let (properties, consumed) = Properties::decode(&data[pos..])?;
    pos += consumed;

    let mut filters = Vec::new();
    while pos < data.len() {
        let (topic, consumed) = decode_string(&data[pos..])?;
        pos += consumed;
        filters.push(topic.to_string());
    }

    Ok(UnsubscribePacket { packet_id, filters, properties })
}

fn encode_unsubscribe(packet: &UnsubscribePacket) -> MqttResult<BytesMut> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet.packet_id.to_be_bytes());
    payload.extend_from_slice(&packet.properties.encode());
    for filter in &packet.filters {
        payload.extend_from_slice(&encode_string(filter));
    }

    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Unsubscribe.to_u8() | 0x02);
    buf.extend_from_slice(&encode_remaining_length(payload.len()));
    buf.extend_from_slice(&payload);
    Ok(buf)
}

// ==================== UNSUBACK (v5) ====================

fn decode_unsuback(data: &[u8]) -> MqttResult<UnsubAckPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("UNSUBACK too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let mut pos = 2;

    let (properties, consumed) = Properties::decode(&data[pos..])?;
    pos += consumed;

    let reason_codes: Vec<ReasonCode> = data[pos..].iter()
        .filter_map(|&b| ReasonCode::from_u8(b))
        .collect();

    Ok(UnsubAckPacket { packet_id, reason_codes, properties })
}

fn encode_unsuback(packet: &UnsubAckPacket) -> MqttResult<BytesMut> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet.packet_id.to_be_bytes());
    payload.extend_from_slice(&packet.properties.encode());
    for rc in &packet.reason_codes {
        payload.push(*rc as u8);
    }

    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::UnsubAck.to_u8());
    buf.extend_from_slice(&encode_remaining_length(payload.len()));
    buf.extend_from_slice(&payload);
    Ok(buf)
}

// ==================== PINGREQ / PINGRESP ====================

fn encode_pingreq() -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PingReq.to_u8());
    buf.put_u8(0x00);
    Ok(buf)
}

fn encode_pingresp() -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PingResp.to_u8());
    buf.put_u8(0x00);
    Ok(buf)
}

// ==================== DISCONNECT (v5) ====================

fn decode_disconnect(data: &[u8]) -> MqttResult<DisconnectPacket> {
    let reason_code = if data.is_empty() {
        ReasonCode::normal_disconnection()
    } else {
        ReasonCode::from_u8(data[0]).unwrap_or(ReasonCode::Success)
    };
    let (properties, _) = if data.len() > 1 {
        Properties::decode(&data[1..])?
    } else {
        (Properties::new(), 0)
    };
    Ok(DisconnectPacket { reason_code, properties })
}

fn encode_disconnect(packet: &DisconnectPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Disconnect.to_u8());
    let props_encoded = packet.properties.encode();
    let remaining_len = 1 + props_encoded.len();
    buf.extend_from_slice(&encode_remaining_length(remaining_len));
    buf.put_u8(packet.reason_code as u8);
    buf.extend_from_slice(&props_encoded);
    Ok(buf)
}

// ==================== AUTH ====================

fn decode_auth(data: &[u8]) -> MqttResult<AuthPacket> {
    if data.is_empty() {
        return Err(MqttError::InvalidPacket("AUTH too short".into()));
    }
    let reason_code = ReasonCode::from_u8(data[0])
        .ok_or_else(|| MqttError::InvalidPacket(format!("Unknown AUTH reason code: {}", data[0])))?;
    let (properties, _) = Properties::decode(&data[1..])?;
    Ok(AuthPacket { reason_code, properties })
}

fn encode_auth(packet: &AuthPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Auth.to_u8());
    let props_encoded = packet.properties.encode();
    let remaining_len = 1 + props_encoded.len();
    buf.extend_from_slice(&encode_remaining_length(remaining_len));
    buf.put_u8(packet.reason_code as u8);
    buf.extend_from_slice(&props_encoded);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_roundtrip_v5() {
        let packet = ConnectPacket {
            client_id: "client_v5".to_string(),
            clean_start: true,
            keep_alive: 30,
            properties: {
                let mut p = Properties::new();
                p.add(PropertyId::SessionExpiryInterval, PropertyValue::FourByteInteger(3600));
                p.add(PropertyId::ReceiveMaximum, PropertyValue::TwoByteInteger(100));
                p
            },
            will: None,
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
        };

        let encoded = encode_connect(&packet).unwrap();
        let (decoded, _) = decode_packet(&encoded).unwrap();

        match decoded {
            MqttPacketV5::Connect(c) => {
                assert_eq!(c.client_id, "client_v5");
                assert!(c.clean_start);
                assert_eq!(c.username, Some("admin".to_string()));
                assert_eq!(c.properties.session_expiry_interval(), Some(3600));
                assert_eq!(c.properties.receive_maximum(), Some(100));
            }
            _ => panic!("Expected Connect packet"),
        }
    }

    #[test]
    fn test_publish_with_properties() {
        let packet = PublishPacket {
            topic: "test/topic".to_string(),
            payload: b"hello v5".to_vec(),
            qos: QoS::AtLeastOnce,
            retain: false,
            packet_id: Some(10),
            properties: {
                let mut p = Properties::new();
                p.add(PropertyId::PayloadFormatIndicator, PropertyValue::Byte(1));
                p.add(PropertyId::ContentType, PropertyValue::UTF8String("text/plain".to_string()));
                p
            },
        };

        let encoded = encode_publish(&packet).unwrap();
        let (decoded, _) = decode_packet(&encoded).unwrap();

        match decoded {
            MqttPacketV5::Publish(p) => {
                assert_eq!(p.topic, "test/topic");
                assert_eq!(p.payload, b"hello v5");
                assert_eq!(p.qos, QoS::AtLeastOnce);
                assert_eq!(p.packet_id, Some(10));
            }
            _ => panic!("Expected Publish packet"),
        }
    }

    #[test]
    fn test_subscribe_v5() {
        let packet = SubscribePacket {
            packet_id: 1,
            filters: vec![
                SubscribeFilter {
                    path: "sensor/+".to_string(),
                    qos: QoS::ExactlyOnce,
                    no_local: true,
                    retain_as_published: false,
                    retain_handling: RetainHandling::Send,
                },
            ],
            properties: Properties::new(),
        };

        let encoded = encode_subscribe(&packet).unwrap();
        let (decoded, _) = decode_packet(&encoded).unwrap();

        match decoded {
            MqttPacketV5::Subscribe(s) => {
                assert_eq!(s.packet_id, 1);
                assert_eq!(s.filters.len(), 1);
                assert!(s.filters[0].no_local);
                assert_eq!(s.filters[0].qos, QoS::ExactlyOnce);
            }
            _ => panic!("Expected Subscribe packet"),
        }
    }
}
