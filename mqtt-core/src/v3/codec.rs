//! MQTT 3.1.1 packet encoder/decoder.

use bytes::{BufMut, BytesMut};
use crate::common::*;
use crate::v3::types::*;

/// Maximum size for a single packet (10 MB).
const MAX_PACKET_SIZE: usize = 10 * 1024 * 1024;

/// Decode a MQTT 3.1.1 packet from a byte buffer.
pub fn decode_packet(src: &BytesMut) -> MqttResult<Option<(MqttPacketV3, usize)>> {
    if src.is_empty() {
        return Ok(None);
    }

    let fixed_header = src[0];
    let packet_type = PacketType::from_u8(fixed_header)
        .ok_or_else(|| MqttError::InvalidPacket(format!("Unknown packet type: 0x{:02X}", fixed_header)))?;

    // Need at least fixed header byte + 1 remaining length byte
    if src.len() < 2 {
        return Ok(None);
    }

    let (remaining_len, len_bytes) = decode_remaining_length(&src[1..])?;

    let total_len = 1 + len_bytes + remaining_len;

    if total_len > MAX_PACKET_SIZE {
        return Err(MqttError::PacketTooLarge(total_len));
    }

    if src.len() < total_len {
        // Need more data
        return Ok(None);
    }

    let packet_data = &src[1 + len_bytes..total_len];
    let packet = match packet_type {
        PacketType::Connect => MqttPacketV3::Connect(decode_connect(packet_data, fixed_header)?),
        PacketType::ConnAck => MqttPacketV3::ConnAck(decode_connack(packet_data, fixed_header)?),
        PacketType::Publish => MqttPacketV3::Publish(decode_publish(packet_data, fixed_header)?),
        PacketType::PubAck => MqttPacketV3::PubAck(decode_puback(packet_data)?),
        PacketType::PubRec => MqttPacketV3::PubRec(decode_pubrec(packet_data)?),
        PacketType::PubRel => MqttPacketV3::PubRel(decode_pubrel(packet_data)?),
        PacketType::PubComp => MqttPacketV3::PubComp(decode_pubcomp(packet_data)?),
        PacketType::Subscribe => MqttPacketV3::Subscribe(decode_subscribe(packet_data)?),
        PacketType::SubAck => MqttPacketV3::SubAck(decode_suback(packet_data)?),
        PacketType::Unsubscribe => MqttPacketV3::Unsubscribe(decode_unsubscribe(packet_data)?),
        PacketType::UnsubAck => MqttPacketV3::UnsubAck(decode_unsuback(packet_data)?),
        PacketType::PingReq => MqttPacketV3::PingReq(PingReqPacket),
        PacketType::PingResp => MqttPacketV3::PingResp(PingRespPacket),
        PacketType::Disconnect => MqttPacketV3::Disconnect(DisconnectPacket),
        PacketType::Auth => return Err(MqttError::InvalidPacket("AUTH not supported in MQTT 3.1.1".into())),
    };

    Ok(Some((packet, total_len)))
}

/// Encode a MQTT 3.1.1 packet into bytes.
pub fn encode_packet(packet: &MqttPacketV3) -> MqttResult<BytesMut> {
    match packet {
        MqttPacketV3::Connect(p) => encode_connect(p),
        MqttPacketV3::ConnAck(p) => encode_connack(p),
        MqttPacketV3::Publish(p) => encode_publish(p),
        MqttPacketV3::PubAck(p) => encode_puback(p),
        MqttPacketV3::PubRec(p) => encode_pubrec(p),
        MqttPacketV3::PubRel(p) => encode_pubrel(p),
        MqttPacketV3::PubComp(p) => encode_pubcomp(p),
        MqttPacketV3::Subscribe(p) => encode_subscribe(p),
        MqttPacketV3::SubAck(p) => encode_suback(p),
        MqttPacketV3::Unsubscribe(p) => encode_unsubscribe(p),
        MqttPacketV3::UnsubAck(p) => encode_unsuback(p),
        MqttPacketV3::PingReq(p) => encode_pingreq(p),
        MqttPacketV3::PingResp(p) => encode_pingresp(p),
        MqttPacketV3::Disconnect(p) => encode_disconnect(p),
    }
}

// ==================== CONNECT ====================

fn decode_connect(data: &[u8], fixed_header: u8) -> MqttResult<ConnectPacket> {
    let mut pos = 0;

    // Protocol name
    let (protocol_name, consumed) = decode_string(data)?;
    pos += consumed;

    // Protocol level
    if pos >= data.len() {
        return Err(MqttError::InvalidPacket("Missing protocol level".into()));
    }
    let protocol_level = data[pos];
    pos += 1;

    // Only accept MQTT 3.1.1 (level 4)
    if protocol_level != 4 {
        return Err(MqttError::UnsupportedVersion(protocol_level));
    }

    // Connect flags
    if pos >= data.len() {
        return Err(MqttError::InvalidPacket("Missing connect flags".into()));
    }
    let connect_flags = data[pos];
    pos += 1;

    let clean_session = (connect_flags & 0x02) != 0;
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

    // Client ID
    let (client_id, consumed) = decode_string(&data[pos..])?;
    pos += consumed;

    // Will
    let will = if will_flag {
        let (topic, consumed) = decode_string(&data[pos..])?;
        pos += consumed;
        let (message, consumed) = {
            if pos + 2 > data.len() {
                return Err(MqttError::InvalidPacket("Missing will message length".into()));
            }
            let msg_len = (data[pos] as usize) << 8 | (data[pos + 1] as usize);
            pos += 2;
            if pos + msg_len > data.len() {
                return Err(MqttError::InvalidPacket("Will message truncated".into()));
            }
            let msg = data[pos..pos + msg_len].to_vec();
            pos += msg_len;
            (msg, 2 + msg_len)
        };
        Some(Will {
            topic: topic.to_string(),
            message,
            qos: will_qos,
            retain: will_retain,
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

    Ok(ConnectPacket {
        client_id: client_id.to_string(),
        clean_session,
        keep_alive,
        will,
        username,
        password,
    })
}

fn encode_connect(packet: &ConnectPacket) -> MqttResult<BytesMut> {
    let mut variable_header = Vec::new();

    // Protocol name + level
    variable_header.extend_from_slice(&[0x00, 0x04]); // "MQTT" length
    variable_header.extend_from_slice(b"MQTT");
    variable_header.push(0x04); // Protocol level for MQTT 3.1.1

    // Connect flags
    let mut flags: u8 = 0;
    if packet.clean_session { flags |= 0x02; }
    if let Some(ref will) = packet.will {
        flags |= 0x04;
        flags |= (will.qos as u8) << 3;
        if will.retain { flags |= 0x20; }
    }
    if packet.password.is_some() { flags |= 0x40; }
    if packet.username.is_some() { flags |= 0x80; }
    variable_header.push(flags);

    // Keep alive
    variable_header.extend_from_slice(&packet.keep_alive.to_be_bytes());

    // Payload
    let mut payload = Vec::new();
    payload.extend_from_slice(&encode_string(&packet.client_id));

    if let Some(ref will) = packet.will {
        payload.extend_from_slice(&encode_string(&will.topic));
        let msg_len = will.message.len();
        payload.extend_from_slice(&(msg_len as u16).to_be_bytes());
        payload.extend_from_slice(&will.message);
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
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("CONNACK too short".into()));
    }
    let session_present = (data[0] & 0x01) != 0;
    let return_code = ConnectReturnCode::from_u8(data[1])
        .ok_or_else(|| MqttError::InvalidPacket(format!("Unknown CONNACK return code: {}", data[1])))?;
    Ok(ConnAckPacket { session_present, return_code })
}

fn encode_connack(packet: &ConnAckPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::ConnAck.to_u8());
    buf.put_u8(0x02); // remaining length
    buf.put_u8(if packet.session_present { 0x01 } else { 0x00 });
    buf.put_u8(packet.return_code.to_u8());
    Ok(buf)
}

// ==================== PUBLISH ====================

fn decode_publish(data: &[u8], fixed_header: u8) -> MqttResult<PublishPacket> {
    let dup = (fixed_header & 0x08) != 0;
    let qos = QoS::from_u8((fixed_header >> 1) & 0x03)
        .ok_or_else(|| MqttError::InvalidPacket("Invalid QoS".into()))?;
    let retain = (fixed_header & 0x01) != 0;

    let mut pos = 0;

    // Topic
    let (topic, consumed) = decode_string(data)?;
    pos += consumed;

    // Packet ID (only for QoS > 0)
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

    // Payload is everything remaining
    let payload = data[pos..].to_vec();

    Ok(PublishPacket {
        topic: topic.to_string(),
        payload,
        qos,
        retain,
        packet_id,
    })
}

fn encode_publish(packet: &PublishPacket) -> MqttResult<BytesMut> {
    let mut fixed_byte = PacketType::Publish.to_u8();
    fixed_byte |= (packet.qos as u8) << 1;
    if packet.retain { fixed_byte |= 0x01; }
    if packet.packet_id.is_some() && packet.qos != QoS::AtMostOnce { fixed_byte |= 0x08; } // dup

    let mut variable = Vec::new();
    variable.extend_from_slice(&encode_string(&packet.topic));
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

// ==================== PUBACK ====================

fn decode_puback(data: &[u8]) -> MqttResult<PubAckPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBACK too short".into()));
    }
    Ok(PubAckPacket {
        packet_id: u16::from_be_bytes([data[0], data[1]]),
    })
}

fn encode_puback(packet: &PubAckPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubAck.to_u8());
    buf.put_u8(0x02);
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    Ok(buf)
}

// ==================== PUBREC ====================

fn decode_pubrec(data: &[u8]) -> MqttResult<PubRecPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBREC too short".into()));
    }
    Ok(PubRecPacket {
        packet_id: u16::from_be_bytes([data[0], data[1]]),
    })
}

fn encode_pubrec(packet: &PubRecPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubRec.to_u8());
    buf.put_u8(0x02);
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    Ok(buf)
}

// ==================== PUBREL ====================

fn decode_pubrel(data: &[u8]) -> MqttResult<PubRelPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBREL too short".into()));
    }
    Ok(PubRelPacket {
        packet_id: u16::from_be_bytes([data[0], data[1]]),
    })
}

fn encode_pubrel(packet: &PubRelPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubRel.to_u8() | 0x02); // fixed header with flag bit 1 set
    buf.put_u8(0x02);
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    Ok(buf)
}

// ==================== PUBCOMP ====================

fn decode_pubcomp(data: &[u8]) -> MqttResult<PubCompPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("PUBCOMP too short".into()));
    }
    Ok(PubCompPacket {
        packet_id: u16::from_be_bytes([data[0], data[1]]),
    })
}

fn encode_pubcomp(packet: &PubCompPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PubComp.to_u8());
    buf.put_u8(0x02);
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    Ok(buf)
}

// ==================== SUBSCRIBE ====================

fn decode_subscribe(data: &[u8]) -> MqttResult<SubscribePacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("SUBSCRIBE too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let mut pos = 2;
    let mut filters = Vec::new();

    while pos < data.len() {
        let (topic, consumed) = decode_string(&data[pos..])?;
        pos += consumed;
        if pos >= data.len() {
            return Err(MqttError::InvalidPacket("Missing QoS in SUBSCRIBE".into()));
        }
        let qos = QoS::from_u8(data[pos] & 0x03)
            .ok_or_else(|| MqttError::InvalidPacket("Invalid QoS in SUBSCRIBE".into()))?;
        pos += 1;
        filters.push(SubscribeFilter {
            path: topic.to_string(),
            qos,
        });
    }

    Ok(SubscribePacket { packet_id, filters })
}

fn encode_subscribe(packet: &SubscribePacket) -> MqttResult<BytesMut> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet.packet_id.to_be_bytes());
    for filter in &packet.filters {
        payload.extend_from_slice(&encode_string(&filter.path));
        payload.push(filter.qos as u8);
    }

    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Subscribe.to_u8() | 0x02); // bit 1 must be 1
    buf.extend_from_slice(&encode_remaining_length(payload.len()));
    buf.extend_from_slice(&payload);
    Ok(buf)
}

// ==================== SUBACK ====================

fn decode_suback(data: &[u8]) -> MqttResult<SubAckPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("SUBACK too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let return_codes: Vec<SubAckReturnCode> = data[2..]
        .iter()
        .filter_map(|&b| SubAckReturnCode::from_u8(b))
        .collect();

    Ok(SubAckPacket { packet_id, return_codes })
}

fn encode_suback(packet: &SubAckPacket) -> MqttResult<BytesMut> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet.packet_id.to_be_bytes());
    for rc in &packet.return_codes {
        payload.push(*rc as u8);
    }

    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::SubAck.to_u8());
    buf.extend_from_slice(&encode_remaining_length(payload.len()));
    buf.extend_from_slice(&payload);
    Ok(buf)
}

// ==================== UNSUBSCRIBE ====================

fn decode_unsubscribe(data: &[u8]) -> MqttResult<UnsubscribePacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("UNSUBSCRIBE too short".into()));
    }
    let packet_id = u16::from_be_bytes([data[0], data[1]]);
    let mut pos = 2;
    let mut filters = Vec::new();

    while pos < data.len() {
        let (topic, consumed) = decode_string(&data[pos..])?;
        pos += consumed;
        filters.push(topic.to_string());
    }

    Ok(UnsubscribePacket { packet_id, filters })
}

fn encode_unsubscribe(packet: &UnsubscribePacket) -> MqttResult<BytesMut> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet.packet_id.to_be_bytes());
    for filter in &packet.filters {
        payload.extend_from_slice(&encode_string(filter));
    }

    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Unsubscribe.to_u8() | 0x02); // bit 1 must be 1
    buf.extend_from_slice(&encode_remaining_length(payload.len()));
    buf.extend_from_slice(&payload);
    Ok(buf)
}

// ==================== UNSUBACK ====================

fn decode_unsuback(data: &[u8]) -> MqttResult<UnsubAckPacket> {
    if data.len() < 2 {
        return Err(MqttError::InvalidPacket("UNSUBACK too short".into()));
    }
    Ok(UnsubAckPacket {
        packet_id: u16::from_be_bytes([data[0], data[1]]),
    })
}

fn encode_unsuback(packet: &UnsubAckPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::UnsubAck.to_u8());
    buf.put_u8(0x02);
    buf.extend_from_slice(&packet.packet_id.to_be_bytes());
    Ok(buf)
}

// ==================== PINGREQ / PINGRESP / DISCONNECT ====================

fn encode_pingreq(_packet: &PingReqPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PingReq.to_u8());
    buf.put_u8(0x00);
    Ok(buf)
}

fn encode_pingresp(_packet: &PingRespPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::PingResp.to_u8());
    buf.put_u8(0x00);
    Ok(buf)
}

fn encode_disconnect(_packet: &DisconnectPacket) -> MqttResult<BytesMut> {
    let mut buf = BytesMut::new();
    buf.put_u8(PacketType::Disconnect.to_u8());
    buf.put_u8(0x00);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_roundtrip() {
        let packet = ConnectPacket {
            client_id: "test_client".to_string(),
            clean_session: true,
            keep_alive: 60,
            will: None,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
        };

        let encoded = encode_connect(&packet).unwrap();
        let (decoded, _) = decode_packet(&encoded).unwrap();

        match decoded {
            MqttPacketV3::Connect(c) => {
                assert_eq!(c.client_id, "test_client");
                assert!(c.clean_session);
                assert_eq!(c.keep_alive, 60);
                assert_eq!(c.username, Some("user".to_string()));
                assert_eq!(c.password, Some("pass".to_string()));
            }
            _ => panic!("Expected Connect packet"),
        }
    }

    #[test]
    fn test_publish_qos0() {
        let packet = PublishPacket {
            topic: "test/topic".to_string(),
            payload: b"hello".to_vec(),
            qos: QoS::AtMostOnce,
            retain: false,
            packet_id: None,
        };

        let encoded = encode_publish(&packet).unwrap();
        let decoded = decode_publish(&encoded[2..], encoded[0]).unwrap();
        assert_eq!(decoded.topic, "test/topic");
        assert_eq!(decoded.payload, b"hello");
        assert_eq!(decoded.qos, QoS::AtMostOnce);
    }

    #[test]
    fn test_publish_qos1() {
        let packet = PublishPacket {
            topic: "test/topic".to_string(),
            payload: b"world".to_vec(),
            qos: QoS::AtLeastOnce,
            retain: false,
            packet_id: Some(42),
        };

        let encoded = encode_publish(&packet).unwrap();
        let (decoded, _) = decode_packet(&encoded).unwrap();

        match decoded {
            MqttPacketV3::Publish(p) => {
                assert_eq!(p.topic, "test/topic");
                assert_eq!(p.qos, QoS::AtLeastOnce);
                assert_eq!(p.packet_id, Some(42));
            }
            _ => panic!("Expected Publish packet"),
        }
    }

    #[test]
    fn test_subscribe_roundtrip() {
        let packet = SubscribePacket {
            packet_id: 1,
            filters: vec![
                SubscribeFilter { path: "sensor/+".to_string(), qos: QoS::AtMostOnce },
                SubscribeFilter { path: "actuator/#".to_string(), qos: QoS::ExactlyOnce },
            ],
        };

        let encoded = encode_subscribe(&packet).unwrap();
        let (decoded, _) = decode_packet(&encoded).unwrap();

        match decoded {
            MqttPacketV3::Subscribe(s) => {
                assert_eq!(s.packet_id, 1);
                assert_eq!(s.filters.len(), 2);
                assert_eq!(s.filters[0].path, "sensor/+");
            }
            _ => panic!("Expected Subscribe packet"),
        }
    }

    #[test]
    fn test_connack() {
        let packet = ConnAckPacket {
            session_present: false,
            return_code: ConnectReturnCode::Accepted,
        };
        let encoded = encode_connack(&packet).unwrap();
        let (decoded, _) = decode_packet(&encoded).unwrap();
        match decoded {
            MqttPacketV3::ConnAck(c) => {
                assert_eq!(c.return_code, ConnectReturnCode::Accepted);
                assert!(!c.session_present);
            }
            _ => panic!("Expected ConnAck"),
        }
    }
}
