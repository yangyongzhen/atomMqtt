//! Unified codec that detects MQTT version and dispatches to v3 or v5 codec.

use bytes::BytesMut;
use crate::common::*;
use crate::v3;
use crate::v5;

/// A decoded MQTT packet with its protocol version.
#[derive(Debug, Clone)]
pub enum MqttPacket {
    /// MQTT 3.1.1 packet.
    V311(v3::types::MqttPacketV3),
    /// MQTT 5.0 packet.
    V5(v5::types::MqttPacketV5),
}

impl MqttPacket {
    /// Get the protocol version.
    pub fn version(&self) -> ProtocolVersion {
        match self {
            MqttPacket::V311(_) => ProtocolVersion::V311,
            MqttPacket::V5(_) => ProtocolVersion::V5,
        }
    }

    /// Get the packet type.
    pub fn packet_type(&self) -> Option<PacketType> {
        match self {
            MqttPacket::V311(p) => match p {
                v3::types::MqttPacketV3::Connect(_) => Some(PacketType::Connect),
                v3::types::MqttPacketV3::ConnAck(_) => Some(PacketType::ConnAck),
                v3::types::MqttPacketV3::Publish(_) => Some(PacketType::Publish),
                v3::types::MqttPacketV3::PubAck(_) => Some(PacketType::PubAck),
                v3::types::MqttPacketV3::PubRec(_) => Some(PacketType::PubRec),
                v3::types::MqttPacketV3::PubRel(_) => Some(PacketType::PubRel),
                v3::types::MqttPacketV3::PubComp(_) => Some(PacketType::PubComp),
                v3::types::MqttPacketV3::Subscribe(_) => Some(PacketType::Subscribe),
                v3::types::MqttPacketV3::SubAck(_) => Some(PacketType::SubAck),
                v3::types::MqttPacketV3::Unsubscribe(_) => Some(PacketType::Unsubscribe),
                v3::types::MqttPacketV3::UnsubAck(_) => Some(PacketType::UnsubAck),
                v3::types::MqttPacketV3::PingReq(_) => Some(PacketType::PingReq),
                v3::types::MqttPacketV3::PingResp(_) => Some(PacketType::PingResp),
                v3::types::MqttPacketV3::Disconnect(_) => Some(PacketType::Disconnect),
            },
            MqttPacket::V5(p) => match p {
                v5::types::MqttPacketV5::Connect(_) => Some(PacketType::Connect),
                v5::types::MqttPacketV5::ConnAck(_) => Some(PacketType::ConnAck),
                v5::types::MqttPacketV5::Publish(_) => Some(PacketType::Publish),
                v5::types::MqttPacketV5::PubAck(_) => Some(PacketType::PubAck),
                v5::types::MqttPacketV5::PubRec(_) => Some(PacketType::PubRec),
                v5::types::MqttPacketV5::PubRel(_) => Some(PacketType::PubRel),
                v5::types::MqttPacketV5::PubComp(_) => Some(PacketType::PubComp),
                v5::types::MqttPacketV5::Subscribe(_) => Some(PacketType::Subscribe),
                v5::types::MqttPacketV5::SubAck(_) => Some(PacketType::SubAck),
                v5::types::MqttPacketV5::Unsubscribe(_) => Some(PacketType::Unsubscribe),
                v5::types::MqttPacketV5::UnsubAck(_) => Some(PacketType::UnsubAck),
                v5::types::MqttPacketV5::PingReq(_) => Some(PacketType::PingReq),
                v5::types::MqttPacketV5::PingResp(_) => Some(PacketType::PingResp),
                v5::types::MqttPacketV5::Disconnect(_) => Some(PacketType::Disconnect),
                v5::types::MqttPacketV5::Auth(_) => Some(PacketType::Auth),
            },
        }
    }
}

/// Protocol-agnostic codec that detects version automatically.
#[derive(Debug, Clone)]
pub struct MqttCodec {
    /// Maximum packet size allowed.
    max_packet_size: usize,
}

impl Default for MqttCodec {
    fn default() -> Self {
        MqttCodec {
            max_packet_size: crate::common::DEFAULT_MAX_PACKET_SIZE,
        }
    }
}

impl MqttCodec {
    pub fn new(max_packet_size: usize) -> Self {
        MqttCodec { max_packet_size }
    }

    /// Decode a single MQTT packet from a buffer.
    /// Returns None if more data is needed.
    pub fn decode(&self, src: &mut BytesMut) -> MqttResult<Option<MqttPacket>> {
        if src.is_empty() {
            return Ok(None);
        }

        // Peek at fixed header to determine the packet type
        let fixed_header = src[0];
        
        // Try to detect protocol version from CONNECT packet
        // For non-CONNECT packets, we need to know the version from context
        let packet_type = PacketType::from_u8(fixed_header)
            .ok_or_else(|| MqttError::InvalidPacket(format!("Unknown packet type: 0x{:02X}", fixed_header)))?;

        // For CONNECT packets, detect version from protocol level byte
        if packet_type == PacketType::Connect {
            return self.decode_connect(src);
        }

        // For all other packet types, try v3 first (more common), then v5
        // Actually, we should determine the version from the connection context.
        // Since we can't know without context, we'll try v3 first.
        // In the broker, the version is known per-connection.
        // Here we provide a best-effort decode.

        // Try v3 codec
        if let Some(packet) = v3::codec::decode_packet(src).unwrap_or(None).map(|(p, _sz)| {
            MqttPacket::V311(p)
        }) {
            return Ok(Some(packet));
        }

        // Try v5 codec
        if let Some(packet) = v5::codec::decode_packet(src).unwrap_or(None).map(|(p, _sz)| {
            MqttPacket::V5(p)
        }) {
            return Ok(Some(packet));
        }

        Ok(None)
    }

    /// Decode a CONNECT packet, detecting the protocol version.
    fn decode_connect(&self, src: &BytesMut) -> MqttResult<Option<MqttPacket>> {
        // Need at least: fixed header (1) + remaining length (1+) + protocol name (>=6) + level (1)
        if src.len() < 10 {
            return Ok(None);
        }

        let (_, len_bytes) = decode_remaining_length(&src[1..])?;
        if src.len() < 1 + len_bytes {
            return Ok(None);
        }

        // Decode remaining length and check if we have enough data
        let (remaining, len_bytes) = decode_remaining_length(&src[1..])?;
        let total_len = 1 + len_bytes + remaining;
        if src.len() < total_len {
            return Ok(None);
        }

        if total_len > self.max_packet_size {
            return Err(MqttError::PacketTooLarge(total_len));
        }

        // Find protocol level byte (after protocol name string: at least 2+4 bytes for "MQTT")
        let protocol_data = &src[1 + len_bytes..];
        if protocol_data.len() < 7 {
            return Err(MqttError::InvalidPacket("CONNECT packet too short".into()));
        }

        let protocol_level = protocol_data[6]; // After: name_len(2) + "MQTT"(4)

        match protocol_level {
            4 => {
                // MQTT 3.1.1
                let (packet, _) = v3::codec::decode_packet(src)?
                    .ok_or_else(|| MqttError::InvalidPacket("Failed to decode MQTT 3.1.1 CONNECT".into()))?;
                Ok(Some(MqttPacket::V311(packet)))
            }
            5 => {
                // MQTT 5.0
                let (packet, _) = v5::codec::decode_packet(src)?
                    .ok_or_else(|| MqttError::InvalidPacket("Failed to decode MQTT 5.0 CONNECT".into()))?;
                Ok(Some(MqttPacket::V5(packet)))
            }
            _ => Err(MqttError::UnsupportedVersion(protocol_level)),
        }
    }
}

/// Encode a MQTT packet (version-agnostic).
pub fn encode_packet(packet: &MqttPacket) -> MqttResult<BytesMut> {
    match packet {
        MqttPacket::V311(p) => v3::codec::encode_packet(p),
        MqttPacket::V5(p) => v5::codec::encode_packet(p),
    }
}

/// Tokio-util codec for MQTT protocol framing.
/// This implements `Encoder` and `Decoder` traits from tokio-util.
pub struct MqttFramedCodec {
    inner: MqttCodec,
}

impl MqttFramedCodec {
    pub fn new(max_packet_size: usize) -> Self {
        MqttFramedCodec {
            inner: MqttCodec::new(max_packet_size),
        }
    }
}

impl Default for MqttFramedCodec {
    fn default() -> Self {
        MqttFramedCodec {
            inner: MqttCodec::default(),
        }
    }
}

impl tokio_util::codec::Decoder for MqttFramedCodec {
    type Item = MqttPacket;
    type Error = MqttError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // For Tokio codec, we need to handle the framing differently.
        // The MqttCodec decode returns the packet and the size consumed.
        // We need to advance the buffer by the consumed size.
        if src.is_empty() {
            return Ok(None);
        }

        // Need at least fixed header + 1 remaining length byte
        if src.len() < 2 {
            return Ok(None);
        }

        let (remaining_len, len_bytes) = match decode_remaining_length(&src[1..]) {
            Ok((len, bytes)) => (len, bytes),
            Err(_) => return Ok(None), // Need more data
        };

        let total_len = 1 + len_bytes + remaining_len;
        if src.len() < total_len {
            // Need more data
            return Ok(None);
        }

        if total_len > self.inner.max_packet_size {
            return Err(MqttError::PacketTooLarge(total_len));
        }

        // Take the bytes for this frame
        let frame = src.split_to(total_len);

        // Decode the frame
        self.inner.decode(&mut frame.clone())
    }
}

impl tokio_util::codec::Encoder<MqttPacket> for MqttFramedCodec {
    type Error = MqttError;

    fn encode(&mut self, item: MqttPacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = encode_packet(&item)?;
        dst.extend_from_slice(&bytes);
        Ok(())
    }
}
