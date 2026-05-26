//! MQTT 3.1.1 命令行客户端测试工具
//!
//! 用法:
//!   mqtt-client pub  broker:port topic payload [--client-id X] [--qos 0|1|2] [--retain]
//!   mqtt-client sub  broker:port topic     [--client-id X] [--qos 0|1|2]
//!   mqtt-client shell broker:port          [--client-id X]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, BytesMut};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncWriteExt, stdin, BufReader};
use tokio::net::TcpStream;
use tokio::time::{interval, Duration};
use tracing::{info, warn, error, debug};

use mqtt_core::v3::codec::{decode_packet, encode_packet};
use mqtt_core::v3::types::*;
use mqtt_core::common::*;

const MAX_PACKET_SIZE: usize = 10 * 1024 * 1024;

// ---------------------------------------------------------------------------
// CLI argument definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "mqtt-client", about = "MQTT 3.1.1 命令行测试客户端")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 发布一条消息后退出
    Pub {
        broker: String,
        topic: String,
        payload: String,
        #[arg(long, default_value = "test-pub")]
        client_id: String,
        #[arg(long, default_value_t = 0)]
        qos: u8,
        #[arg(long)]
        retain: bool,
    },
    /// 订阅主题并持续监听
    Sub {
        broker: String,
        topic: String,
        #[arg(long, default_value = "test-sub")]
        client_id: String,
        #[arg(long, default_value_t = 0)]
        qos: u8,
    },
    /// 进入交互式 Shell 模式
    Shell {
        broker: String,
        #[arg(long, default_value = "test-shell")]
        client_id: String,
    },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_qos(v: u8) -> Result<QoS> {
    QoS::from_u8(v).ok_or_else(|| anyhow!("无效 QoS: {}，可选 0 / 1 / 2", v))
}

fn next_packet_id() -> u16 {
    use std::sync::atomic::AtomicU16;
    static PID: AtomicU16 = AtomicU16::new(1);
    loop {
        let id = PID.fetch_add(1, Ordering::Relaxed);
        if id != 0 {
            return id;
        }
    }
}

/// 从 `Arc<TcpStream>` 读取数据到 buf，自动处理 WouldBlock。
/// 返回实际读取的字节数，0 表示连接已关闭。
async fn try_read_arc(stream: &TcpStream, buf: &mut BytesMut) -> Result<usize> {
    loop {
        stream.readable().await?;
        match stream.try_read_buf(buf) {
            Ok(n) => return Ok(n),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// 从普通 `TcpStream` 读取数据到 buf。
async fn read_into_buf(stream: &mut TcpStream, buf: &mut BytesMut) -> Result<usize> {
    loop {
        stream.readable().await?;
        match stream.try_read_buf(buf) {
            Ok(n) => return Ok(n),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// 读取一个完整的 MQTT 包（使用 decode_packet，自动积累数据）。
/// 支持普通 `&mut TcpStream`。
async fn read_packet_by_mut(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
) -> Result<MqttPacketV3> {
    loop {
        if let Some((packet, size)) = decode_packet(buf)
            .map_err(|e| anyhow!("解码失败: {e}"))?
        {
            buf.advance(size);
            return Ok(packet);
        }
        let n = read_into_buf(stream, buf).await?;
        if n == 0 {
            return Err(anyhow!("连接已关闭"));
        }
        if buf.len() > MAX_PACKET_SIZE {
            return Err(anyhow!("包大小超过限制"));
        }
    }
}

/// 读取一个完整的 MQTT 包（支持 `&TcpStream`，Arc 场景）。
async fn read_packet_by_ref(
    stream: &TcpStream,
    buf: &mut BytesMut,
) -> Result<MqttPacketV3> {
    loop {
        if let Some((packet, size)) = decode_packet(buf)
            .map_err(|e| anyhow!("解码失败: {e}"))?
        {
            buf.advance(size);
            return Ok(packet);
        }
        let n = try_read_arc(stream, buf).await?;
        if n == 0 {
            return Err(anyhow!("连接已关闭"));
        }
        if buf.len() > MAX_PACKET_SIZE {
            return Err(anyhow!("包大小超过限制"));
        }
    }
}

/// 发送一个 MQTT 包
async fn send_packet(stream: &mut TcpStream, packet: &MqttPacketV3) -> Result<()> {
    let bytes = encode_packet(packet).map_err(|e| anyhow!("编码失败: {e}"))?;
    stream.write_all(&bytes).await.context("TCP 发送失败")?;
    Ok(())
}

/// MQTT 连接流程
async fn mqtt_connect(
    stream: &mut TcpStream,
    client_id: &str,
) -> Result<ConnAckPacket> {
    let connect = ConnectPacket {
        client_id: client_id.to_string(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    };
    send_packet(stream, &MqttPacketV3::Connect(connect)).await?;

    let mut buf = BytesMut::new();
    match read_packet_by_mut(stream, &mut buf).await? {
        MqttPacketV3::ConnAck(ack) => {
            info!("连接成功: session_present={}, return_code={:?}",
                ack.session_present, ack.return_code);
            if ack.return_code != ConnectReturnCode::Accepted {
                return Err(anyhow!("连接被拒绝: {:?}", ack.return_code));
            }
            Ok(ack)
        }
        other => Err(anyhow!("期望 ConnAck, 收到: {other:?}")),
    }
}

/// Ping 保活循环（单独的 task，使用 `Arc<TcpStream>` 发送）
async fn ping_loop(
    stream: Arc<TcpStream>,
    running: Arc<AtomicBool>,
    interval_secs: u64,
) {
    let mut tick = interval(Duration::from_secs(interval_secs));
    while running.load(Ordering::Relaxed) {
        tick.tick().await;
        if !running.load(Ordering::Relaxed) {
            break;
        }
        let ping_bytes = match encode_packet(&MqttPacketV3::PingReq(PingReqPacket)) {
            Ok(b) => b,
            Err(e) => {
                error!("Ping 编码失败: {e}");
                continue;
            }
        };
        if let Err(e) = stream.writable().await {
            error!("Ping 等待可写失败: {e}");
            break;
        }
        match stream.try_write(&ping_bytes) {
            Ok(_) => debug!("Ping 发送成功"),
            Err(e) => {
                error!("Ping 发送失败: {e}");
                break;
            }
        }
    }
}

/// 向 stream 发送数据（基于 Arc<TcpStream>）
async fn send_bytes_arc(stream: &TcpStream, bytes: &[u8]) -> Result<()> {
    stream.writable().await?;
    let mut written = 0;
    while written < bytes.len() {
        match stream.try_write(&bytes[written..]) {
            Ok(n) => written += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                stream.writable().await?;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// One-shot 发布
// ---------------------------------------------------------------------------

async fn run_pub(
    broker: &str,
    topic: &str,
    payload: &str,
    client_id: &str,
    qos: u8,
    retain: bool,
) -> Result<()> {
    let qos = parse_qos(qos)?;
    println!("📤 连接到 {} ...", broker);
    let mut stream = TcpStream::connect(broker)
        .await
        .context("无法连接 Broker")?;
    mqtt_connect(&mut stream, client_id).await?;

    let pid = if qos != QoS::AtMostOnce {
        Some(next_packet_id())
    } else {
        None
    };
    let publish = PublishPacket {
        topic: topic.to_string(),
        payload: payload.as_bytes().to_vec(),
        qos,
        retain,
        packet_id: pid,
    };
    send_packet(&mut stream, &MqttPacketV3::Publish(publish)).await?;
    println!("📨 发布成功: topic={topic}, payload={payload}, qos={qos:?}, retain={retain}");

    // 如果是 QoS 1，等待 PubAck
    if qos == QoS::AtLeastOnce {
        let mut buf = BytesMut::new();
        match read_packet_by_mut(&mut stream, &mut buf).await? {
            MqttPacketV3::PubAck(_) => println!("  ✅ 收到 PubAck"),
            other => warn!("期望 PubAck, 收到: {other:?}"),
        }
    }

    // 发送 Disconnect
    send_packet(&mut stream, &MqttPacketV3::Disconnect(DisconnectPacket)).await.ok();
    println!("断开连接");
    Ok(())
}

// ---------------------------------------------------------------------------
// One-shot 订阅
// ---------------------------------------------------------------------------

async fn run_sub(
    broker: &str,
    topic: &str,
    client_id: &str,
    qos: u8,
) -> Result<()> {
    let qos = parse_qos(qos)?;
    let running = Arc::new(AtomicBool::new(true));
    let stream = Arc::new(
        TcpStream::connect(broker)
            .await
            .context("无法连接 Broker")?,
    );

    // 发送 CONNECT
    let connect = ConnectPacket {
        client_id: client_id.to_string(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    };
    let bytes = encode_packet(&MqttPacketV3::Connect(connect))
        .map_err(|e| anyhow!("编码失败: {e}"))?;
    send_bytes_arc(&stream, &bytes).await?;

    // 读取 CONNACK
    let mut buf = BytesMut::new();
    let ack = read_packet_by_ref(&stream, &mut buf).await?;
    match ack {
        MqttPacketV3::ConnAck(ack) => {
            info!("连接成功: {:?}", ack.return_code);
            if ack.return_code != ConnectReturnCode::Accepted {
                return Err(anyhow!("连接被拒绝: {:?}", ack.return_code));
            }
        }
        other => return Err(anyhow!("期望 ConnAck, 收到: {other:?}")),
    }

    // 发送 SUBSCRIBE
    let sub = SubscribePacket {
        packet_id: next_packet_id(),
        filters: vec![SubscribeFilter {
            path: topic.to_string(),
            qos,
        }],
    };
    let bytes = encode_packet(&MqttPacketV3::Subscribe(sub))
        .map_err(|e| anyhow!("编码失败: {e}"))?;
    send_bytes_arc(&stream, &bytes).await?;

    // 读取 SUBACK
    let ack = read_packet_by_ref(&stream, &mut buf).await?;
    match ack {
        MqttPacketV3::SubAck(ack) => {
            info!("订阅成功: packet_id={}, codes={:?}",
                ack.packet_id, ack.return_codes);
        }
        other => warn!("期望 SubAck, 收到: {other:?}"),
    }

    // 启动 Ping 保活
    let stream_ping = stream.clone();
    let r_ping = running.clone();
    tokio::spawn(async move {
        ping_loop(stream_ping, r_ping, 30).await;
    });

    println!("📡 等待消息 (topic={topic})，按 Ctrl+C 退出...");

    // Ctrl+C 监听
    let r_cancel = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n⚠️  收到退出信号，正在断开...");
        r_cancel.store(false, Ordering::Relaxed);
    });

    // 接收消息循环
    while running.load(Ordering::Relaxed) {
        match read_packet_by_ref(&stream, &mut buf).await {
            Ok(packet) => {
                match packet {
                    MqttPacketV3::Publish(pub_pkt) => {
                        let payload_str = String::from_utf8_lossy(&pub_pkt.payload);
                        println!("\n📩 [{}] QoS={:?} ({}字节)",
                            pub_pkt.topic, pub_pkt.qos, pub_pkt.payload.len());
                        println!("  载荷: {}", payload_str);

                        // QoS 1 回复 PubAck
                        if let Some(pid) = pub_pkt.packet_id {
                            if pub_pkt.qos == QoS::AtLeastOnce {
                                let ack = MqttPacketV3::PubAck(PubAckPacket { packet_id: pid });
                                if let Ok(bytes) = encode_packet(&ack) {
                                    send_bytes_arc(&stream, &bytes).await.ok();
                                }
                            }
                        }
                    }
                    MqttPacketV3::PingResp(_) => debug!("PingResp"),
                    _ => debug!("收到其他包: {:?}", std::mem::discriminant(&packet)),
                }
            }
            Err(e) => {
                if running.load(Ordering::Relaxed) {
                    error!("接收消息失败: {e}");
                    break;
                }
            }
        }
    }

    info!("已退出");
    Ok(())
}

// ---------------------------------------------------------------------------
// 交互式 Shell
// ---------------------------------------------------------------------------

async fn run_shell(broker: &str, client_id: &str) -> Result<()> {
    let stream = Arc::new(
        TcpStream::connect(broker)
            .await
            .context("无法连接 Broker")?,
    );
    let running = Arc::new(AtomicBool::new(true));

    // CONNECT
    {
        let connect = ConnectPacket {
            client_id: client_id.to_string(),
            clean_session: true,
            keep_alive: 60,
            will: None,
            username: None,
            password: None,
        };
        let bytes = encode_packet(&MqttPacketV3::Connect(connect))
            .map_err(|e| anyhow!("编码失败: {e}"))?;
        send_bytes_arc(&stream, &bytes).await?;
    }

    // 读取 CONNACK
    let mut buf = BytesMut::new();
    let ack = read_packet_by_ref(&stream, &mut buf).await?;
    match ack {
        MqttPacketV3::ConnAck(ack) => {
            if ack.return_code != ConnectReturnCode::Accepted {
                return Err(anyhow!("连接被拒绝: {:?}", ack.return_code));
            }
            println!("✅ 已连接到 {broker} (client_id={client_id})");
        }
        other => return Err(anyhow!("期望 ConnAck, 收到: {other:?}")),
    }

    // Ctrl+C 通知
    let r = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n⚠️  收到退出信号");
        r.store(false, Ordering::Relaxed);
    });

    // 后台接收消息 task
    let stream_rcv = stream.clone();
    let r_rcv = running.clone();
    tokio::spawn(async move {
        let mut buf = BytesMut::new();
        while r_rcv.load(Ordering::Relaxed) {
            match read_packet_by_ref(&stream_rcv, &mut buf).await {
                Ok(packet) => {
                    match packet {
                        MqttPacketV3::Publish(pub_pkt) => {
                            let payload_str = String::from_utf8_lossy(&pub_pkt.payload);
                            println!("\n📩 [{}] QoS={:?}: {}",
                                pub_pkt.topic, pub_pkt.qos, payload_str);

                            // QoS 1 回复
                            if let Some(pid) = pub_pkt.packet_id {
                                if pub_pkt.qos == QoS::AtLeastOnce {
                                    let ack = MqttPacketV3::PubAck(PubAckPacket { packet_id: pid });
                                    if let Ok(bytes) = encode_packet(&ack) {
                                        send_bytes_arc(&stream_rcv, &bytes).await.ok();
                                    }
                                }
                            }

                            // 重新显示提示符
                            print!("\n> ");
                            use std::io::Write;
                            std::io::stderr().write_all(b"> ").ok();
                            std::io::stderr().flush().ok();
                        }
                        MqttPacketV3::PingResp(_) => {}
                        MqttPacketV3::SubAck(ack) => {
                            println!("  ✅ 订阅确认: codes={:?}", ack.return_codes);
                            print!("> ");
                            use std::io::Write;
                            std::io::stderr().write_all(b"> ").ok();
                            std::io::stderr().flush().ok();
                        }
                        MqttPacketV3::UnsubAck(ack) => {
                            println!("  ✅ 取消订阅确认: packet_id={}", ack.packet_id);
                            print!("> ");
                            use std::io::Write;
                            std::io::stderr().write_all(b"> ").ok();
                            std::io::stderr().flush().ok();
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    if r_rcv.load(Ordering::Relaxed) {
                        error!("接收消息失败: {e}");
                    }
                    break;
                }
            }
        }
    });

    // Ping 保活
    let stream_ping = stream.clone();
    let r_ping = running.clone();
    tokio::spawn(async move {
        ping_loop(stream_ping, r_ping, 30).await;
    });

    // Shell 主循环
    println!("可用命令:");
    println!("  pub <topic> <msg>  [-q 0|1|2] [-r]  发布消息");
    println!("  sub <topic>        [-q 0|1|2]       订阅主题");
    println!("  unsub <topic>                       取消订阅");
    println!("  ping                                发送 Ping");
    println!("  quit / exit / q                     退出");

    let mut stdin_buf = String::new();
    let mut stdin_reader = BufReader::new(stdin());

    while running.load(Ordering::Relaxed) {
        use tokio::io::AsyncBufReadExt;
        stdin_buf.clear();

        // 显示提示符 (stderr 避免干扰 stdout)
        use std::io::Write as _;
        std::io::stderr().write_all(b"> ").ok();
        std::io::stderr().flush().ok();

        let n = stdin_reader
            .read_line(&mut stdin_buf)
            .await
            .context("读取输入失败")?;
        if n == 0 {
            break; // EOF
        }

        let line = stdin_buf.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[0] {
            "quit" | "exit" | "q" => {
                println!("再见!");
                break;
            }
            "ping" => {
                let bytes = encode_packet(&MqttPacketV3::PingReq(PingReqPacket))
                    .map_err(|e| anyhow!("编码失败: {e}"))?;
                send_bytes_arc(&stream, &bytes).await?;
                println!("  Ping 已发送");
            }
            "pub" => {
                if parts.len() < 3 {
                    println!("  ❌ 用法: pub <topic> <msg> [-q 0|1|2] [-r]");
                    continue;
                }
                let topic = parts[1];
                let payload = parts[2];
                let mut qos_val = QoS::AtMostOnce;
                let mut retain = false;

                let mut i = 3;
                while i < parts.len() {
                    match parts[i] {
                        "-q" => {
                            i += 1;
                            if i < parts.len() {
                                qos_val = parse_qos(parts[i].parse::<u8>().unwrap_or(0))?;
                            }
                        }
                        "-r" => retain = true,
                        _ => {}
                    }
                    i += 1;
                }

                let publish = PublishPacket {
                    topic: topic.to_string(),
                    payload: payload.as_bytes().to_vec(),
                    qos: qos_val,
                    retain,
                    packet_id: if qos_val != QoS::AtMostOnce {
                        Some(next_packet_id())
                    } else {
                        None
                    },
                };
                let bytes = encode_packet(&MqttPacketV3::Publish(publish))
                    .map_err(|e| anyhow!("编码失败: {e}"))?;
                send_bytes_arc(&stream, &bytes).await?;
                println!("  ✅ 已发布 topic={topic}, qos={qos_val:?}");
            }
            "sub" => {
                if parts.len() < 2 {
                    println!("  ❌ 用法: sub <topic> [-q 0|1|2]");
                    continue;
                }
                let topic = parts[1];
                let mut qos_val = QoS::AtMostOnce;
                if parts.len() > 3 && parts[2] == "-q" {
                    qos_val = parse_qos(parts[3].parse::<u8>().unwrap_or(0))?;
                }

                let sub = SubscribePacket {
                    packet_id: next_packet_id(),
                    filters: vec![SubscribeFilter {
                        path: topic.to_string(),
                        qos: qos_val,
                    }],
                };
                let bytes = encode_packet(&MqttPacketV3::Subscribe(sub))
                    .map_err(|e| anyhow!("编码失败: {e}"))?;
                send_bytes_arc(&stream, &bytes).await?;
                println!("  ⏳ 订阅请求已发送 topic={topic}");
            }
            "unsub" => {
                if parts.len() < 2 {
                    println!("  ❌ 用法: unsub <topic>");
                    continue;
                }
                let topic = parts[1];
                let unsub = UnsubscribePacket {
                    packet_id: next_packet_id(),
                    filters: vec![topic.to_string()],
                };
                let bytes = encode_packet(&MqttPacketV3::Unsubscribe(unsub))
                    .map_err(|e| anyhow!("编码失败: {e}"))?;
                send_bytes_arc(&stream, &bytes).await?;
                println!("  ✅ 已取消订阅 topic={topic}");
            }
            _ => {
                println!("  ❓ 未知命令: {}", parts[0]);
                println!("  可用: pub / sub / unsub / ping / quit");
            }
        }
    }

    running.store(false, Ordering::Relaxed);

    // 发送 Disconnect
    if let Ok(bytes) = encode_packet(&MqttPacketV3::Disconnect(DisconnectPacket)) {
        send_bytes_arc(&stream, &bytes).await.ok();
    }

    println!("已断开连接");
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mqtt_client=info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Pub { broker, topic, payload, client_id, qos, retain } => {
            run_pub(&broker, &topic, &payload, &client_id, qos, retain).await
        }
        Command::Sub { broker, topic, client_id, qos } => {
            run_sub(&broker, &topic, &client_id, qos).await
        }
        Command::Shell { broker, client_id } => {
            run_shell(&broker, &client_id).await
        }
    }
}
