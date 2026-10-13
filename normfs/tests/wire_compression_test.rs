mod common;

use bytes::Bytes;
use common::memory_fs;
use normfs::proto::{
    read_response, ClientRequest, Encoding, ReadRequest, ReadResponse, ServerResponse, WriteRequest,
};
use normfs::server::Server;
use normfs::{NormFS, QueueConfig, QueueSettings};
use normfs_types::CompressionType;
use prost::Message;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn connect(fs: Arc<NormFS>) -> TcpStream {
    let server = Server::new("127.0.0.1:0".parse().unwrap(), fs)
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move { server.run().await });
    TcpStream::connect(addr).await.unwrap()
}

async fn send(stream: &mut TcpStream, request: ClientRequest) {
    let payload = request.encode_to_vec();
    let mut out = (payload.len() as u64).to_le_bytes().to_vec();
    out.extend_from_slice(&payload);
    stream.write_all(&out).await.unwrap();
}

async fn receive(stream: &mut TcpStream) -> ServerResponse {
    let size = stream.read_u64_le().await.unwrap();
    let mut payload = vec![0; size as usize];
    stream.read_exact(&mut payload).await.unwrap();
    ServerResponse::decode(payload.as_slice()).unwrap()
}

/// A sysinfo-like record: a process table, the same fields over and over.
fn compressible(size: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut pid = 1;
    while out.len() < size {
        out.extend(
            format!("pid={pid} user=root cmd=/usr/bin/station status=sleeping cpu=0.1\n").bytes(),
        );
        pid += 1;
    }
    out.truncate(size);
    out
}

fn incompressible(size: usize) -> Vec<u8> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..size)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// Writes `data` to `queue` and reads it back as entry 0, asking for `accept`.
async fn round_trip(
    settings: QueueSettings,
    queue: &str,
    data: Vec<u8>,
    accept: Vec<i32>,
) -> ReadResponse {
    let dir = TempDir::new().unwrap();
    let fs = memory_fs(&dir, settings).await;
    let mut stream = connect(fs).await;
    send(
        &mut stream,
        ClientRequest {
            write: Some(WriteRequest {
                write_id: 0,
                queue_id: queue.into(),
                packets: vec![Bytes::from(data)],
            }),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(receive(&mut stream).await.write.unwrap().result, 0);
    send(
        &mut stream,
        ClientRequest {
            read: Some(ReadRequest {
                read_id: 1,
                queue_id: queue.into(),
                step: 1,
                offset: None,
                limit: 1,
                accept_encodings: accept,
            }),
            ..Default::default()
        },
    )
    .await;
    loop {
        let read = receive(&mut stream).await.read.unwrap();
        if read.result == read_response::Result::RrEntry as i32 {
            return read;
        }
        assert_eq!(read.result, read_response::Result::RrStart as i32);
    }
}

fn video_raw() -> QueueSettings {
    let raw = QueueConfig {
        compression_type: CompressionType::None,
        ..QueueConfig::active()
    };
    QueueSettings::new(vec![("*video/*".to_string(), raw)], QueueConfig::active()).unwrap()
}

const ZSTD: i32 = Encoding::EncZstd as i32;

#[tokio::test]
async fn a_client_that_asks_gets_zstd_it_can_decode() {
    let data = compressible(64 * 1024);
    let read = round_trip(video_raw(), "system/rx", data.clone(), vec![ZSTD]).await;
    assert_eq!(read.encoding, ZSTD);
    assert!(
        read.data.len() < data.len() / 4,
        "{} bytes",
        read.data.len()
    );
    assert_eq!(normfs_store::zstd_decompress(&read.data).unwrap(), data);
}

#[tokio::test]
async fn a_client_that_does_not_ask_gets_the_record_as_stored() {
    let data = compressible(64 * 1024);
    let read = round_trip(video_raw(), "system/rx", data.clone(), Vec::new()).await;
    assert_eq!(read.encoding, Encoding::EncNone as i32);
    assert_eq!(read.data, data);
}

#[tokio::test]
async fn a_queue_stored_raw_is_sent_raw() {
    let data = compressible(64 * 1024);
    let read = round_trip(video_raw(), "usbvideo/frames", data.clone(), vec![ZSTD]).await;
    assert_eq!(read.encoding, Encoding::EncNone as i32);
    assert_eq!(read.data, data);
}

#[tokio::test]
async fn a_record_below_the_minimum_is_sent_raw() {
    let data = compressible(1023);
    let read = round_trip(video_raw(), "system/rx", data.clone(), vec![ZSTD]).await;
    assert_eq!(read.encoding, Encoding::EncNone as i32);
    assert_eq!(read.data, data);
}

#[tokio::test]
async fn a_record_zstd_cannot_shrink_is_sent_raw() {
    let data = incompressible(64 * 1024);
    let read = round_trip(video_raw(), "system/rx", data.clone(), vec![ZSTD]).await;
    assert_eq!(read.encoding, Encoding::EncNone as i32);
    assert_eq!(read.data, data);
}
