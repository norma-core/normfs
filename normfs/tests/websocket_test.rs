#![cfg(feature = "websocket")]

mod common;

use bytes::Bytes;
use common::{entries, memory_fs};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use normfs::proto::{
    read_response, ClientRequest, Encoding, PingRequest, ReadRequest, ServerResponse, WriteRequest,
};
use normfs::{NormFS, QueueSettings};
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn serve(fs: Arc<NormFS>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(move |mut request| {
            let fs = fs.clone();
            async move {
                let (response, upgrade) = fastwebsockets::upgrade::upgrade(&mut request).unwrap();
                tokio::spawn(async move {
                    let ws = upgrade.await.unwrap();
                    let _ =
                        normfs::server::websocket::handle_websocket(ws, fs, "test".into()).await;
                });
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        let _ = http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .with_upgrades()
            .await;
    });
    addr
}

async fn connect(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"GET / HTTP/1.1\r\nHost: normfs\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
              Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        )
        .await
        .unwrap();
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await.unwrap());
    }
    assert!(head.starts_with(b"HTTP/1.1 101"));
    stream
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x82];
    match payload.len() {
        len @ 0..126 => frame.push(0x80 | len as u8),
        len @ 126..65536 => {
            frame.push(0x80 | 126);
            frame.extend((len as u16).to_be_bytes());
        }
        len => {
            frame.push(0x80 | 127);
            frame.extend((len as u64).to_be_bytes());
        }
    }
    let mask = [1, 2, 3, 4];
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    frame
}

fn ping_frame(sequence: u64) -> Vec<u8> {
    let payload = ClientRequest {
        ping: Some(PingRequest {
            sequence,
            client_timestamp_ns: 0,
        }),
        ..Default::default()
    }
    .encode_to_vec();
    frame(&payload)
}

async fn read_frame<R: AsyncReadExt + Unpin>(stream: &mut R) -> (u8, Vec<u8>) {
    let opcode = stream.read_u8().await.unwrap() & 0x0f;
    let len = match stream.read_u8().await.unwrap() & 0x7f {
        126 => stream.read_u16().await.unwrap() as usize,
        127 => stream.read_u64().await.unwrap() as usize,
        len => len as usize,
    };
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).await.unwrap();
    (opcode, payload)
}

async fn read_response(stream: &mut TcpStream) -> ServerResponse {
    let (opcode, payload) = read_frame(stream).await;
    assert_eq!(opcode, 0x2);
    ServerResponse::decode(payload.as_slice()).unwrap()
}

#[tokio::test]
async fn response_written_mid_frame_does_not_drop_the_frame() {
    let temp_dir = TempDir::new().unwrap();
    let fs = memory_fs(&temp_dir, QueueSettings::default()).await;
    let mut stream = connect(serve(fs).await).await;

    let first = ping_frame(1);
    let second = ping_frame(2);
    let mut burst = first.clone();
    burst.extend_from_slice(&second[..2]);
    stream.write_all(&burst).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    stream.write_all(&second[2..]).await.unwrap();

    let mut sequences = Vec::new();
    for _ in 0..2 {
        let response = tokio::time::timeout(Duration::from_secs(2), read_response(&mut stream))
            .await
            .expect("no response to the second ping");
        sequences.push(response.ping.unwrap().request.unwrap().sequence);
    }
    sequences.sort();
    assert_eq!(sequences, [1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_from_one_connection_land_in_order() {
    let temp_dir = TempDir::new().unwrap();
    let fs = memory_fs(&temp_dir, QueueSettings::default()).await;
    let mut stream = connect(serve(fs.clone()).await).await;

    const N: u64 = 2000;
    let mut burst = Vec::new();
    for i in 0..N {
        let request = ClientRequest {
            write: Some(WriteRequest {
                write_id: i,
                queue_id: "order".into(),
                packets: vec![Bytes::copy_from_slice(&i.to_le_bytes())],
            }),
            ..Default::default()
        };
        burst.extend(frame(&request.encode_to_vec()));
    }
    stream.write_all(&burst).await.unwrap();
    for _ in 0..N {
        let response = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
            .await
            .expect("a write went unanswered");
        assert_eq!(response.write.unwrap().result, 0);
    }

    let got = entries(&fs, "order", N as usize).await;
    assert_eq!(got.len(), N as usize);
    if let Some(i) = (0..N).position(|i| got[i as usize] != i) {
        panic!("entry {i} holds write {}", got[i]);
    }
}

#[tokio::test]
async fn a_client_that_asks_gets_zstd_over_a_websocket() {
    let temp_dir = TempDir::new().unwrap();
    let fs = memory_fs(&temp_dir, QueueSettings::all_active()).await;
    let mut stream = connect(serve(fs).await).await;

    let data: Vec<u8> = (0..4096)
        .flat_map(|pid| format!("pid={pid} user=root status=sleeping\n").into_bytes())
        .collect();
    let write = ClientRequest {
        write: Some(WriteRequest {
            write_id: 0,
            queue_id: "system/rx".into(),
            packets: vec![Bytes::from(data.clone())],
        }),
        ..Default::default()
    };
    stream
        .write_all(&frame(&write.encode_to_vec()))
        .await
        .unwrap();
    assert_eq!(read_response(&mut stream).await.write.unwrap().result, 0);

    let read = ClientRequest {
        read: Some(ReadRequest {
            read_id: 1,
            queue_id: "system/rx".into(),
            step: 1,
            offset: None,
            limit: 1,
            accept_encodings: vec![Encoding::EncZstd as i32],
        }),
        ..Default::default()
    };
    stream
        .write_all(&frame(&read.encode_to_vec()))
        .await
        .unwrap();
    let entry = loop {
        let read = read_response(&mut stream).await.read.unwrap();
        if read.result == read_response::Result::RrEntry as i32 {
            break read;
        }
    };
    assert_eq!(entry.encoding, Encoding::EncZstd as i32);
    assert_eq!(normfs_store::zstd_decompress(&entry.data).unwrap(), data);
}
