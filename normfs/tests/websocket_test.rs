#![cfg(feature = "websocket")]

mod common;

use common::memory_fs;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use normfs::proto::{ClientRequest, PingRequest, ServerResponse};
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
