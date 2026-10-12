use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::S3Client;

/// A bucket that holds every PUT for a while and counts how many it holds.
async fn slow_bucket(held: Arc<AtomicUsize>, most: Arc<AtomicUsize>) -> url::Url {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let (held, most) = (held.clone(), most.clone());
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                let mut request = Vec::new();
                loop {
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buf[..n]);
                    let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .map_or(0, |v| v.trim().parse().unwrap());
                    if request.len() < end + 4 + len {
                        continue;
                    }
                    request.drain(..end + 4 + len);
                    let now = held.fetch_add(1, Ordering::SeqCst) + 1;
                    most.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    held.fetch_sub(1, Ordering::SeqCst);
                    let answer = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
                    if socket.write_all(answer).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    url::Url::parse(&format!("http://127.0.0.1:{port}")).unwrap()
}

async fn most_in_flight(limit: usize) -> usize {
    let held = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let endpoint = slow_bucket(held, most.clone()).await;
    let client = S3Client::new(
        endpoint,
        "b".into(),
        "us-east-1".into(),
        "k".into(),
        "s".into(),
    )
    .unwrap()
    .with_upload_limit(limit);
    let puts: Vec<_> = (0..6)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .put_object(&format!("q/{i}.store"), Bytes::from(vec![7u8; 1024]))
                    .await
                    .unwrap()
            })
        })
        .collect();
    for put in puts {
        assert_eq!(put.await.unwrap(), 200);
    }
    most.load(Ordering::SeqCst)
}

#[tokio::test]
async fn uploads_past_the_limit_wait_their_turn() {
    assert_eq!(most_in_flight(2).await, 2);
}

#[tokio::test]
async fn no_limit_lets_every_upload_run() {
    assert_eq!(most_in_flight(0).await, 6);
}
