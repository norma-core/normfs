mod common;

use common::memory_fs;
use normfs::proto::{ClientRequest, PingRequest, ServerResponse};
use normfs::server::{CommandProcessor, ResponseSender};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;

/// A client that reads nothing until `open` is set.
struct Stalled {
    open: watch::Receiver<bool>,
    sent: AtomicUsize,
}

impl ResponseSender for Stalled {
    fn send_response(
        &self,
        _response: ServerResponse,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move {
            let mut open = self.open.clone();
            let _ = open.wait_for(|open| *open).await;
            self.sent.fetch_add(1, Ordering::Relaxed);
            true
        })
    }

    fn client_id(&self) -> String {
        "stalled".into()
    }
}

fn ping(sequence: u64) -> ClientRequest {
    ClientRequest {
        ping: Some(PingRequest {
            sequence,
            client_timestamp_ns: 0,
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_does_not_read_stops_being_served() {
    const IN_FLIGHT: u64 = 1024;
    let dir = TempDir::new().unwrap();
    let processor = CommandProcessor::new(memory_fs(&dir, Default::default()).await);
    let (open, rx) = watch::channel(false);
    let client = Arc::new(Stalled {
        open: rx,
        sent: AtomicUsize::new(0),
    });

    for i in 0..IN_FLIGHT {
        tokio::time::timeout(
            Duration::from_secs(5),
            processor.handle_request(ping(i), client.clone()),
        )
        .await
        .expect("a request below the limit waited");
    }
    let mut next = Box::pin(processor.handle_request(ping(IN_FLIGHT), client.clone()));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut next)
            .await
            .is_err(),
        "a request past the limit was taken while every response was still pending"
    );

    open.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), next)
        .await
        .expect("the request past the limit was not taken once responses drained");
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.sent.load(Ordering::Relaxed) < IN_FLIGHT as usize + 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("not every ping was answered");
}
