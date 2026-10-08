mod common;

use bytes::Bytes;
use common::{entries, memory_fs};
use normfs::proto::{ClientRequest, PingRequest, ServerResponse, WriteRequest};
use normfs::server::Server;
use normfs::{NormFS, NormFsSettings, QueueSettings, UintN};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
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

fn message(request: &ClientRequest) -> Vec<u8> {
    let payload = request.encode_to_vec();
    let mut out = (payload.len() as u64).to_le_bytes().to_vec();
    out.extend_from_slice(&payload);
    out
}

async fn read_response<R: AsyncReadExt + Unpin>(stream: &mut R) -> ServerResponse {
    let size = stream.read_u64_le().await.unwrap();
    let mut payload = vec![0; size as usize];
    stream.read_exact(&mut payload).await.unwrap();
    ServerResponse::decode(payload.as_slice()).unwrap()
}

fn write_request(write_id: u64, queue: &str, data: Vec<u8>) -> ClientRequest {
    ClientRequest {
        write: Some(WriteRequest {
            write_id,
            queue_id: queue.into(),
            packets: vec![Bytes::from(data)],
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_from_one_connection_land_in_order() {
    let dir = TempDir::new().unwrap();
    let fs = memory_fs(&dir, QueueSettings::default()).await;
    let mut stream = connect(fs.clone()).await;

    const N: u64 = 2000;
    let mut burst = Vec::new();
    for i in 0..N {
        burst.extend(message(&write_request(
            i,
            "order",
            i.to_le_bytes().to_vec(),
        )));
    }
    stream.write_all(&burst).await.unwrap();
    for _ in 0..N {
        let response = read_response(&mut stream).await;
        assert_eq!(response.write.unwrap().result, 0);
    }

    let got = entries(&fs, "order", N as usize).await;
    assert_eq!(got.len(), N as usize);
    if let Some(i) = (0..N).position(|i| got[i as usize] != i) {
        panic!("entry {i} holds write {}", got[i]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_queue_holds_up_only_its_own_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let mut settings = NormFsSettings::all_active();
    settings.max_memory_usage = 1024 * 1024;
    settings.wal_settings.flush_max_retries = u32::MAX;
    settings.wal_settings.flush_retry_delay = Duration::from_millis(20);
    let fs = Arc::new(NormFS::new(path.clone(), settings).await.unwrap());
    let stuck = fs.resolve("stuck");
    fs.ensure_queue_exists_for_write(&stuck).await.unwrap();
    let first_file =
        UintN::from(1u64).to_file_path(stuck.to_wal_dir(&path).to_str().unwrap(), "wal");
    normfs_wal::fail_flushes(&first_file, u32::MAX);
    let block = Bytes::from(vec![0u8; 200 * 1024]);
    while fs.try_enqueue(&stuck, block.clone()).is_ok() {}

    let mut stream = connect(fs).await;
    let mut burst = message(&write_request(1, "stuck", vec![0; 200 * 1024]));
    burst.extend(message(&ClientRequest {
        ping: Some(PingRequest {
            sequence: 1,
            client_timestamp_ns: 0,
        }),
        ..Default::default()
    }));
    burst.extend(message(&write_request(2, "healthy", vec![1; 16])));
    stream.write_all(&burst).await.unwrap();

    let answered = tokio::time::timeout(Duration::from_secs(3), async {
        let (mut ponged, mut written) = (false, false);
        while !(ponged && written) {
            let response = read_response(&mut stream).await;
            ponged |= response.ping.is_some();
            written |= response.write.is_some_and(|w| w.write_id == 2);
        }
    })
    .await;
    normfs_wal::heal(&first_file);
    answered.expect("a stuck queue held up a ping and a write to another queue");
}
