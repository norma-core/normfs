mod common;

use bytes::Bytes;
use common::{entries, memory_fs};
use normfs::proto::{
    read_response, ClientRequest, PingRequest, ReadRequest, ServerResponse, WriteRequest,
};
use normfs::server::Server;
use normfs::{NormFS, NormFsSettings, QueueSettings, ReadPosition, UintN};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

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
async fn a_request_bigger_than_the_first_buffer_arrives_whole() {
    let dir = TempDir::new().unwrap();
    let fs = Arc::new(
        NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
            .await
            .unwrap(),
    );
    let mut stream = connect(fs.clone()).await;

    const N: u64 = 100;
    let packet = |i: u64| Bytes::from(i.to_le_bytes().repeat(256));
    let request = ClientRequest {
        write: Some(WriteRequest {
            write_id: 1,
            queue_id: "big".into(),
            packets: (0..N).map(packet).collect(),
        }),
        ..Default::default()
    };
    let bytes = message(&request);
    assert!(bytes.len() > 3 * 65536);
    for chunk in bytes.chunks(7000) {
        stream.write_all(chunk).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(read_response(&mut stream).await.write.unwrap().result, 0);

    let (tx, mut rx) = mpsc::channel(N as usize + 1);
    fs.read(
        &fs.resolve("big"),
        ReadPosition::Absolute(UintN::zero()),
        N,
        1,
        tx,
    )
    .await
    .unwrap();
    for i in 0..N {
        assert_eq!(rx.recv().await.unwrap().data, packet(i), "entry {i}");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follow_past_the_limit_is_refused() {
    const FOLLOWS: u64 = 256;
    let dir = TempDir::new().unwrap();
    let fs = memory_fs(&dir, QueueSettings::default()).await;
    let mut stream = connect(fs).await;
    stream
        .write_all(&message(&write_request(0, "follow", vec![1])))
        .await
        .unwrap();
    assert_eq!(read_response(&mut stream).await.write.unwrap().result, 0);

    let mut burst = Vec::new();
    for read_id in 0..=FOLLOWS {
        burst.extend(message(&ClientRequest {
            read: Some(ReadRequest {
                read_id,
                queue_id: "follow".into(),
                step: 1,
                offset: None,
                limit: 0,
                accept_encodings: Vec::new(),
            }),
            ..Default::default()
        }));
    }
    burst.extend(message(&ClientRequest {
        ping: Some(PingRequest {
            sequence: 1,
            client_timestamp_ns: 0,
        }),
        ..Default::default()
    }));
    stream.write_all(&burst).await.unwrap();

    let (mut refused, mut ponged) = (false, false);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !(refused && ponged) {
            let response = read_response(&mut stream).await;
            if let Some(read) = response.read {
                if read.result == read_response::Result::RrServerError as i32 {
                    assert_eq!(read.read_id, FOLLOWS);
                    refused = true;
                }
            }
            ponged |= response.ping.is_some();
        }
    })
    .await
    .expect("the extra follow was not refused or the ping went unanswered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_request_closes_the_connection_a_follow_holds() {
    let dir = TempDir::new().unwrap();
    let fs = memory_fs(&dir, QueueSettings::default()).await;
    let mut stream = connect(fs).await;
    stream
        .write_all(&message(&write_request(0, "follow", vec![1])))
        .await
        .unwrap();
    assert_eq!(read_response(&mut stream).await.write.unwrap().result, 0);
    let follow = ClientRequest {
        read: Some(ReadRequest {
            read_id: 1,
            queue_id: "follow".into(),
            step: 1,
            offset: None,
            limit: 0,
            accept_encodings: Vec::new(),
        }),
        ..Default::default()
    };
    stream.write_all(&message(&follow)).await.unwrap();
    // field number 0, which no decoder accepts
    stream
        .write_all(&[2, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        let mut sink = Vec::new();
        stream.read_to_end(&mut sink).await
    })
    .await
    .expect("the connection stayed open")
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replies_go_out_while_the_client_keeps_sending() {
    let dir = TempDir::new().unwrap();
    let fs = Arc::new(
        NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
            .await
            .unwrap(),
    );
    let (mut rd, mut wr) = connect(fs).await.into_split();

    const ENTRIES: u64 = 1000;
    let request = ClientRequest {
        write: Some(WriteRequest {
            write_id: 1,
            queue_id: "big".into(),
            packets: (0..ENTRIES).map(|_| Bytes::from(vec![7u8; 4000])).collect(),
        }),
        ..Default::default()
    };
    wr.write_all(&message(&request)).await.unwrap();
    assert_eq!(read_response(&mut rd).await.write.unwrap().result, 0);
    let read = ClientRequest {
        read: Some(ReadRequest {
            read_id: 1,
            queue_id: "big".into(),
            step: 1,
            offset: None,
            limit: ENTRIES,
            accept_encodings: Vec::new(),
        }),
        ..Default::default()
    };
    wr.write_all(&message(&read)).await.unwrap();

    // Empty frames, the keepalives, for as long as the socket takes them.
    let keepalives = tokio::spawn(async move {
        let zeros = vec![0u8; 1 << 20];
        for _ in 0..200 {
            wr.write_all(&zeros).await.unwrap();
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let replies = async {
        let mut entries = 0;
        while entries < ENTRIES {
            if read_response(&mut rd).await.read.unwrap().result
                == read_response::Result::RrEntry as i32
            {
                entries += 1;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(60), replies)
        .await
        .expect("the replies never came");
    if keepalives.is_finished() {
        keepalives.await.expect("the keepalives failed");
        panic!("the read waited for the keepalives to end");
    }
    keepalives.abort();
}
