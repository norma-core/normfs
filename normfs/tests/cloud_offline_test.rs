//! A cloud-direct queue starts while the bucket cannot be reached, and what it
//! takes meanwhile lands once the bucket is back, without an id or an object
//! of an earlier life written over. The bucket sits behind a local proxy that
//! a test can cut or let swallow answers. The tests that land need a
//! reachable S3 (see `cloud_direct_test.rs`); the rest need none.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs::{CloudSettings, Error, NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition};
use normfs_cloud::S3Client;
use normfs_wal::WalSettings;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

const PAGE: usize = 4096;
const RECORD_LEN: usize = 1000;
const PER_PAGE: u64 = 4;
/// `memory_pointers::RESERVE_AHEAD`.
const RESERVE: u64 = 1 << 16;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Link {
    Up,
    /// Connections are dropped as soon as they carry anything.
    Down,
    /// Requests reach the bucket, answers never come back.
    Swallow,
}

struct Proxy {
    port: u16,
    link: Arc<Mutex<Link>>,
}

impl Proxy {
    async fn start(upstream: SocketAddr, link: Link) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let link = Arc::new(Mutex::new(link));
        let state = link.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let now = *state.lock().unwrap();
                if now == Link::Down {
                    drop(client);
                } else {
                    tokio::spawn(pipe(client, upstream, state.clone()));
                }
            }
        });
        Self { port, link }
    }

    fn set(&self, link: Link) {
        *self.link.lock().unwrap() = link;
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

async fn pipe(client: TcpStream, upstream: SocketAddr, link: Arc<Mutex<Link>>) {
    let Ok(server) = TcpStream::connect(upstream).await else {
        return;
    };
    let (mut client_rx, mut client_tx) = client.into_split();
    let (mut server_rx, mut server_tx) = server.into_split();
    let now = |link: &Mutex<Link>| *link.lock().unwrap();
    let up_link = link.clone();
    let up = async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match client_rx.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            if now(&up_link) == Link::Down || server_tx.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    };
    let down = async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match server_rx.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            match now(&link) {
                Link::Up => {
                    if client_tx.write_all(&buf[..n]).await.is_err() {
                        return;
                    }
                }
                Link::Swallow => {}
                Link::Down => return,
            }
        }
    };
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
}

fn s3_from_env() -> Option<CloudSettings> {
    let endpoint = std::env::var("S3_ENDPOINT_URL").ok()?;
    Some(CloudSettings {
        endpoint,
        bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "normfs-test".to_string()),
        region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        access_key: std::env::var("AWS_ACCESS_KEY_ID").ok()?,
        secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok()?,
        prefix: format!(
            "cloud-offline-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
    })
}

fn client(cloud: &CloudSettings) -> S3Client {
    S3Client::new(
        url::Url::parse(&cloud.endpoint).unwrap(),
        cloud.bucket.clone(),
        cloud.region.clone(),
        cloud.access_key.clone(),
        cloud.secret_key.clone(),
    )
    .unwrap()
}

/// The bucket behind a proxy, or `None` to skip: no S3 configured.
async fn s3_behind_proxy(link: Link) -> Option<(CloudSettings, CloudSettings, Proxy)> {
    let direct = s3_from_env()?;
    client(&direct).create_bucket().await.expect("bucket");
    let url = url::Url::parse(&direct.endpoint).unwrap();
    let upstream = tokio::net::lookup_host((url.host_str().unwrap(), url.port().unwrap_or(80)))
        .await
        .unwrap()
        .next()
        .unwrap();
    let proxy = Proxy::start(upstream, link).await;
    let proxied = CloudSettings {
        endpoint: proxy.endpoint(),
        ..direct.clone()
    };
    Some((direct, proxied, proxy))
}

/// Credentials that would work, at an endpoint that answers nothing.
fn unreachable(endpoint: String) -> CloudSettings {
    CloudSettings {
        endpoint,
        bucket: "normfs-test".to_string(),
        region: "us-east-1".to_string(),
        access_key: "minio".to_string(),
        secret_key: "minio12345".to_string(),
        prefix: "unreachable".to_string(),
    }
}

fn settings(cloud: CloudSettings) -> NormFsSettings {
    NormFsSettings {
        mem_page_size: PAGE,
        max_memory_usage: 64 * 1024,
        wal_settings: WalSettings {
            write_interval: Duration::from_millis(20),
            flush_retry_delay: Duration::from_millis(10),
            flush_max_retries: 3,
            ..Default::default()
        },
        cloud_settings: Some(cloud),
        queue_settings: QueueSettings::all_active().with_default_persist(Persist::CLOUD),
        ..NormFsSettings::all_active()
    }
}

async fn open(root: &Path, cloud: &CloudSettings) -> NormFS {
    NormFS::new(root.to_path_buf(), settings(cloud.clone()))
        .await
        .unwrap()
}

async fn write(fs: &NormFS, queue: &normfs::QueueId, count: u64, life: u8) -> Vec<u64> {
    let mut ids = Vec::new();
    for _ in 0..count {
        let id = fs
            .enqueue(queue, Bytes::from(vec![life; RECORD_LEN]))
            .await
            .unwrap();
        ids.push(id.to_u64().unwrap());
    }
    ids
}

/// The life that wrote each record, read back from the bucket.
async fn lives(fs: &NormFS, queue: &normfs::QueueId, from: u64, count: u64) -> Vec<u8> {
    let (tx, mut rx) = mpsc::channel(count as usize + 1);
    fs.read(
        queue,
        ReadPosition::Absolute(UintN::from(from)),
        count,
        1,
        tx,
    )
    .await
    .unwrap();
    let mut lives = Vec::new();
    for i in from..from + count {
        let entry = timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} never arrived"))
            .expect("stream ended early");
        assert_eq!(entry.id.to_u64().unwrap(), i);
        assert_eq!(entry.data.len(), RECORD_LEN);
        lives.push(entry.data[0]);
    }
    lives
}

/// Every id a read from `from` delivers, the read covering `span` ids.
async fn ids(fs: &NormFS, queue: &normfs::QueueId, from: u64, span: u64) -> Vec<u64> {
    let (tx, mut rx) = mpsc::channel(16);
    let read = fs.read(
        queue,
        ReadPosition::Absolute(UintN::from(from)),
        span,
        1,
        tx,
    );
    let collect = async {
        let mut ids = Vec::new();
        while let Some(entry) = rx.recv().await {
            ids.push(entry.id.to_u64().unwrap());
        }
        ids
    };
    let (read, ids) = timeout(Duration::from_secs(10), async {
        tokio::join!(read, collect)
    })
    .await
    .expect("the read never ended");
    read.unwrap();
    ids
}

async fn objects(direct: &CloudSettings, queue: &normfs::QueueId) -> usize {
    client(direct)
        .list_objects(&format!("{}/", direct.prefix), None)
        .await
        .unwrap()
        .contents
        .iter()
        .filter(|o| o.key.contains(queue.as_str().trim_start_matches('/')))
        .count()
}

/// Runs a life on a runtime of its own, so that its end takes its background
/// tasks with it, as the end of a process does.
async fn own_runtime(life: impl std::future::Future<Output = ()> + Send + 'static) {
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(life);
        rt.shutdown_background();
    })
    .await
    .unwrap();
}

async fn own_runtime_returning<T: Send + 'static>(
    life: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let out = rt.block_on(life);
        rt.shutdown_background();
        out
    })
    .await
    .unwrap()
}

fn pointer(root: &Path, queue: &normfs::QueueId) -> Vec<String> {
    std::fs::read_to_string(root.join(".memory_pointers"))
        .unwrap()
        .lines()
        .find(|l| l.starts_with(queue.as_str()))
        .expect("the queue has a pointer")
        .split('\t')
        .skip(1)
        .map(str::to_string)
        .collect()
}

/// An instance that has seen the queue before: its pointer says file 2 landed
/// and no id past 7 was uploaded.
async fn recorded(root: &Path, cloud: &CloudSettings) -> normfs::QueueId {
    let fs = open(root, cloud).await;
    let queue = fs.resolve("cam0");
    fs.close().await.unwrap();
    std::fs::write(
        root.join(".memory_pointers"),
        format!("{}\t7\t2\n", queue.as_str()),
    )
    .unwrap();
    queue
}

fn refusing() -> CloudSettings {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    unreachable(format!("http://127.0.0.1:{port}"))
}

/// An S3 lookalike that answers every request the same way.
async fn fake_bucket(status: u16, body: &'static str) -> CloudSettings {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    unreachable(endpoint)
}

/// An instance whose seed exists but whose pointers are gone.
async fn known_instance(root: &Path, cloud: &CloudSettings) -> normfs::QueueId {
    let fs = open(root, cloud).await;
    let queue = fs.resolve("cam0");
    fs.close().await.unwrap();
    let _ = std::fs::remove_file(root.join(".memory_pointers"));
    queue
}

fn assert_needs_bucket(result: Result<(), Error>, queue: &normfs::QueueId, cause: &str) {
    match result {
        Err(e @ Error::BucketUnreachable { .. }) => {
            let text = e.to_string();
            assert!(
                text.contains(queue.as_str()) && text.contains(cause),
                "{text}"
            );
        }
        other => panic!("started without proof of free ids: {other:?}"),
    }
}

#[tokio::test]
async fn a_fresh_instance_starts_its_cloud_queues_without_the_bucket() {
    let temp = tempfile::TempDir::new().unwrap();
    let cloud = refusing();
    let queue;
    {
        let fs = open(temp.path(), &cloud).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        assert_eq!(write(&fs, &queue, 2, 1).await, [0, 1]);
    }
    // Power lost before anything landed: the record made at the first start
    // still lets the queue start.
    let fs = open(temp.path(), &cloud).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
}

#[tokio::test]
async fn a_fresh_instance_reopens_a_queue_after_what_it_wrote() {
    let Some((direct, cloud, _proxy)) = s3_behind_proxy(Link::Up).await else {
        return;
    };
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), &cloud).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(write(&fs, &queue, PER_PAGE, 1).await, [0, 1, 2, 3]);
    fs.close_queue(&queue).await.unwrap();
    assert_eq!(objects(&direct, &queue).await, 1);

    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(write(&fs, &queue, PER_PAGE, 2).await, [4, 5, 6, 7]);
    fs.flush_queue(&queue).await.unwrap();
    assert_eq!(objects(&direct, &queue).await, 2);
    assert_eq!(
        lives(&fs, &queue, 0, 2 * PER_PAGE).await,
        [1, 1, 1, 1, 2, 2, 2, 2]
    );
    fs.close().await.unwrap();
}

async fn object(direct: &CloudSettings, queue: &normfs::QueueId, file: u64) -> Option<Bytes> {
    client(direct)
        .get_object(&queue.to_cloud_key(&format!("{}/", direct.prefix), &UintN::from(file)))
        .await
        .unwrap()
}

fn persist_settings(cloud: &CloudSettings, persist: Persist) -> NormFsSettings {
    let mut settings = settings(cloud.clone());
    settings.queue_settings = QueueSettings::all_active().with_default_persist(persist);
    settings
}

const STORE_CLOUD: Persist = Persist {
    wal: false,
    store: true,
    cloud: true,
};

#[tokio::test]
async fn a_store_queue_after_a_crashed_cloud_life_writes_after_its_objects() {
    let Some((direct, cloud, _proxy)) = s3_behind_proxy(Link::Up).await else {
        return;
    };
    let temp = tempfile::TempDir::new().unwrap();
    let (root, life1_cloud) = (temp.path().to_path_buf(), cloud.clone());
    let queue = own_runtime_returning(async move {
        let fs = open(&root, &life1_cloud).await;
        let queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 2 * PER_PAGE, 1).await;
        fs.flush_queue(&queue).await.unwrap();
        queue
    })
    .await;
    let first = object(&direct, &queue, 1).await.expect("object 1");

    let fs = NormFS::new(
        temp.path().to_path_buf(),
        persist_settings(&cloud, STORE_CLOUD),
    )
    .await
    .unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(write(&fs, &queue, PER_PAGE, 2).await[0], 2 * PER_PAGE);
    fs.flush_queue(&queue).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while objects(&direct, &queue).await < 3 {
        assert!(Instant::now() < deadline, "file 3 was never offloaded");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(object(&direct, &queue, 1).await, Some(first));
    fs.close().await.unwrap();
}

/// A cloud-direct life lands two objects and crashes before its pointer
/// names them; a local life follows, then one that offloads.
async fn local_life_after_a_crashed_cloud_life(offline: bool) {
    let Some((direct, cloud, proxy)) = s3_behind_proxy(Link::Up).await else {
        return;
    };
    let temp = tempfile::TempDir::new().unwrap();
    let (root, life1_cloud) = (temp.path().to_path_buf(), cloud.clone());
    let queue = own_runtime_returning(async move {
        let fs = open(&root, &life1_cloud).await;
        let queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 2 * PER_PAGE, 1).await;
        fs.flush_queue(&queue).await.unwrap();
        queue
    })
    .await;
    let objects_before = [
        object(&direct, &queue, 1).await.expect("object 1"),
        object(&direct, &queue, 2).await.expect("object 2"),
    ];

    let store = Persist {
        wal: false,
        store: true,
        cloud: false,
    };
    if offline {
        proxy.set(Link::Down);
        let fs = NormFS::new(temp.path().to_path_buf(), persist_settings(&cloud, store))
            .await
            .unwrap();
        assert_needs_bucket(
            fs.ensure_queue_exists_for_write(&queue).await,
            &queue,
            "cloud-direct life",
        );
        fs.close().await.unwrap();
        proxy.set(Link::Up);
    }
    {
        let fs = NormFS::new(temp.path().to_path_buf(), persist_settings(&cloud, store))
            .await
            .unwrap();
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, PER_PAGE, 2).await;
        fs.flush_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }
    for file in [1u64, 2] {
        assert!(
            !UintN::from(file)
                .to_file_path(queue.to_store_dir(temp.path()).to_str().unwrap(), "store")
                .exists(),
            "local file {file} shares its number with an object"
        );
    }

    proxy.set(Link::Up);
    let fs = NormFS::new(
        temp.path().to_path_buf(),
        persist_settings(&cloud, STORE_CLOUD),
    )
    .await
    .unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while objects(&direct, &queue).await < 3 {
        assert!(
            Instant::now() < deadline,
            "the local file was never offloaded"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        object(&direct, &queue, 1).await.as_ref(),
        Some(&objects_before[0])
    );
    assert_eq!(
        object(&direct, &queue, 2).await.as_ref(),
        Some(&objects_before[1])
    );
    assert_eq!(lives(&fs, &queue, 0, 2 * PER_PAGE).await, [1; 8]);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_store_life_after_a_crashed_cloud_life_numbers_after_its_objects() {
    local_life_after_a_crashed_cloud_life(false).await;
}

#[tokio::test]
async fn a_store_life_after_a_cloud_life_needs_the_bucket_to_start() {
    local_life_after_a_crashed_cloud_life(true).await;
}

#[tokio::test]
async fn a_store_life_with_no_bucket_configured_numbers_past_the_reserve() {
    let temp = tempfile::TempDir::new().unwrap();
    let cloud = refusing();
    let queue = recorded(temp.path(), &cloud).await;
    let mut settings = persist_settings(
        &cloud,
        Persist {
            wal: false,
            store: true,
            cloud: false,
        },
    );
    settings.cloud_settings = None;
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(write(&fs, &queue, PER_PAGE, 2).await, [8, 9, 10, 11]);
    fs.flush_queue(&queue).await.unwrap();
    // The record says file 2 and id 7; a lost hint could hide up to 7 more.
    assert!(UintN::from(9u64)
        .to_file_path(queue.to_store_dir(temp.path()).to_str().unwrap(), "store")
        .exists());
    assert_eq!(lives(&fs, &queue, 8, PER_PAGE).await, [2; 4]);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_memory_life_between_cloud_and_store_lives_keeps_the_record() {
    let Some((direct, cloud, _proxy)) = s3_behind_proxy(Link::Up).await else {
        return;
    };
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), &cloud).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 2 * PER_PAGE, 1).await;
        fs.close().await.unwrap();
    }
    let first = object(&direct, &queue, 1).await.expect("object 1");
    {
        let settings = persist_settings(&cloud, Persist::MEMORY);
        let fs = NormFS::new(temp.path().to_path_buf(), settings)
            .await
            .unwrap();
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        assert!(write(&fs, &queue, 2, 2).await[0] >= 2 * PER_PAGE);
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(
        temp.path().to_path_buf(),
        persist_settings(&cloud, STORE_CLOUD),
    )
    .await
    .unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(write(&fs, &queue, PER_PAGE, 3).await[0], 2 * PER_PAGE);
    fs.flush_queue(&queue).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while objects(&direct, &queue).await < 3 {
        assert!(Instant::now() < deadline, "file 3 was never offloaded");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(object(&direct, &queue, 1).await, Some(first));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_known_instance_without_a_record_needs_the_bucket() {
    let temp = tempfile::TempDir::new().unwrap();
    let cloud = refusing();
    let queue = known_instance(temp.path(), &cloud).await;
    let fs = open(temp.path(), &cloud).await;
    assert_needs_bucket(
        fs.ensure_queue_exists_for_write(&queue).await,
        &queue,
        "did not answer",
    );
}

#[tokio::test]
async fn a_refusing_bucket_is_named_as_a_refusal() {
    let temp = tempfile::TempDir::new().unwrap();
    let cloud = fake_bucket(403, "").await;
    let queue = known_instance(temp.path(), &cloud).await;
    let fs = open(temp.path(), &cloud).await;
    assert_needs_bucket(
        fs.ensure_queue_exists_for_write(&queue).await,
        &queue,
        "refused",
    );
    drop(fs);

    // With a record the rover still drives.
    let queue = recorded(temp.path(), &cloud).await;
    let fs = open(temp.path(), &cloud).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
}

#[tokio::test]
async fn a_queue_the_bucket_held_nothing_for_starts_without_it_later() {
    let temp = tempfile::TempDir::new().unwrap();
    let empty = fake_bucket(200, "<ListBucketResult></ListBucketResult>").await;
    let queue = known_instance(temp.path(), &empty).await;
    {
        let fs = open(temp.path(), &empty).await;
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        fs.close().await.unwrap();
    }
    let fs = open(temp.path(), &refusing()).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(write(&fs, &queue, 1, 1).await, [1]);
}

#[tokio::test]
async fn queues_after_a_failed_check_start_without_waiting_again() {
    let temp = tempfile::TempDir::new().unwrap();
    let cloud = unreachable("http://10.255.255.1:9000".to_string());
    let fs = open(temp.path(), &cloud).await;
    let queues: Vec<_> = (0..5).map(|i| fs.resolve(&format!("cam{i}"))).collect();
    fs.close().await.unwrap();
    let lines: String = queues
        .iter()
        .map(|q| format!("{}\t7\t2\n", q.as_str()))
        .collect();
    std::fs::write(temp.path().join(".memory_pointers"), lines).unwrap();

    let fs = open(temp.path(), &cloud).await;
    let started = Instant::now();
    let first = Instant::now();
    fs.ensure_queue_exists_for_write(&queues[0]).await.unwrap();
    let one = first.elapsed();
    for queue in &queues[1..] {
        fs.ensure_queue_exists_for_write(queue).await.unwrap();
    }
    assert!(
        started.elapsed() < one + Duration::from_secs(2),
        "{:?} for five queues, {one:?} for the first",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_recorded_cloud_queue_opens_while_the_bucket_refuses() {
    let temp = tempfile::TempDir::new().unwrap();
    let cloud = refusing();
    let queue = recorded(temp.path(), &cloud).await;
    let fs = open(temp.path(), &cloud).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(write(&fs, &queue, 2, 1).await, [8, 9]);
}

#[tokio::test]
async fn a_recorded_cloud_queue_opens_within_the_bound_while_the_bucket_is_blackholed() {
    let temp = tempfile::TempDir::new().unwrap();
    // Non-routable: either refused at once or dropped until the connect
    // timeout.
    let cloud = unreachable("http://10.255.255.1:9000".to_string());
    let queue = recorded(temp.path(), &cloud).await;
    let fs = open(temp.path(), &cloud).await;
    timeout(
        Duration::from_secs(15),
        fs.ensure_queue_exists_for_write(&queue),
    )
    .await
    .expect("the start waited out the OS connect timeout")
    .unwrap();
    assert_eq!(write(&fs, &queue, 2, 1).await, [8, 9]);
}

#[tokio::test]
async fn records_taken_offline_land_once_the_bucket_is_back() {
    let Some((direct, cloud, proxy)) = s3_behind_proxy(Link::Up).await else {
        return;
    };
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), &cloud).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        proxy.set(Link::Down);
        assert_eq!(write(&fs, &queue, 2 * PER_PAGE + 1, 1).await[0], 0);
        proxy.set(Link::Up);
        timeout(Duration::from_secs(30), fs.flush_queue(&queue))
            .await
            .expect("the records never landed")
            .unwrap();
        fs.close().await.unwrap();
    }
    assert_eq!(objects(&direct, &queue).await, 3);
    let reserve = PER_PAGE - 1 + RESERVE;
    assert_eq!(
        pointer(temp.path(), &queue),
        [reserve.to_string(), "3".into()]
    );

    // A restart while offline goes past the reserve. What the closed life
    // still held in memory is lost: no upload of it started, so the next
    // life may hand its ids out again.
    proxy.set(Link::Down);
    let (root, life2_cloud, life2_queue) =
        (temp.path().to_path_buf(), cloud.clone(), queue.clone());
    own_runtime(async move {
        let fs = open(&root, &life2_cloud).await;
        fs.ensure_queue_exists_for_write(&life2_queue)
            .await
            .unwrap();
        assert_eq!(
            write(&fs, &life2_queue, 2, 2).await,
            [reserve + 1, reserve + 2]
        );
        assert!(fs.close().await.is_err(), "the bucket is down");
    })
    .await;
    {
        let fs = open(temp.path(), &cloud).await;
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        assert_eq!(write(&fs, &queue, 1, 3).await, [reserve + 1]);
        proxy.set(Link::Up);
        timeout(Duration::from_secs(30), fs.flush_queue(&queue))
            .await
            .expect("the record never landed")
            .unwrap();
        fs.close().await.unwrap();
    }

    let fs = open(temp.path(), &cloud).await;
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    assert_eq!(lives(&fs, &queue, 0, 9).await, [1; 9]);
    assert_eq!(lives(&fs, &queue, reserve + 1, 1).await, [3]);
    assert_eq!(
        ids(&fs, &queue, 8, reserve).await,
        [8, reserve + 1],
        "the read skips the gap"
    );
    assert_eq!(
        fs.get_last_id(&queue).unwrap().to_u64().unwrap(),
        reserve + 1
    );
    assert_eq!(objects(&direct, &queue).await, 4);
    fs.close().await.unwrap();
}

/// A life that ends after its upload reached the bucket but before it heard
/// back, as in a crash or a cut link: neither the object nor its ids are
/// used again by a restart that cannot list the bucket.
#[tokio::test]
async fn an_upload_that_landed_unrecorded_is_not_written_over() {
    let Some((direct, cloud, proxy)) = s3_behind_proxy(Link::Up).await else {
        return;
    };
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), &cloud).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 2 * PER_PAGE, 1).await;
        fs.flush_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }
    assert_eq!(objects(&direct, &queue).await, 2);
    let reserve = PER_PAGE - 1 + RESERVE;

    let (root, life2_cloud, link, life2_direct, life2_queue) = (
        temp.path().to_path_buf(),
        cloud.clone(),
        proxy.link.clone(),
        direct.clone(),
        queue.clone(),
    );
    own_runtime(async move {
        let fs = Arc::new(open(&root, &life2_cloud).await);
        fs.ensure_queue_exists_for_write(&life2_queue)
            .await
            .unwrap();
        assert_eq!(write(&fs, &life2_queue, PER_PAGE, 2).await, [8, 9, 10, 11]);
        *link.lock().unwrap() = Link::Swallow;
        let (flushing, flush_queue) = (fs.clone(), life2_queue.clone());
        tokio::spawn(async move { flushing.flush_queue(&flush_queue).await });
        let deadline = Instant::now() + Duration::from_secs(30);
        while objects(&life2_direct, &life2_queue).await < 3 {
            assert!(
                Instant::now() < deadline,
                "the upload never reached the bucket"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(
        pointer(temp.path(), &queue),
        [reserve.to_string(), "2".into()],
        "the reserve covers file 3, which is not recorded as landed"
    );

    proxy.set(Link::Down);
    {
        let fs = open(temp.path(), &cloud).await;
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        let ids = write(&fs, &queue, PER_PAGE, 3).await;
        assert_eq!(
            ids,
            (reserve + 1..reserve + 1 + PER_PAGE).collect::<Vec<_>>()
        );
        proxy.set(Link::Up);
        timeout(Duration::from_secs(30), fs.flush_queue(&queue))
            .await
            .expect("the records never landed")
            .unwrap();
        fs.close().await.unwrap();
    }
    assert_eq!(objects(&direct, &queue).await, 4);
    assert_eq!(pointer(temp.path(), &queue)[1], "4");

    let fs = open(temp.path(), &cloud).await;
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    let mut expected = vec![1u8; 8];
    expected.extend([2; 4]);
    assert_eq!(lives(&fs, &queue, 0, 12).await, expected);
    assert_eq!(lives(&fs, &queue, reserve + 1, PER_PAGE).await, [3; 4]);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_durable_queue_ignores_a_record_an_older_version_kept_for_memory() {
    let temp = tempfile::TempDir::new().unwrap();
    let cloud = refusing();
    let queue = known_instance(temp.path(), &cloud).await;
    std::fs::write(
        temp.path().join(".memory_pointers"),
        format!("# normfs memory-only pointers v1\n{}\t5\n", queue.as_str()),
    )
    .unwrap();
    let mut settings = settings(cloud);
    settings.queue_settings = QueueSettings::all_active().with_default_persist(Persist {
        store: true,
        ..Persist::CLOUD
    });
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
}
