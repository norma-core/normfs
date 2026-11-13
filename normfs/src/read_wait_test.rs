use crate::read_wait::{line, Hist, ReadWait, State};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[test]
fn test_quantile_is_within_a_quarter_over() {
    for us in [1u64, 3, 4, 7, 100, 999, 12_345, 2_000_000, 1 << 40] {
        let mut h = Hist::new();
        h.add(us);
        h.add(us * 2);
        let q = h.quantile_us(0.5);
        assert!(q >= us && q <= us + us / 4, "{us}: {q}");
    }
}

#[test]
fn test_quantiles_of_a_spread() {
    let mut h = Hist::new();
    for ms in 1..=1000u64 {
        h.add(ms * 1000);
    }
    let p50 = h.quantile_us(0.5);
    let p99 = h.quantile_us(0.99);
    assert!((500_000..=625_000).contains(&p50), "{p50}");
    assert!((990_000..=1_000_000).contains(&p99), "{p99}");
    assert_eq!(h.quantile_us(1.0), 1_000_000);
}

#[test]
fn test_line_without_waits_has_no_quantiles() {
    let l = line(
        "Read",
        "read_id: 1",
        7,
        &Hist::new(),
        0,
        Duration::from_secs(60),
        None,
    );
    assert_eq!(
        l,
        "Read waits over 60 s (read_id: 1): entries=7 waits=0 total=0.0 ms (0.0 %)"
    );
}

#[test]
fn test_a_wait_longer_than_the_window_fills_it_at_most() {
    let t0 = Instant::now();
    let mut s = State::new(t0);
    let end = t0 + Duration::from_secs(70);
    s.add(t0, end);
    let (h, busy_us) = s.window(end);
    assert_eq!(h.n, 1);
    assert_eq!(h.max_us, 70_000_000);
    assert!(busy_us as u128 <= s.span(end).as_micros(), "{busy_us}");
}

#[test]
fn test_an_old_wait_leaves_the_window() {
    let t0 = Instant::now();
    let mut s = State::new(t0);
    s.add(t0, t0 + Duration::from_millis(5));
    assert_eq!(s.window(t0 + Duration::from_millis(10)).0.n, 1);
    assert_eq!(s.window(t0 + Duration::from_secs(70)).0.n, 0);
    assert!(s
        .line("read_id: 1", t0 + Duration::from_secs(70), false)
        .is_none());
}

#[tokio::test]
async fn test_only_a_blocked_recv_counts() {
    let wait = ReadWait::start("read_id: 1".into());
    let probe = wait.following();
    let (tx, mut rx) = mpsc::channel(4);
    tx.send(1).await.unwrap();
    assert_eq!(wait.recv(&mut rx).await, Some(1));

    let late = tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        late.send(2).await.unwrap();
    });
    assert_eq!(wait.recv(&mut rx).await, Some(2));
    let (n, total) = probe.waits();
    assert_eq!(n, 1);
    assert!(total >= Duration::from_millis(30), "{total:?}");
}

#[tokio::test]
async fn test_a_wait_that_ends_the_read_counts() {
    let wait = ReadWait::start("read_id: 2".into());
    let probe = wait.following();
    let (tx, mut rx) = mpsc::channel::<u32>(4);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(tx);
    });
    assert_eq!(wait.recv(&mut rx).await, None);
    assert_eq!(probe.waits().0, 1);
}

#[tokio::test]
async fn test_a_following_read_stops_counting() {
    let wait = ReadWait::start("read_id: 3".into());
    let probe = wait.following();
    let (tx, mut rx) = mpsc::channel(4);
    probe.mark();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(1).await.unwrap();
    });
    assert_eq!(wait.recv(&mut rx).await, Some(1));
    assert_eq!(probe.waits().0, 0);
}

#[tokio::test]
async fn test_a_wait_running_when_the_read_follows_ends_there() {
    let wait = ReadWait::start("read_id: 4".into());
    let probe = wait.following();
    let (tx, mut rx) = mpsc::channel(4);
    let drain = tokio::spawn(async move {
        let got = wait.recv(&mut rx).await;
        (wait, got)
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    probe.mark();
    let (n, total) = probe.waits();
    assert_eq!(n, 1);
    assert!(total >= Duration::from_millis(30), "{total:?}");
    tokio::time::sleep(Duration::from_millis(20)).await;
    tx.send(1).await.unwrap();
    let (_wait, got) = drain.await.unwrap();
    assert_eq!(got, Some(1));
    assert_eq!(probe.waits(), (n, total));
}

#[test]
fn test_a_running_wait_counts_in_the_total() {
    let mut s = State::new(Instant::now());
    let t0 = Instant::now();
    s.start_wait(t0);
    let l = s
        .line("read_id: 5", t0 + Duration::from_secs(5), false)
        .unwrap();
    assert!(l.contains("waits=0 total=5000.0 ms ("), "{l}");
    assert!(l.ends_with("waiting=5000.0 ms"), "{l}");
}

#[tokio::test]
async fn test_a_cancelled_wait_counts_when_the_read_ends() {
    let wait = ReadWait::start("read_id: 6".into());
    let probe = wait.following();
    let (_tx, mut rx) = mpsc::channel::<u32>(4);
    let pending = tokio::time::timeout(Duration::from_millis(20), wait.recv(&mut rx)).await;
    assert!(pending.is_err());
    assert_eq!(probe.waits().0, 0);
    drop(wait);
    let (n, total) = probe.waits();
    assert_eq!(n, 1);
    assert!(total >= Duration::from_millis(20), "{total:?}");
}

#[tokio::test]
async fn test_a_write_is_timed_per_queue() {
    use super::read_wait::WriteTimer;
    let queue = normfs_types::QueueIdResolver::new("test").resolve("timed-write");
    drop(WriteTimer::start(&queue, 3));
    let timer = WriteTimer::start(&queue, 2);
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(timer);
    let (waits, records, total) = super::read_wait::write_load(&queue).unwrap();
    assert_eq!((waits, records), (2, 5));
    assert!(total >= Duration::from_millis(20), "{total:?}");
}

#[tokio::test]
async fn test_an_unfinished_write_stays_listed() {
    use super::read_wait::{write_lines, write_load, WriteTimer};
    let queue = normfs_types::QueueIdResolver::new("test").resolve("stuck-write");
    let timer = WriteTimer::start(&queue, 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let lines = write_lines();
    let ours = lines
        .iter()
        .find(|l| l.contains("queue_id: stuck-write"))
        .unwrap();
    assert!(
        ours.contains("waits=0") && ours.contains("waiting="),
        "{ours}"
    );
    assert!(write_load(&queue).is_some());
    drop(timer);
    assert_eq!(write_load(&queue).unwrap().0, 1);
}

#[test]
fn test_overlapping_writes_keep_their_periods() {
    let mut s = State::new(Instant::now());
    let t0 = super::read_wait::period_start(super::read_wait::period_of(Instant::now()) + 1);
    let at = |secs: u64| t0 + Duration::from_secs(secs);
    s.add(at(1), at(25));
    s.add(at(2), at(5));
    // The window now starts one period after t0: 15 s of the long write, none of the short.
    let (h, busy_us) = s.window(at(61));
    assert_eq!(h.n, 1);
    assert_eq!(busy_us, 15_000_000);
}

#[test]
fn test_a_new_read_counts_all_its_entries() {
    let mut s = State::new(Instant::now());
    assert_eq!(s.entries(5, Instant::now()), 5);
    assert_eq!(s.entries(9, Instant::now()), 9);
}

#[tokio::test]
async fn test_running_writes_all_count_in_the_total() {
    use super::read_wait::{write_lines, WriteTimer};
    let queue = normfs_types::QueueIdResolver::new("test").resolve("two-writes");
    let (a, b) = (WriteTimer::start(&queue, 1), WriteTimer::start(&queue, 1));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let lines = write_lines();
    let ours = lines
        .iter()
        .find(|l| l.contains("queue_id: two-writes"))
        .unwrap();
    let total: f64 = ours
        .split("total=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(total >= 60.0, "{ours}");
    drop((a, b));
}

#[test]
fn test_compression_shows_on_the_read_and_in_the_total() {
    let wait = Arc::new(ReadWait::start("read_id: 7".into()));
    let probe = wait.following();
    assert!(!probe.line().contains("compress="));
    let before = super::read_wait::compress_total();
    wait.compress_meter().add(Duration::from_millis(40));
    assert!(super::read_wait::compress_total() - before >= Duration::from_millis(40));
    let l = probe.line();
    let share: f64 = l
        .split("compress=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(share > 0.0, "{l}");
}
