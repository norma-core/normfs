//! How long each read's sending side waits for the reader's next entry, logged
//! per read over a sliding window. A slow client keeps the reader's channel full
//! and never shows a wait; a reader that cannot load its next file in time does.
//! Each queue's writes and the load of the pools behind them go in the same log.

use log::info;
use normfs_types::QueueId;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tokio::task::JoinHandle;

const PERIOD: Duration = Duration::from_secs(10);
/// Periods in the window.
const SLOTS: u64 = 6;
const BUCKETS: usize = 160;
/// Reads listed in one report, live and ended each; the rest are only counted.
const MAX_LISTED: usize = 64;

/// Waits in microseconds, four buckets per power of two.
#[derive(Clone)]
pub(crate) struct Hist {
    pub(crate) n: u64,
    pub(crate) max_us: u64,
    counts: [u32; BUCKETS],
}

fn bucket(us: u64) -> usize {
    if us < 4 {
        return us as usize;
    }
    let b = 63 - us.leading_zeros() as usize;
    ((b - 1) * 4 + ((us >> (b - 2)) & 3) as usize).min(BUCKETS - 1)
}

fn bucket_top(i: usize) -> u64 {
    if i < 4 {
        return i as u64;
    }
    let (b, sub) = (i / 4 + 1, (i % 4) as u64);
    ((4 + sub + 1) << (b - 2)) - 1
}

impl Hist {
    pub(crate) fn new() -> Self {
        Self {
            n: 0,
            max_us: 0,
            counts: [0; BUCKETS],
        }
    }

    pub(crate) fn add(&mut self, us: u64) {
        self.n += 1;
        self.max_us = self.max_us.max(us);
        let c = &mut self.counts[bucket(us)];
        *c = c.saturating_add(1);
    }

    fn merge(&mut self, other: &Hist) {
        self.n += other.n;
        self.max_us = self.max_us.max(other.max_us);
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a = a.saturating_add(*b);
        }
    }

    /// The top of the bucket holding the `p` quantile, so at most a quarter
    /// over the true value, and never over the max.
    pub(crate) fn quantile_us(&self, p: f64) -> u64 {
        let rank = ((p * self.n as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, c) in self.counts.iter().enumerate() {
            seen += *c as u64;
            if seen >= rank {
                return bucket_top(i).min(self.max_us);
            }
        }
        self.max_us
    }
}

fn base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

pub(crate) fn period_of(at: Instant) -> u64 {
    at.saturating_duration_since(base()).as_secs() / PERIOD.as_secs()
}

pub(crate) fn period_start(p: u64) -> Instant {
    base() + PERIOD * p as u32
}

struct Slot {
    period: u64,
    /// Each wait whole, in the period it ended.
    hist: Hist,
    /// The part of each wait that fell in this period.
    busy_us: u64,
}

/// One read's waits by period, the last `SLOTS` of them kept.
pub(crate) struct State {
    since: Instant,
    slots: VecDeque<Slot>,
    waiting: Option<Instant>,
    following: bool,
    /// The entry count and compression time as each report saw them, to
    /// count the window's share of both.
    marks: VecDeque<(u64, u64, u64)>,
    /// Writes still running, by start.
    open: Vec<Instant>,
}

impl State {
    pub(crate) fn new(since: Instant) -> Self {
        // Periods count from the first read; a wait before the base would lose its start.
        base();
        Self {
            since,
            slots: VecDeque::new(),
            waiting: None,
            following: false,
            marks: VecDeque::new(),
            open: Vec::new(),
        }
    }

    fn slot(&mut self, period: u64) -> &mut Slot {
        let new = Slot {
            period,
            hist: Hist::new(),
            busy_us: 0,
        };
        let i = match self.slots.iter().position(|s| s.period >= period) {
            Some(i) if self.slots[i].period == period => i,
            Some(i) => {
                self.slots.insert(i, new);
                i
            }
            None => {
                self.slots.push_back(new);
                self.slots.len() - 1
            }
        };
        &mut self.slots[i]
    }

    /// A wait from `start` to `end`. Writes to one queue can overlap and end
    /// out of order, so each part goes to its own period.
    pub(crate) fn add(&mut self, start: Instant, end: Instant) {
        let last = period_of(end);
        for p in period_of(start).max((last + 1).saturating_sub(SLOTS))..=last {
            let lo = start.max(period_start(p));
            let hi = end.min(period_start(p + 1));
            self.slot(p).busy_us += hi.saturating_duration_since(lo).as_micros() as u64;
        }
        let whole = end.saturating_duration_since(start).as_micros() as u64;
        self.slot(last).hist.add(whole);
        while self.slots.front().is_some_and(|s| s.period + SLOTS <= last) {
            self.slots.pop_front();
        }
    }

    /// The waits that ended in the window and the time spent waiting in it.
    pub(crate) fn window(&self, now: Instant) -> (Hist, u64) {
        let p = period_of(now);
        let mut hist = Hist::new();
        let mut busy_us = 0;
        for s in self.slots.iter().filter(|s| s.period + SLOTS > p) {
            hist.merge(&s.hist);
            busy_us += s.busy_us;
        }
        (hist, busy_us)
    }

    /// How much of the window the read has been there for.
    pub(crate) fn span(&self, now: Instant) -> Duration {
        let first = (period_of(now) + 1).saturating_sub(SLOTS);
        now.saturating_duration_since(self.since.max(period_start(first)))
    }

    pub(crate) fn start_wait(&mut self, at: Instant) {
        self.waiting = Some(at);
    }

    /// Entries since the window began, from the count each report saw; a
    /// report every period keeps it within one period of exact.
    #[cfg(test)]
    pub(crate) fn entries(&mut self, total: u64, now: Instant) -> u64 {
        self.in_window((total, 0), now).0
    }

    /// Entries and compression time since the window began, from the totals
    /// each report saw; a report every period keeps them within one period.
    fn in_window(&mut self, (entries, compress_us): (u64, u64), now: Instant) -> (u64, u64) {
        let p = period_of(now);
        let first = (p + 1).saturating_sub(SLOTS);
        if self.marks.back().is_none_or(|(at, _, _)| *at != p) {
            self.marks.push_back((p, entries, compress_us));
        }
        while self.marks.front().is_some_and(|(at, _, _)| *at < first) {
            self.marks.pop_front();
        }
        // A read that began inside the window took all its entries in it, as
        // does every read while the window still reaches back to the first one.
        if first == 0 || self.since >= period_start(first) {
            return (entries, compress_us);
        }
        let (_, e0, c0) = self.marks.front().copied().unwrap_or((0, 0, 0));
        (entries - e0, compress_us - c0)
    }

    /// The line, the time waited in the window, and whether there was a wait.
    fn render(
        &mut self,
        what: &str,
        label: &str,
        totals: (u64, u64),
        now: Instant,
    ) -> (String, u64, bool) {
        let (entries, compress_us) = self.in_window(totals, now);
        let (hist, mut busy_us) = self.window(now);
        let span = self.span(now);
        // Waits still running count for their part inside the window; the
        // oldest shows as waiting=.
        let running = self.waiting.iter().chain(self.open.iter());
        let running: Vec<Duration> = running
            .map(|at| now.saturating_duration_since(*at))
            .collect();
        for w in &running {
            busy_us += w.min(&span).as_micros() as u64;
        }
        let waiting = running.into_iter().max();
        let waited = hist.n > 0 || waiting.is_some();
        let mut l = line(what, label, entries, &hist, busy_us, span, waiting);
        if totals.1 > 0 {
            let share = compress_us as f64 / 1e4 / span.as_secs_f64().max(1e-6);
            l += &format!(" compress={share:.1} %");
        }
        (l, busy_us, waited)
    }

    #[cfg(test)]
    pub(crate) fn line(&mut self, label: &str, now: Instant, ended: bool) -> Option<String> {
        let (mut l, _, waited) = self.render("Read", label, (0, 0), now);
        if !waited {
            return None;
        }
        if ended {
            l += ", ended";
        }
        Some(l)
    }
}

struct Shared {
    label: String,
    /// Entries taken, or records written.
    taken: AtomicU64,
    /// Time spent compressing what the read sends.
    compress_us: AtomicU64,
    state: Mutex<State>,
}

#[derive(Default)]
struct Registry {
    next: u64,
    reads: HashMap<u64, Arc<Shared>>,
    ended: Vec<String>,
    unlisted: u64,
    reporting: bool,
    task: Option<JoinHandle<()>>,
}

impl Registry {
    fn keep_reporting(&mut self) {
        // A loop the runtime dropped before it ran never cleared the flag.
        if self.reporting && self.task.as_ref().is_some_and(|t| !t.is_finished()) {
            return;
        }
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            self.reporting = true;
            self.task = Some(rt.spawn(report_loop()));
        }
    }
}

fn registry() -> &'static Mutex<Registry> {
    static REG: OnceLock<Mutex<Registry>> = OnceLock::new();
    REG.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) fn ended_read(label: &str) -> Option<String> {
    let label = format!("({label}):");
    registry()
        .lock()
        .unwrap()
        .ended
        .iter()
        .find(|line| line.contains(&label))
        .cloned()
}

/// One read's place in the log, removed on drop.
pub struct ReadWait {
    id: u64,
    shared: Arc<Shared>,
}

/// Marks a read that went on to follow the queue: its waits from then on are
/// for new writes, not for the server.
pub struct Following(Arc<Shared>);

/// Where the time compressing a read's entries goes: to the read's line and to
/// the process total behind the pools line.
/// Retains the reporting owner so pending compression reaches the final log.
#[derive(Clone, Default)]
pub struct CompressMeter(Option<Arc<ReadWait>>);

static COMPRESS_US: AtomicU64 = AtomicU64::new(0);

impl CompressMeter {
    pub fn add(&self, took: Duration) {
        let us = took.as_micros() as u64;
        COMPRESS_US.fetch_add(us, Ordering::Relaxed);
        if let Some(wait) = &self.0 {
            wait.shared.compress_us.fetch_add(us, Ordering::Relaxed);
        }
    }
}

/// Cumulative elapsed time in compression calls, including scheduling delays.
pub fn compress_total() -> Duration {
    Duration::from_micros(COMPRESS_US.load(Ordering::Relaxed))
}

impl Following {
    /// A wait still running ends here: what came before was the server's.
    pub fn mark(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.following = true;
        if let Some(at) = state.waiting.take() {
            state.add(at, Instant::now());
        }
    }

    #[cfg(test)]
    pub(crate) fn line(&self) -> String {
        let totals = (
            self.0.taken.load(Ordering::Relaxed),
            self.0.compress_us.load(Ordering::Relaxed),
        );
        let mut state = self.0.state.lock().unwrap();
        state
            .render("Read", &self.0.label, totals, Instant::now())
            .0
    }

    #[cfg(test)]
    pub(crate) fn waits(&self) -> (u64, Duration) {
        let (h, busy_us) = self.0.state.lock().unwrap().window(Instant::now());
        (h.n, Duration::from_micros(busy_us))
    }
}

impl ReadWait {
    pub fn start(label: String) -> Self {
        let shared = Arc::new(Shared {
            label,
            taken: AtomicU64::new(0),
            compress_us: AtomicU64::new(0),
            state: Mutex::new(State::new(Instant::now())),
        });
        let mut reg = registry().lock().unwrap();
        let id = reg.next;
        reg.next += 1;
        reg.reads.insert(id, shared.clone());
        reg.keep_reporting();
        Self { id, shared }
    }

    pub fn compress_meter(self: &Arc<Self>) -> CompressMeter {
        CompressMeter(Some(self.clone()))
    }

    pub fn following(&self) -> Following {
        Following(self.shared.clone())
    }

    /// A wait that ends the read counts too: a read that stalls and then
    /// fails is the one most worth seeing.
    pub async fn recv<T>(&self, rx: &mut mpsc::Receiver<T>) -> Option<T> {
        match rx.try_recv() {
            Ok(v) => {
                self.shared.taken.fetch_add(1, Ordering::Relaxed);
                return Some(v);
            }
            Err(TryRecvError::Disconnected) => return None,
            Err(TryRecvError::Empty) => {}
        }
        // Under the lock `mark` takes, so a wait never starts after it.
        let follows = {
            let mut state = self.shared.state.lock().unwrap();
            if !state.following {
                state.start_wait(Instant::now());
            }
            state.following
        };
        if follows {
            let got = rx.recv().await;
            if got.is_some() {
                self.shared.taken.fetch_add(1, Ordering::Relaxed);
            }
            return got;
        }
        let got = rx.recv().await;
        if got.is_some() {
            self.shared.taken.fetch_add(1, Ordering::Relaxed);
        }
        let mut state = self.shared.state.lock().unwrap();
        if let Some(at) = state.waiting.take() {
            state.add(at, Instant::now());
        }
        got
    }
}

/// A read that waited is listed once more after it ends, so one shorter than
/// a period shows up too.
impl Drop for ReadWait {
    fn drop(&mut self) {
        let last = {
            let mut state = self.shared.state.lock().unwrap();
            let now = Instant::now();
            // A recv cancelled mid-wait never recorded it.
            if let Some(at) = state.waiting.take() {
                state.add(at, now);
            }
            let totals = (
                self.shared.taken.load(Ordering::Relaxed),
                self.shared.compress_us.load(Ordering::Relaxed),
            );
            let (mut l, _, waited) = state.render("Read", &self.shared.label, totals, now);
            l += ", ended";
            waited.then_some(l)
        };
        let mut reg = registry().lock().unwrap();
        reg.reads.remove(&self.id);
        if let Some(last) = last {
            if reg.ended.len() < MAX_LISTED {
                reg.ended.push(last);
            } else {
                reg.unlisted += 1;
            }
            reg.keep_reporting();
        }
    }
}

fn ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

pub(crate) fn line(
    what: &str,
    label: &str,
    entries: u64,
    h: &Hist,
    busy_us: u64,
    span: Duration,
    waiting: Option<Duration>,
) -> String {
    let mut s = format!(
        "{what} waits over {:.0} s ({}): entries={entries} waits={} total={:.1} ms ({:.1} %)",
        span.as_secs_f64(),
        label,
        h.n,
        ms(busy_us),
        busy_us as f64 / 1e4 / span.as_secs_f64().max(1e-6),
    );
    if h.n > 0 {
        s += &format!(
            " p50={:.1} ms p90={:.1} ms p99={:.1} ms max={:.1} ms",
            ms(h.quantile_us(0.5)),
            ms(h.quantile_us(0.9)),
            ms(h.quantile_us(0.99)),
            ms(h.max_us),
        );
    }
    if let Some(w) = waiting {
        s += &format!(" waiting={:.1} ms", w.as_secs_f64() * 1000.0);
    }
    s
}

/// A snapshot of every live read, those that waited longest first, then the
/// reads that ended since the last report; `None` once there is no read
/// left, which also stops the loop.
fn report() -> Option<Vec<String>> {
    let (ended, unlisted, mut reads) = {
        let mut reg = registry().lock().unwrap();
        if reg.reads.is_empty() && reg.ended.is_empty() && writers().read().unwrap().is_empty() {
            reg.reporting = false;
            return None;
        }
        let reads: Vec<_> = reg.reads.iter().map(|(id, s)| (*id, s.clone())).collect();
        let unlisted = std::mem::take(&mut reg.unlisted);
        (std::mem::take(&mut reg.ended), unlisted, reads)
    };
    reads.sort_unstable_by_key(|(id, _)| *id);
    let mut live = Vec::new();
    let mut done = Vec::new();
    for (id, shared) in reads {
        let mut state = shared.state.lock().unwrap();
        let totals = (
            shared.taken.load(Ordering::Relaxed),
            shared.compress_us.load(Ordering::Relaxed),
        );
        // Taken under the lock, so no wait can end after it and still count.
        let (l, busy_us, waited) = state.render("Read", &shared.label, totals, Instant::now());
        // A follow whose window emptied has nothing more to report.
        if state.following && !waited {
            state.slots = VecDeque::new();
            done.push(id);
            continue;
        }
        live.push((l, busy_us, waited));
    }
    if !done.is_empty() {
        let mut reg = registry().lock().unwrap();
        for id in done {
            reg.reads.remove(&id);
        }
    }
    let waited = live.iter().filter(|(_, _, w)| *w).count();
    let mut lines = vec![format!(
        "{} live reads, {waited} waited in the last {} s",
        live.len(),
        (PERIOD * SLOTS as u32).as_secs()
    )];
    live.sort_by_key(|(_, busy_us, _)| std::cmp::Reverse(*busy_us));
    let more = live.len().saturating_sub(MAX_LISTED);
    lines.extend(live.into_iter().take(MAX_LISTED).map(|(l, _, _)| l));
    if more > 0 {
        lines.push(format!("{more} more live reads"));
    }
    lines.extend(ended);
    if unlisted > 0 {
        lines.push(format!("{unlisted} more ended reads waited"));
    }
    lines.extend(write_lines());
    let pools: Vec<String> = {
        let mut probes = probes().lock().unwrap();
        let mut out = Vec::new();
        probes.retain(|probe| probe().map(|l| out.push(l)).is_some());
        out
    };
    if !pools.is_empty() {
        lines.push(format!("Pools: {}", pools.join("; ")));
    }
    Some(lines)
}

type Probe = Box<dyn Fn() -> Option<String> + Send>;

fn probes() -> &'static Mutex<Vec<Probe>> {
    static PROBES: OnceLock<Mutex<Vec<Probe>>> = OnceLock::new();
    PROBES.get_or_init(Default::default)
}

/// A pool's load for each report; dropped once it returns `None`, as when
/// what it watches is gone.
pub fn add_probe(probe: impl Fn() -> Option<String> + Send + 'static) {
    probes().lock().unwrap().push(Box::new(probe));
}

fn writers() -> &'static RwLock<HashMap<QueueId, Arc<Shared>>> {
    static WRITERS: OnceLock<RwLock<HashMap<QueueId, Arc<Shared>>>> = OnceLock::new();
    WRITERS.get_or_init(Default::default)
}

/// Times one enqueue of `records` records into `queue`, until dropped.
pub(crate) struct WriteTimer {
    shared: Arc<Shared>,
    started: Instant,
}

impl WriteTimer {
    pub(crate) fn start(queue: &QueueId, records: u64) -> Self {
        let started = Instant::now();
        // Opened under the map's lock, so a report cannot drop the queue in between.
        let open = |shared: &Arc<Shared>| {
            shared.taken.fetch_add(records, Ordering::Relaxed);
            shared.state.lock().unwrap().open.push(started);
            shared.clone()
        };
        let found = writers().read().unwrap().get(queue).map(open);
        let shared = found.unwrap_or_else(|| {
            let shared = open(
                writers()
                    .write()
                    .unwrap()
                    .entry(queue.clone())
                    .or_insert_with(|| {
                        Arc::new(Shared {
                            label: format!("queue_id: {}", queue.short()),
                            taken: AtomicU64::new(0),
                            compress_us: AtomicU64::new(0),
                            state: Mutex::new(State::new(started)),
                        })
                    }),
            );
            registry().lock().unwrap().keep_reporting();
            shared
        });
        Self { shared, started }
    }
}

/// Writes to `queue` in the window, the records in them, and the time they took.
#[cfg(test)]
pub(crate) fn write_load(queue: &QueueId) -> Option<(u64, u64, Duration)> {
    let shared = writers().read().unwrap().get(queue)?.clone();
    let (h, busy_us) = shared.state.lock().unwrap().window(Instant::now());
    let records = shared.taken.load(Ordering::Relaxed);
    Some((h.n, records, Duration::from_micros(busy_us)))
}

impl Drop for WriteTimer {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(i) = state.open.iter().position(|at| *at == self.started) {
            state.open.swap_remove(i);
        }
        state.add(self.started, Instant::now());
    }
}

/// Every queue written in the window, the longest waits first; a queue with
/// no write in it and none running leaves.
pub(crate) fn write_lines() -> Vec<String> {
    let all: Vec<_> = writers()
        .read()
        .unwrap()
        .iter()
        .map(|(q, s)| (q.clone(), s.clone()))
        .collect();
    let mut live = Vec::new();
    let mut idle = Vec::new();
    for (queue, shared) in all {
        let mut state = shared.state.lock().unwrap();
        let taken = shared.taken.load(Ordering::Relaxed);
        let (l, busy_us, wrote) = state.render("Write", &shared.label, (taken, 0), Instant::now());
        if wrote {
            live.push((l, busy_us));
        } else {
            idle.push(queue);
        }
    }
    if !idle.is_empty() {
        let mut writers = writers().write().unwrap();
        for queue in idle {
            let still_idle = writers.get(&queue).is_some_and(|shared| {
                let state = shared.state.lock().unwrap();
                state.open.is_empty() && state.window(Instant::now()).0.n == 0
            });
            if still_idle {
                writers.remove(&queue);
            }
        }
    }
    if live.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![format!(
        "{} queues written in the last {} s",
        live.len(),
        (PERIOD * SLOTS as u32).as_secs()
    )];
    live.sort_by_key(|(_, busy_us)| std::cmp::Reverse(*busy_us));
    let more = live.len().saturating_sub(MAX_LISTED);
    lines.extend(live.into_iter().take(MAX_LISTED).map(|(l, _)| l));
    if more > 0 {
        lines.push(format!("{more} more queues written"));
    }
    lines
}

async fn report_loop() {
    loop {
        tokio::time::sleep(PERIOD).await;
        let Some(lines) = report() else {
            return;
        };
        for l in lines {
            info!(target: "normfs-read-wait", "{}", l);
        }
    }
}
