//! How long each read's sending side waits for the reader's next entry, logged
//! per read over a sliding window. A slow client keeps the reader's channel full
//! and never shows a wait; a reader that cannot load its next file in time does.

use log::info;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
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

fn period_of(at: Instant) -> u64 {
    at.saturating_duration_since(base()).as_secs() / PERIOD.as_secs()
}

fn period_start(p: u64) -> Instant {
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
        }
    }

    fn slot(&mut self, period: u64) -> &mut Slot {
        if self.slots.back().is_none_or(|s| s.period != period) {
            self.slots.push_back(Slot {
                period,
                hist: Hist::new(),
                busy_us: 0,
            });
        }
        self.slots.back_mut().expect("pushed above")
    }

    /// A wait from `start` to `end`; waits of one read never overlap, so
    /// periods only grow.
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

    pub(crate) fn line(&self, label: &str, now: Instant, ended: bool) -> Option<String> {
        let (hist, mut busy_us) = self.window(now);
        let waiting = self.waiting.map(|at| now.saturating_duration_since(at));
        if hist.n == 0 && waiting.is_none() {
            return None;
        }
        let span = self.span(now);
        // A wait still running counts for the part of it inside the window.
        if let Some(w) = waiting {
            busy_us += w.min(span).as_micros() as u64;
        }
        let mut l = line(label, &hist, busy_us, span, waiting);
        if ended {
            l += ", ended";
        }
        Some(l)
    }
}

struct Shared {
    label: String,
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

/// One read's place in the log, removed on drop.
pub(crate) struct ReadWait {
    id: u64,
    shared: Arc<Shared>,
}

/// Marks a read that went on to follow the queue: its waits from then on are
/// for new writes, not for the server.
pub(crate) struct Following(Arc<Shared>);

impl Following {
    /// A wait still running ends here: what came before was the server's.
    pub(crate) fn mark(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.following = true;
        if let Some(at) = state.waiting.take() {
            state.add(at, Instant::now());
        }
    }

    #[cfg(test)]
    pub(crate) fn waits(&self) -> (u64, Duration) {
        let (h, busy_us) = self.0.state.lock().unwrap().window(Instant::now());
        (h.n, Duration::from_micros(busy_us))
    }
}

impl ReadWait {
    pub(crate) fn start(label: String) -> Self {
        let shared = Arc::new(Shared {
            label,
            state: Mutex::new(State::new(Instant::now())),
        });
        let mut reg = registry().lock().unwrap();
        let id = reg.next;
        reg.next += 1;
        reg.reads.insert(id, shared.clone());
        reg.keep_reporting();
        Self { id, shared }
    }

    pub(crate) fn following(&self) -> Following {
        Following(self.shared.clone())
    }

    /// A wait that ends the read counts too: a read that stalls and then
    /// fails is the one most worth seeing.
    pub(crate) async fn recv<T>(&self, rx: &mut mpsc::Receiver<T>) -> Option<T> {
        match rx.try_recv() {
            Ok(v) => return Some(v),
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
            return rx.recv().await;
        }
        let got = rx.recv().await;
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
            state.line(&self.shared.label, now, true)
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
    label: &str,
    h: &Hist,
    busy_us: u64,
    span: Duration,
    waiting: Option<Duration>,
) -> String {
    let mut s = format!(
        "Read waits over {:.0} s ({}): waits={} total={:.1} ms ({:.1} %)",
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

/// Lines for the reads that ended since the last report and every live read
/// that waited in the window, or `None` once there is no read left, which
/// also stops the loop.
fn report() -> Option<Vec<String>> {
    let (mut lines, unlisted, mut reads) = {
        let mut reg = registry().lock().unwrap();
        if reg.reads.is_empty() && reg.ended.is_empty() {
            reg.reporting = false;
            return None;
        }
        let reads: Vec<_> = reg.reads.iter().map(|(id, s)| (*id, s.clone())).collect();
        let unlisted = std::mem::take(&mut reg.unlisted);
        (std::mem::take(&mut reg.ended), unlisted, reads)
    };
    if unlisted > 0 {
        lines.push(format!("{unlisted} more ended reads waited"));
    }
    reads.sort_unstable_by_key(|(id, _)| *id);
    let (mut live, mut more) = (0, 0);
    let mut done = Vec::new();
    for (id, shared) in reads {
        let mut state = shared.state.lock().unwrap();
        // Taken under the lock, so no wait can end after it and still count.
        let Some(l) = state.line(&shared.label, Instant::now(), false) else {
            // A follow whose window emptied has nothing more to report.
            if state.following {
                state.slots = VecDeque::new();
                done.push(id);
            }
            continue;
        };
        if live < MAX_LISTED {
            lines.push(l);
            live += 1;
        } else {
            more += 1;
        }
    }
    if !done.is_empty() {
        let mut reg = registry().lock().unwrap();
        for id in done {
            reg.reads.remove(&id);
        }
    }
    if more > 0 {
        lines.push(format!("{more} more reads waited"));
    }
    Some(lines)
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
