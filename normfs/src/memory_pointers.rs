use bytes::Bytes;
use normfs_fs::{Fs, PublishSpec, Runs, TmpMode};
use normfs_types::QueueId;
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use uintn::UintN;

const POINTERS_FILE: &str = ".memory_pointers";
const POINTERS_TMP_FILE: &str = ".memory_pointers.tmp";

const HEADER: &str = "# normfs pointers v1: queue, id reserve, last landed file, exact\n";
/// Fourth column of a queue whose last file no upload can have gone past.
/// Parsers before 0.4.2 read three columns and ignore the rest.
const EXACT: &str = "exact";

/// Ids reserved past the one an upload needs, so the file is rewritten once
/// per this many ids rather than once per upload. A restart that cannot list
/// the bucket skips what was left of it.
pub(crate) const RESERVE_AHEAD: u64 = 1 << 16;

/// What survives a restart: an id no earlier life has gone past, and for a
/// queue that was ever cloud-direct the last file known to have landed, 0
/// before the first. A line without a file is a memory life's last id, as in
/// 0.4.2, so it names no file anywhere. The file is written lazily, so it is
/// exact only while the queue is settled; otherwise the bucket is the
/// authority for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pointer {
    pub id: u64,
    pub file: Option<u64>,
}

impl Pointer {
    /// What [`MemoryPointers::record`] writes: a cloud queue that used no id
    /// yet, so its id 0 covers nothing.
    fn is_bare(&self) -> bool {
        self.id == 0 && self.file == Some(0)
    }

    fn landed_file(&self) -> Option<u64> {
        self.file.filter(|&f| f > 0)
    }
}

struct PointerState {
    queues: HashMap<String, Pointer>,
    dirty: bool,
    /// A landed file not yet written out; never worth a write on its own.
    hints: bool,
    /// Queues whose last file on disk may be behind the bucket: an upload
    /// started since it was written, or the file was not written settled.
    unsettled: HashSet<String>,
    /// The last id each queue landed in this life.
    landed_now: HashMap<String, u64>,
    /// What the file on disk holds, so a reserve is reported only once it
    /// is written there.
    written: HashMap<String, Pointer>,
    written_exact: HashSet<String>,
    /// The last id a memory life used, which a close never lowers a line
    /// below: it is the only record of those ids.
    memory_floor: HashMap<String, u64>,
    #[cfg(test)]
    publishes: u64,
}

pub(crate) struct MemoryPointers {
    fs: Fs,
    path: PathBuf,
    tmp_path: PathBuf,
    state: Arc<Mutex<PointerState>>,
    flush_lock: Arc<tokio::sync::Mutex<()>>,
}

impl MemoryPointers {
    pub(crate) async fn open(fs: Fs, root: &Path) -> Result<Self, Error> {
        let path = root.join(POINTERS_FILE);
        let tmp_path = root.join(POINTERS_TMP_FILE);
        let (queues, exact) = match read_optional(&fs, &path).await? {
            Some(contents) => parse_pointers(&contents)?,
            None => (HashMap::new(), HashSet::new()),
        };
        // A bare record never had an upload start: the bucket holds nothing
        // of it. A memory life's line names no file.
        let unsettled = queues
            .iter()
            .filter(|(queue, p)| !exact.contains(*queue) && !p.is_bare() && p.file.is_some())
            .map(|(queue, _)| queue.clone())
            .collect();

        let memory_floor = queues
            .iter()
            .filter(|(_, p)| p.file.is_none())
            .map(|(queue, p)| (queue.clone(), p.id))
            .collect();
        let written = queues.clone();
        Ok(Self {
            fs,
            path,
            tmp_path,
            state: Arc::new(Mutex::new(PointerState {
                queues,
                dirty: false,
                hints: false,
                unsettled,
                landed_now: HashMap::new(),
                written,
                written_exact: exact,
                memory_floor,
                #[cfg(test)]
                publishes: 0,
            })),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub(crate) fn last_id(&self, queue: &QueueId) -> Option<UintN> {
        self.pointer(queue).map(|p| UintN::from(p.id))
    }

    /// The highest id an earlier life may have used, a memory-only one
    /// included; a bare record says none.
    pub(crate) fn used_id(&self, queue: &QueueId) -> Option<UintN> {
        self.pointer(queue)
            .filter(|p| !p.is_bare())
            .map(|p| UintN::from(p.id))
    }

    pub(crate) fn last_landed(&self, queue: &QueueId) -> Option<(UintN, UintN)> {
        let p = self.pointer(queue)?;
        Some((UintN::from(p.id), UintN::from(p.landed_file()?)))
    }

    /// Whether the queue's last file is known to be the bucket's last.
    pub(crate) fn is_settled(&self, queue: &QueueId) -> bool {
        let state = self.state.lock().unwrap();
        state.queues.contains_key(queue.as_str()) && !state.unsettled.contains(queue.as_str())
    }

    fn pointer(&self, queue: &QueueId) -> Option<Pointer> {
        let state = self.state.lock().unwrap();
        state.queues.get(queue.as_str()).copied()
    }

    /// Records that `file_id` holding ids up to `last_id` is in the cloud. Only
    /// an id past the reserve is written out at once, which happens only when
    /// the bucket held more than this file recorded.
    pub(crate) async fn mark_landed(
        &self,
        queue: &QueueId,
        last_id: &UintN,
        file_id: &UintN,
    ) -> Result<(), Error> {
        let last = to_u64(last_id, "id")?;
        self.advance(queue, last_id, Some(file_id))?;
        {
            let mut state = self.state.lock().unwrap();
            let landed = state
                .landed_now
                .entry(queue.as_str().to_string())
                .or_default();
            *landed = (*landed).max(last);
        }
        self.flush_if_dirty().await
    }

    /// What the bucket said it holds: its last file is now exact.
    pub(crate) async fn settle_from_bucket(
        &self,
        queue: &QueueId,
        last_id: &UintN,
        file_id: &UintN,
    ) -> Result<(), Error> {
        self.advance(queue, last_id, Some(file_id))?;
        self.settle(queue);
        self.flush_if_dirty().await
    }

    /// The bucket answered for the queue, so its last file is exact.
    pub(crate) fn settle(&self, queue: &QueueId) {
        let mut state = self.state.lock().unwrap();
        if state.unsettled.remove(queue.as_str()) {
            state.hints = true;
        }
    }

    /// Makes sure no restart hands out an id up to `id` again, writing out a
    /// reserve [`RESERVE_AHEAD`] past it when the current one falls short.
    /// The first upload of a settled queue also writes, since its last file
    /// stops being exact once that upload may have landed.
    pub(crate) async fn reserve(&self, queue: &QueueId, id: &UintN) -> Result<(), Error> {
        let id = to_u64(id, "id")?;
        let short = {
            let mut state = self.state.lock().unwrap();
            state.unsettled.insert(queue.as_str().to_string());
            // A memory life's line covers ids but names no file; an upload
            // needs a cloud line, or a crash would leave it looking settled.
            // Raised in the same step, so no flush sees it as a bare record.
            if let Some(p) = state.queues.get_mut(queue.as_str()) {
                if p.file.is_none() {
                    p.file = Some(0);
                    p.id = p.id.max(id.saturating_add(RESERVE_AHEAD));
                }
            }
            let in_memory = state
                .queues
                .get(queue.as_str())
                .is_some_and(|p| !p.is_bare() && p.id >= id);
            // Memory too: a close may have lowered it, and the next write
            // would put that lower reserve on disk.
            if in_memory && state.reserved_on_disk(queue.as_str(), id) {
                return Ok(());
            }
            // Written again even when memory already covers it: an earlier
            // write of this reserve may have failed.
            state.dirty = true;
            !in_memory
        };
        if short {
            let ahead = id.saturating_add(RESERVE_AHEAD);
            self.advance(queue, &UintN::from(ahead), Some(&UintN::zero()))?;
        }
        self.flush_if_dirty().await?;
        if self
            .state
            .lock()
            .unwrap()
            .reserved_on_disk(queue.as_str(), id)
        {
            Ok(())
        } else {
            Err(Error::other(format!(
                "queue {}: the id reserve for {id} is not on disk",
                queue.short()
            )))
        }
    }

    /// After a close that landed everything: each queue that uploaded in this
    /// life has its reserve brought down to the last id it landed, and its
    /// last file is exact again.
    pub(crate) fn settle_landed(&self) {
        let mut state = self.state.lock().unwrap();
        let state = &mut *state;
        for (queue, last) in state.landed_now.drain() {
            let floor = state.memory_floor.get(&queue).copied().unwrap_or(0);
            if let Some(entry) = state.queues.get_mut(&queue) {
                entry.id = last.max(floor);
            }
            state.unsettled.remove(&queue);
            state.dirty = true;
        }
    }

    /// [`MemoryPointers::settle_landed`] for one queue a close drained; the
    /// next write takes it along.
    pub(crate) fn settle_landed_queue(&self, queue: &QueueId) {
        let mut state = self.state.lock().unwrap();
        let state = &mut *state;
        if let Some(last) = state.landed_now.remove(queue.as_str()) {
            let floor = state.memory_floor.get(queue.as_str()).copied().unwrap_or(0);
            if let Some(entry) = state.queues.get_mut(queue.as_str()) {
                entry.id = last.max(floor);
            }
            state.unsettled.remove(queue.as_str());
            state.hints = true;
        }
    }

    /// Notes a cloud-direct queue the bucket held nothing for, so a later
    /// start without the bucket knows it is not a stranger. Written once.
    pub(crate) async fn record(&self, queue: &QueueId) -> Result<(), Error> {
        if self
            .state
            .lock()
            .unwrap()
            .written
            .contains_key(queue.as_str())
        {
            return Ok(());
        }
        self.advance(queue, &UintN::zero(), Some(&UintN::zero()))?;
        // An earlier write of the record may have failed.
        self.state.lock().unwrap().dirty = true;
        self.flush_if_dirty().await
    }

    /// Records a memory-only queue's last accepted id; the flusher writes it
    /// out within its interval, which is the loss a memory queue accepts. An
    /// earlier cloud life's file stays on the line.
    pub(crate) fn mark(&self, queue: &QueueId, id: &UintN) -> Result<(), Error> {
        let id = to_u64(id, "id")?;
        let mut state = self.state.lock().unwrap();
        let state = &mut *state;
        let floor = state
            .memory_floor
            .entry(queue.as_str().to_string())
            .or_insert(id);
        *floor = (*floor).max(id);
        match state.queues.get_mut(queue.as_str()) {
            // A bare record has an empty bucket behind it; marked, it becomes a
            // memory line, or entry 0 would read as unused.
            Some(entry) if entry.is_bare() => *entry = Pointer { id, file: None },
            Some(entry) if id > entry.id => entry.id = id,
            Some(_) => return Ok(()),
            None => {
                state
                    .queues
                    .insert(queue.as_str().to_string(), Pointer { id, file: None });
            }
        }
        state.dirty = true;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn publishes(&self) -> u64 {
        self.state.lock().unwrap().publishes
    }

    pub(crate) fn advance(
        &self,
        queue: &QueueId,
        id: &UintN,
        file: Option<&UintN>,
    ) -> Result<(), Error> {
        let id = to_u64(id, "id")?;
        let file = file.map(|f| to_u64(f, "file id")).transpose()?;

        let mut state = self.state.lock().unwrap();
        let state = &mut *state;
        let Some(entry) = state.queues.get_mut(queue.as_str()) else {
            state
                .queues
                .insert(queue.as_str().to_string(), Pointer { id, file });
            state.dirty = true;
            return Ok(());
        };
        if id > entry.id {
            entry.id = id;
            state.dirty = true;
        }
        if file.is_some_and(|file| entry.file.is_none_or(|f| file > f)) {
            entry.file = file;
            state.hints = true;
        }
        Ok(())
    }

    /// Writes out the landed files too; for a close.
    pub(crate) async fn flush_all(&self) -> Result<(), Error> {
        {
            let mut state = self.state.lock().unwrap();
            if state.hints {
                state.dirty = true;
            }
        }
        self.flush_if_dirty().await
    }

    pub(crate) async fn flush_if_dirty(&self) -> Result<(), Error> {
        let flush_guard = self.flush_lock.clone().lock_owned().await;
        let snapshot = {
            let mut state = self.state.lock().unwrap();
            if !state.dirty {
                return Ok(());
            }
            state.dirty = false;
            state.hints = false;
            #[cfg(test)]
            {
                state.publishes += 1;
            }
            (state.queues.clone(), state.unsettled.clone())
        };

        let (fs, tmp, path, state) = (
            self.fs.clone(),
            self.tmp_path.clone(),
            self.path.clone(),
            self.state.clone(),
        );
        // The task owns serialization until the executor has finished, even
        // when the caller abandons a close or a cloud landing.
        tokio::spawn(async move {
            let _guard = flush_guard;
            let (snapshot, unsettled) = snapshot;
            let lines = snapshot
                .iter()
                .map(|(queue, p)| (queue, p.id, p.file, !unsettled.contains(queue.as_str())));
            let result = Self::publish(&fs, tmp, path, lines).await;
            let mut state = state.lock().unwrap();
            match result {
                Ok(()) => {
                    state.written_exact = snapshot
                        .iter()
                        .filter(|(queue, p)| p.file.is_some() && !unsettled.contains(*queue))
                        .map(|(queue, _)| queue.clone())
                        .collect();
                    state.written = snapshot;
                }
                Err(_) => state.dirty = true,
            }
            result
        })
        .await
        .map_err(Error::other)?
    }

    pub(crate) fn spawn_flusher(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        let pointers = self.clone();
        let interval = interval.max(Duration::from_millis(1));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = pointers.flush_if_dirty().await {
                    log::warn!(target: "normfs", "Failed to write memory pointers: {e}");
                }
            }
        })
    }

    /// One PUBLISH plan: the fixed temporary name, truncated, then the rename
    /// and the directory sync, so a restart reads either the previous
    /// snapshot or this one whole. A line is `queue, id[, file[, exact]]`.
    async fn publish<'a>(
        fs: &Fs,
        tmp: PathBuf,
        path: PathBuf,
        lines: impl Iterator<Item = (&'a String, u64, Option<u64>, bool)>,
    ) -> Result<(), Error> {
        let mut lines: Vec<_> = lines.collect();
        lines.sort_by_key(|(queue, ..)| *queue);

        let mut out = Vec::new();
        out.extend_from_slice(HEADER.as_bytes());
        for (queue, id, file, exact) in lines {
            out.extend_from_slice(queue.as_bytes());
            out.push(b'\t');
            out.extend_from_slice(id.to_string().as_bytes());
            if let Some(f) = file {
                out.push(b'\t');
                out.extend_from_slice(f.to_string().as_bytes());
                if exact {
                    out.push(b'\t');
                    out.extend_from_slice(EXACT.as_bytes());
                }
            }
            out.push(b'\n');
        }

        fs.publish(
            PublishSpec {
                tmp,
                dst: path,
                runs: Runs(vec![Bytes::from(out)]),
                tmp_mode: TmpMode::Trunc,
                sync: true,
            },
            None,
        )
        .await?;
        Ok(())
    }
}

impl PointerState {
    /// Whether the file on disk keeps a restart from handing out `id` again:
    /// a reserve at or past it, with the last file not claimed exact, since
    /// an upload is about to go past it.
    fn reserved_on_disk(&self, queue: &str, id: u64) -> bool {
        self.written
            .get(queue)
            .is_some_and(|p| p.file.is_some() && !p.is_bare() && p.id >= id)
            && !self.written_exact.contains(queue)
    }
}

impl normfs_store::LandedIndex for MemoryPointers {
    fn mark_landed<'a>(
        &'a self,
        queue: &'a QueueId,
        last_entry_id: &'a UintN,
        file_id: &'a UintN,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(MemoryPointers::mark_landed(
            self,
            queue,
            last_entry_id,
            file_id,
        ))
    }

    fn reserve<'a>(
        &'a self,
        queue: &'a QueueId,
        last_entry_id: &'a UintN,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(MemoryPointers::reserve(self, queue, last_entry_id))
    }
}

async fn read_optional(fs: &Fs, path: &Path) -> Result<Option<String>, Error> {
    let path = path.to_path_buf();
    fs.run_blocking(move || match std::fs::read_to_string(&path) {
        Ok(contents) => Ok(Some(contents)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    })
    .await
    .map_err(Error::from)
}

fn to_u64(n: &UintN, what: &str) -> Result<u64, Error> {
    n.to_u64().map_err(|e| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("memory pointers support u64 {what}s only: {e}"),
        )
    })
}

/// `queue\tid`, `queue\tid\tfile` or `queue\tid\tfile\texact`; the third
/// column is the cloud-direct queue's last file (0 before the first), the
/// fourth says it is exact, and a v1 file without them still parses.
fn parse_pointers(contents: &str) -> Result<(HashMap<String, Pointer>, HashSet<String>), Error> {
    let mut queues = HashMap::new();
    let mut exact = HashSet::new();
    for (line_no, line) in contents.lines().enumerate() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut cols = line.split('\t');
        let (Some(queue), Some(id)) = (cols.next(), cols.next()) else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid memory pointer line {}", line_no + 1),
            ));
        };
        let parse = |field: &str, what: &str| {
            field.parse::<u64>().map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid memory pointer {what} on line {}: {e}", line_no + 1),
                )
            })
        };
        let id = parse(id, "id")?;
        let file = cols.next().map(|f| parse(f, "file id")).transpose()?;
        if cols.next() == Some(EXACT) {
            exact.insert(queue.to_string());
        }
        queues.insert(queue.to_string(), Pointer { id, file });
    }
    Ok((queues, exact))
}
