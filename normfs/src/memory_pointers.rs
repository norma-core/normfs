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

const LEGACY_HEADER: &str = "# normfs memory-only pointers v1\n";
const HEADER: &str = "# normfs pointers v1: queue, id reserve, last landed file, exact\n";
/// Fourth column of a queue whose last file no upload can have gone past.
/// Parsers before 0.4.2 read three columns and ignore the rest.
const EXACT: &str = "exact";

/// Ids reserved past the one an upload needs, so the file is rewritten once
/// per this many ids rather than once per upload. A restart that cannot list
/// the bucket skips what was left of it.
pub(crate) const RESERVE_AHEAD: u64 = 1 << 16;

/// What survives a restart of a cloud-direct queue: an id no upload has gone
/// past, and the last file known to have landed. The file is written lazily,
/// so it is exact only while the queue is settled; otherwise the bucket is the
/// authority for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pointer {
    pub id: u64,
    pub file: Option<u64>,
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
        let read_path = path.clone();
        let contents = fs
            .run_blocking(move || match std::fs::read_to_string(&read_path) {
                Ok(contents) => Ok(Some(contents)),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            })
            .await
            .map_err(Error::from)?;
        let (queues, exact) = match contents {
            Some(contents) => {
                let (mut queues, exact) = parse_pointers(&contents)?;
                // Before 0.4.2 every memory-only queue kept a line, with no
                // file; nothing else wrote one without a file then.
                if contents.starts_with(LEGACY_HEADER) {
                    queues.retain(|_, p| p.file.is_some());
                }
                (queues, exact)
            }
            None => (HashMap::new(), HashSet::new()),
        };
        // A bare record never had an upload start: the bucket holds nothing
        // of it.
        let unsettled = queues
            .iter()
            .filter(|(queue, p)| !exact.contains(*queue) && !(p.file.is_none() && p.id == 0))
            .map(|(queue, _)| queue.clone())
            .collect();

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
                #[cfg(test)]
                publishes: 0,
            })),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub(crate) fn last_id(&self, queue: &QueueId) -> Option<UintN> {
        self.pointer(queue).map(|p| UintN::from(p.id))
    }

    /// The highest id an earlier life may have used; a bare record says none.
    pub(crate) fn used_id(&self, queue: &QueueId) -> Option<UintN> {
        self.pointer(queue)
            .filter(|p| p.id > 0 || p.file.is_some())
            .map(|p| UintN::from(p.id))
    }

    pub(crate) fn last_landed(&self, queue: &QueueId) -> Option<(UintN, UintN)> {
        let p = self.pointer(queue)?;
        Some((UintN::from(p.id), UintN::from(p.file?)))
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
        let covered = self
            .pointer(queue)
            .is_some_and(|p| UintN::from(p.id) >= *id);
        let newly_unsettled = {
            let mut state = self.state.lock().unwrap();
            let fresh = state.unsettled.insert(queue.as_str().to_string());
            if fresh {
                state.dirty = true;
            }
            fresh
        };
        if covered && !newly_unsettled {
            return Ok(());
        }
        if !covered {
            let ahead = to_u64(id, "id")?.saturating_add(RESERVE_AHEAD);
            self.advance(queue, &UintN::from(ahead), None)?;
        }
        self.flush_if_dirty().await
    }

    /// After a close that landed everything: each queue that uploaded in this
    /// life has its reserve brought down to the last id it landed, and its
    /// last file is exact again.
    pub(crate) fn settle_landed(&self) {
        let mut state = self.state.lock().unwrap();
        let state = &mut *state;
        for (queue, last) in state.landed_now.drain() {
            if let Some(entry) = state.queues.get_mut(&queue) {
                entry.id = last;
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
            if let Some(entry) = state.queues.get_mut(queue.as_str()) {
                entry.id = last;
            }
            state.unsettled.remove(queue.as_str());
            state.hints = true;
        }
    }

    /// Notes a cloud-direct queue the bucket held nothing for, so a later
    /// start without the bucket knows it is not a stranger. Written once.
    pub(crate) async fn record(&self, queue: &QueueId) -> Result<(), Error> {
        if self.pointer(queue).is_some() {
            return Ok(());
        }
        self.advance(queue, &UintN::zero(), None)?;
        self.flush_if_dirty().await
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
            let result = Self::write_snapshot(&fs, tmp, path, &snapshot, &unsettled).await;
            if result.is_err() {
                state.lock().unwrap().dirty = true;
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
                    log::warn!(target: "normfs", "Failed to write the cloud pointers: {e}");
                }
            }
        })
    }

    /// One PUBLISH plan: the fixed temporary name, truncated, then the rename
    /// and the directory sync, so a restart reads either the previous
    /// snapshot or this one whole.
    async fn write_snapshot(
        fs: &Fs,
        tmp: PathBuf,
        path: PathBuf,
        snapshot: &HashMap<String, Pointer>,
        unsettled: &HashSet<String>,
    ) -> Result<(), Error> {
        let mut entries: Vec<_> = snapshot.iter().collect();
        entries.sort_by_key(|(queue, _)| *queue);

        let mut out = Vec::new();
        out.extend_from_slice(HEADER.as_bytes());
        for (queue, pointer) in entries {
            out.extend_from_slice(queue.as_bytes());
            out.push(b'\t');
            out.extend_from_slice(pointer.id.to_string().as_bytes());
            if let Some(f) = pointer.file {
                out.push(b'\t');
                out.extend_from_slice(f.to_string().as_bytes());
                if !unsettled.contains(queue.as_str()) {
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

fn to_u64(n: &UintN, what: &str) -> Result<u64, Error> {
    n.to_u64().map_err(|e| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("memory pointers support u64 {what}s only: {e}"),
        )
    })
}

/// `queue\tid`, `queue\tid\tfile` or `queue\tid\tfile\texact`; the third
/// column is the cloud-direct queue's last file, the fourth says it is exact,
/// and a v1 file without them still parses.
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
