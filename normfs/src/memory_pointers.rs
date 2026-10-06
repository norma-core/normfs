use bytes::Bytes;
use normfs_fs::{Fs, PublishSpec, Runs, TmpMode};
use normfs_types::QueueId;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use uintn::UintN;

const POINTERS_FILE: &str = ".memory_pointers";
const POINTERS_TMP_FILE: &str = ".memory_pointers.tmp";

/// What survives a restart for a queue that keeps no local files: the last
/// id, and for a cloud-direct queue the file that id landed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pointer {
    pub id: u64,
    pub file: Option<u64>,
}

struct PointerState {
    queues: HashMap<String, Pointer>,
    dirty: bool,
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
        let queues = match contents {
            Some(contents) => parse_pointers(&contents)?,
            None => HashMap::new(),
        };

        Ok(Self {
            fs,
            path,
            tmp_path,
            state: Arc::new(Mutex::new(PointerState {
                queues,
                dirty: false,
            })),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub(crate) fn last_id(&self, queue: &QueueId) -> Option<UintN> {
        self.pointer(queue).map(|p| UintN::from(p.id))
    }

    pub(crate) fn last_landed(&self, queue: &QueueId) -> Option<(UintN, UintN)> {
        let p = self.pointer(queue)?;
        Some((UintN::from(p.id), UintN::from(p.file?)))
    }

    fn pointer(&self, queue: &QueueId) -> Option<Pointer> {
        let state = self.state.lock().unwrap();
        state.queues.get(queue.as_str()).copied()
    }

    /// Records the last accepted id; the flusher writes it out within its
    /// interval, which is the loss a memory queue accepts.
    pub(crate) fn mark(&self, queue: &QueueId, id: &UintN) -> Result<(), Error> {
        self.advance(queue, id, None)
    }

    /// Records that `file_id` holding ids up to `last_id` is in the cloud, and
    /// writes it out before returning: the next life starts from this, and a
    /// stale one would overwrite that object.
    pub(crate) async fn mark_landed(
        &self,
        queue: &QueueId,
        last_id: &UintN,
        file_id: &UintN,
    ) -> Result<(), Error> {
        self.advance(queue, last_id, Some(file_id))?;
        self.flush_if_dirty().await
    }

    fn advance(&self, queue: &QueueId, id: &UintN, file: Option<&UintN>) -> Result<(), Error> {
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
        // Independent: a memory life can leave the id ahead of the first file a
        // cloud-direct life lands, and readers bound their walk by the file.
        if id > entry.id {
            entry.id = id;
            state.dirty = true;
        }
        if file.is_some_and(|file| entry.file.is_none_or(|f| file > f)) {
            entry.file = file;
            state.dirty = true;
        }
        Ok(())
    }

    pub(crate) async fn flush_if_dirty(&self) -> Result<(), Error> {
        let flush_guard = self.flush_lock.clone().lock_owned().await;
        let snapshot = {
            let mut state = self.state.lock().unwrap();
            if !state.dirty {
                return Ok(());
            }
            state.dirty = false;
            state.queues.clone()
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
            let result = Self::write_snapshot(&fs, tmp, path, &snapshot).await;
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
                    log::warn!(target: "normfs", "Failed to flush memory-only pointers: {e}");
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
    ) -> Result<(), Error> {
        let mut entries: Vec<_> = snapshot.iter().collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut out = Vec::new();
        out.extend_from_slice(b"# normfs memory-only pointers v1\n");
        for (queue, pointer) in entries {
            out.extend_from_slice(queue.as_bytes());
            out.push(b'\t');
            out.extend_from_slice(pointer.id.to_string().as_bytes());
            if let Some(f) = pointer.file {
                out.push(b'\t');
                out.extend_from_slice(f.to_string().as_bytes());
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

impl normfs_cloud::LandedIndex for MemoryPointers {
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
}

fn to_u64(n: &UintN, what: &str) -> Result<u64, Error> {
    n.to_u64().map_err(|e| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("memory pointers support u64 {what}s only: {e}"),
        )
    })
}

/// `queue\tid` or `queue\tid\tfile`; the third column is the cloud-direct
/// queue's last file and a v1 file without it still parses.
fn parse_pointers(contents: &str) -> Result<HashMap<String, Pointer>, Error> {
    let mut queues = HashMap::new();
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
        queues.insert(queue.to_string(), Pointer { id, file });
    }
    Ok(queues)
}
