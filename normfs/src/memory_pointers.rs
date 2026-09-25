use normfs_types::QueueId;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Write};
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
    path: PathBuf,
    tmp_path: PathBuf,
    state: Mutex<PointerState>,
    flush_lock: Mutex<()>,
}

impl MemoryPointers {
    pub(crate) fn open(root: &Path) -> Result<Self, Error> {
        let path = root.join(POINTERS_FILE);
        let tmp_path = root.join(POINTERS_TMP_FILE);
        let queues = match std::fs::read_to_string(&path) {
            Ok(contents) => parse_pointers(&contents)?,
            Err(e) if e.kind() == ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e),
        };

        Ok(Self {
            path,
            tmp_path,
            state: Mutex::new(PointerState {
                queues,
                dirty: false,
            }),
            flush_lock: Mutex::new(()),
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
    pub(crate) fn mark_landed(
        &self,
        queue: &QueueId,
        last_id: &UintN,
        file_id: &UintN,
    ) -> Result<(), Error> {
        self.advance(queue, last_id, Some(file_id))?;
        self.flush_if_dirty()
    }

    fn advance(&self, queue: &QueueId, id: &UintN, file: Option<&UintN>) -> Result<(), Error> {
        let id = to_u64(id, "id")?;
        let file = file.map(|f| to_u64(f, "file id")).transpose()?;

        let mut state = self.state.lock().unwrap();
        let entry = state
            .queues
            .entry(queue.as_str().to_string())
            .or_insert(Pointer { id, file });
        if id >= entry.id {
            entry.id = id;
            entry.file = file.or(entry.file);
            state.dirty = true;
        }
        Ok(())
    }

    pub(crate) fn flush_if_dirty(&self) -> Result<(), Error> {
        let _flush_guard = self.flush_lock.lock().unwrap();
        let snapshot = {
            let mut state = self.state.lock().unwrap();
            if !state.dirty {
                return Ok(());
            }
            state.dirty = false;
            state.queues.clone()
        };

        if let Err(e) = self.write_snapshot(&snapshot) {
            self.state.lock().unwrap().dirty = true;
            return Err(e);
        }

        Ok(())
    }

    pub(crate) fn spawn_flusher(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        let pointers = self.clone();
        let interval = interval.max(Duration::from_millis(1));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = pointers.flush_if_dirty() {
                    log::warn!(target: "normfs", "Failed to flush memory-only pointers: {e}");
                }
            }
        })
    }

    fn write_snapshot(&self, snapshot: &HashMap<String, Pointer>) -> Result<(), Error> {
        let mut entries: Vec<_> = snapshot.iter().collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut file = std::fs::File::create(&self.tmp_path)?;
        file.write_all(b"# normfs memory-only pointers v1\n")?;
        for (queue, pointer) in entries {
            file.write_all(queue.as_bytes())?;
            file.write_all(b"\t")?;
            file.write_all(pointer.id.to_string().as_bytes())?;
            if let Some(f) = pointer.file {
                file.write_all(b"\t")?;
                file.write_all(f.to_string().as_bytes())?;
            }
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        drop(file);

        std::fs::rename(&self.tmp_path, &self.path)?;
        std::fs::File::open(self.path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
        Ok(())
    }
}

impl normfs_cloud::LandedIndex for MemoryPointers {
    fn mark_landed(
        &self,
        queue: &QueueId,
        last_entry_id: &UintN,
        file_id: &UintN,
    ) -> Result<(), Error> {
        MemoryPointers::mark_landed(self, queue, last_entry_id, file_id)
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
