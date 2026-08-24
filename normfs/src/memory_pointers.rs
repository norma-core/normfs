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

struct PointerState {
    queues: HashMap<String, u64>,
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
        let state = self.state.lock().unwrap();
        state.queues.get(queue.as_str()).copied().map(UintN::from)
    }

    pub(crate) fn mark(&self, queue: &QueueId, id: &UintN) -> Result<(), Error> {
        let id = id.to_u64().map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("memory-only pointers support u64 ids only: {e}"),
            )
        })?;

        let mut state = self.state.lock().unwrap();
        let entry = state.queues.entry(queue.as_str().to_string()).or_insert(id);
        if id >= *entry {
            *entry = id;
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

    fn write_snapshot(&self, snapshot: &HashMap<String, u64>) -> Result<(), Error> {
        let mut entries: Vec<_> = snapshot.iter().collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut file = std::fs::File::create(&self.tmp_path)?;
        file.write_all(b"# normfs memory-only pointers v1\n")?;
        for (queue, id) in entries {
            file.write_all(queue.as_bytes())?;
            file.write_all(b"\t")?;
            file.write_all(id.to_string().as_bytes())?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        drop(file);

        std::fs::rename(&self.tmp_path, &self.path)?;
        Ok(())
    }
}

fn parse_pointers(contents: &str) -> Result<HashMap<String, u64>, Error> {
    let mut queues = HashMap::new();
    for (line_no, line) in contents.lines().enumerate() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (queue, id) = line.split_once('\t').ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid memory pointer line {}", line_no + 1),
            )
        })?;
        let id = id.parse::<u64>().map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid memory pointer id on line {}: {e}", line_no + 1),
            )
        })?;
        queues.insert(queue.to_string(), id);
    }
    Ok(queues)
}
