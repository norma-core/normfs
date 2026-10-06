//! What was accepted is on disk in the order it was accepted.
//!
//! V1 stores no entry id. The reader derives one from position — the file
//! header's `num_entries_before` plus the index of the entry within the file —
//! so the byte order in a WAL file *is* the id order. Two records swapped there
//! are not one record out of place: every entry from that point on answers to
//! the wrong id, permanently, and nothing reports an error.
//!
//! These tests therefore assert the sequence twice. Once through the reader,
//! which is what a client sees, and once by decoding the WAL files directly,
//! which is what is actually on disk. A reader that reconstructs the right
//! answer from the wrong bytes would pass the first and fail the second.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use normfs_types::{DataSource, QueueId, QueueIdResolver, ReadEntry};
use tokio::sync::mpsc;
use uintn::UintN;

/// A payload whose first bytes name the record, so a swap is visible in the
/// bytes rather than only in the count.
fn payload(i: usize, size: usize) -> Bytes {
    let mut v = vec![0u8; size];
    let tag = format!("record-{i:06}-");
    let tag = tag.as_bytes();
    let n = tag.len().min(size);
    v[..n].copy_from_slice(&tag[..n]);
    Bytes::from(v)
}

/// Sizes chosen to cross every boundary the write path has: well inside a page,
/// most of the way across one, and larger than the file it has to go in.
///
/// Nothing here exceeds a page: a record that does is refused before it is
/// given an id, and `a_record_larger_than_a_page_is_refused` covers that.
fn size_for(i: usize) -> usize {
    match i % 7 {
        0..=3 => 512,
        4 => 8 * 1024,
        5 => 200 * 1024, // most of a 256 KiB page
        _ => 160 * 1024, // larger than max_file_size below
    }
}

fn wal_dir(base: &std::path::Path, queue: &QueueId) -> std::path::PathBuf {
    base.join(queue.to_string().trim_start_matches('/'))
        .join("wal")
}

/// Every entry in the queue's WAL files, decoded from the files themselves and
/// concatenated in file order.
///
/// This does not consult the memory store, the reader FSM or anything that
/// could paper over the bytes: `read_wal_file_range` decodes the frames and
/// derives each id from the header and the entry's position, exactly as
/// recovery does.
async fn entries_on_disk(dir: &std::path::Path) -> Vec<ReadEntry> {
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    let mut file_ids: Vec<UintN> = std::fs::read_dir(dir)
        .expect("wal directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "wal"))
        .filter_map(|e| {
            // File names are the id in hex, zero-padded to a multiple of three
            // so that lexicographic order matches numeric order.
            e.path()
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| u64::from_str_radix(s, 16).ok())
                .map(UintN::from)
        })
        .collect();
    file_ids.sort();

    let mut out = Vec::new();
    for file_id in file_ids {
        // Where this file's ids start. A V1 file stores no ids, so asking it
        // for a range below its own first entry asks for nothing -- and the
        // first entry is exactly what its header says came before it.
        let header = normfs_wal::read_wal_header(&fs, dir, &file_id)
            .await
            .expect("a wal file has a header");
        let (tx, mut rx) = mpsc::channel(4096);
        let dir = dir.to_path_buf();
        let id = file_id.clone();
        let from = header.num_entries_before.clone();
        let fs = fs.clone();
        let reader = tokio::spawn(async move {
            normfs_wal::read_wal_file_range(
                &fs,
                &dir,
                &id,
                &from,
                &None,
                1,
                &tx,
                DataSource::DiskWal,
            )
            .await
        });
        while let Some(entry) = rx.recv().await {
            out.push(entry);
        }
        let _ = reader.await;
    }
    out
}

fn settings() -> NormFsSettings {
    let mut settings = NormFsSettings::all_active();
    // Pinned rather than inherited. The page size is the record cap and the
    // granularity a file ends at, so a test about size classes and file
    // boundaries has to fix it: the default is chosen for production and moves
    // when the benchmarks say it should.
    settings.mem_page_size = 256 * 1024;
    // Small enough that the queue rotates files several times over the run, so
    // the boundary between files is exercised rather than assumed away -- and
    // below the page size, so a record can be larger than its whole file.
    settings.wal_settings.max_file_size = 128 * 1024;
    settings.wal_settings.write_interval = Duration::from_millis(20);
    settings.max_memory_usage = 8 * 1024 * 1024;
    settings
}

/// Records of every size class, written from several tasks at once, with a
/// reader following along. What comes back must be what went in, in order.
#[tokio::test]
async fn what_was_accepted_is_on_disk_in_the_order_it_was_accepted() {
    const COUNT: usize = 280;
    const WRITERS: usize = 4;

    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let expected = {
        let fs = Arc::new(NormFS::new(path.clone(), settings()).await.unwrap());
        let queue = fs.resolve("ordered");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        // Somebody reading while the writing happens, so the pins a read takes
        // are in play rather than absent.
        let follower = {
            let fs = Arc::clone(&fs);
            let queue = queue.clone();
            tokio::spawn(async move {
                let (tx, mut rx) = mpsc::channel(64);
                let _ = fs
                    .read(&queue, ReadPosition::Absolute(UintN::zero()), 64, 1, tx)
                    .await;
                let mut seen = 0usize;
                while rx.recv().await.is_some() {
                    seen += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                seen
            })
        };

        // Several tasks appending at once. Each holds the id it was given, so
        // the expected payload for every id is known however they interleaved.
        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let fs = Arc::clone(&fs);
            let queue = queue.clone();
            tasks.push(tokio::spawn(async move {
                let mut mine: Vec<(UintN, usize)> = Vec::new();
                let mut i = w;
                while i < COUNT {
                    let id = fs.enqueue(&queue, payload(i, size_for(i))).await.unwrap();
                    mine.push((id, i));
                    i += WRITERS;
                }
                mine
            }));
        }

        let mut expected: BTreeMap<UintN, usize> = BTreeMap::new();
        for task in tasks {
            for (id, i) in task.await.unwrap() {
                assert!(
                    expected.insert(id.clone(), i).is_none(),
                    "id {id} was handed to two records"
                );
            }
        }
        assert_eq!(expected.len(), COUNT, "every record must get an id");
        for (n, id) in expected.keys().enumerate() {
            assert_eq!(*id, UintN::from(n as u64), "ids must be dense from zero");
        }

        follower.abort();
        let instance = fs.get_instance_id().to_string();
        fs.close().await.unwrap();

        // The claim, made about the bytes on disk rather than about what a
        // reader reconstructs from them.
        let resolver = QueueIdResolver::new(&instance);
        let dir = wal_dir(&path, &resolver.resolve("ordered"));
        assert!(
            std::fs::read_dir(&dir).unwrap().count() > 1,
            "the queue should have rotated, or this proves nothing about file boundaries"
        );

        // Whatever the WAL still holds -- the older files have been migrated to
        // the store and removed, which is the normal course of things -- must
        // be a contiguous run of the sequence, in order, carrying the payloads
        // those ids were given.
        let on_disk = entries_on_disk(&dir).await;
        assert!(
            !on_disk.is_empty(),
            "the queue's remaining WAL files decoded to nothing"
        );
        let first = on_disk[0].id.to_u64().unwrap();
        for (offset, entry) in on_disk.iter().enumerate() {
            let id = UintN::from(first + offset as u64);
            assert_eq!(
                entry.id, id,
                "the WAL holds a gap: entry at offset {offset} answers to {}",
                entry.id
            );
            let i = expected[&id];
            assert_eq!(
                entry.data,
                payload(i, size_for(i)),
                "the entry answering to {id} carries record {i}'s id and another record's \
                 payload: the bytes reached the file out of the order they were accepted in"
            );
        }
        expected
    };

    // And through the reader, from a fresh instance, which is what a client
    // sees after a restart.
    let fs = NormFS::new(path, settings()).await.unwrap();
    let queue = fs.resolve("ordered");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let (tx, mut rx) = mpsc::channel(COUNT + 16);
    fs.read(
        &queue,
        ReadPosition::Absolute(UintN::zero()),
        COUNT as u64,
        1,
        tx,
    )
    .await
    .unwrap();

    let mut got = Vec::with_capacity(COUNT);
    while got.len() < COUNT {
        match tokio::time::timeout(Duration::from_secs(20), rx.recv()).await {
            Ok(Some(entry)) => got.push(entry),
            _ => break,
        }
    }
    assert_eq!(
        got.len(),
        COUNT,
        "every record must read back after a restart"
    );
    for (n, entry) in got.iter().enumerate() {
        let id = UintN::from(n as u64);
        assert_eq!(entry.id, id);
        // Which record took which id is what the writers reported; the ids run
        // dense from zero, but four tasks were appending so id n is not
        // record n.
        let i = expected[&id];
        assert_eq!(
            entry.data,
            payload(i, size_for(i)),
            "the entry answering to {id} after a restart carries another record's payload"
        );
    }

    fs.close().await.unwrap();
}

/// A record that fills most of a page keeps its place among small ones.
///
/// Every other end-to-end order test uses records of one size class. A record
/// wide enough to open a page of its own moves the file boundary around it, and
/// V1 numbers entries by position, so one landing out of place is not one
/// record misfiled but every record after it answering to the wrong id.
#[tokio::test]
async fn a_wide_record_keeps_its_place_among_ordinary_ones() {
    const COUNT: usize = 60;

    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().to_path_buf();
    let mut settings = settings();
    // One file, so this is about order within a file rather than between files.
    settings.wal_settings.max_file_size = 512 * 1024 * 1024;

    let instance = {
        let fs = NormFS::new(path.clone(), settings.clone()).await.unwrap();
        let queue = fs.resolve("mixed");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        for i in 0..COUNT {
            // Every third record takes most of a page.
            let size = if i % 3 == 2 { 200 * 1024 } else { 256 };
            let id = fs.enqueue(&queue, payload(i, size)).await.unwrap();
            assert_eq!(id, UintN::from(i as u64));
        }
        let instance = fs.get_instance_id().to_string();
        fs.close().await.unwrap();
        instance
    };

    let resolver = QueueIdResolver::new(&instance);
    let dir = wal_dir(&path, &resolver.resolve("mixed"));
    let on_disk = entries_on_disk(&dir).await;

    assert_eq!(on_disk.len(), COUNT);
    for (n, entry) in on_disk.iter().enumerate() {
        let size = if n % 3 == 2 { 200 * 1024 } else { 256 };
        assert_eq!(entry.id, UintN::from(n as u64));
        assert_eq!(
            entry.data,
            payload(n, size),
            "position {n} on disk holds another record: a wide record overtook, or was \
             overtaken by, the small ones around it"
        );
    }
}

/// A record too large for a page is refused before it takes an id.
///
/// The refusal has to happen here, before the id. Once an id is taken there is
/// no good answer left: the record reaches no file while every id after it
/// keeps counting, and V1 derives entry ids from position, so the result is not
/// one missing record but every later record answering to the wrong id.
#[tokio::test]
async fn a_record_larger_than_a_page_is_refused() {
    let temp = tempfile::TempDir::new().unwrap();
    let settings = settings();
    let page = settings.mem_page_size;
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("bounded");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    for size in [page, page + 1, page * 2] {
        let refused = fs.enqueue(&queue, Bytes::from(vec![0u8; size])).await;
        assert!(
            matches!(refused, Err(normfs::Error::RecordTooLarge(_))),
            "a record of {size} bytes against a {page} byte page: got {refused:?}"
        );
    }

    // The widest record that does fit is accepted, and takes the first id --
    // so none of the refusals above consumed one.
    let cap = normfs_wal::max_record_len(page);
    let id = fs
        .enqueue(&queue, Bytes::from(vec![1u8; cap]))
        .await
        .unwrap();
    assert_eq!(id, UintN::zero(), "a refused record must consume no id");
    fs.close().await.unwrap();
}

/// A record wider than the configured memory total is refused, and consumes
/// no id.
///
/// It is the page cap that refuses it: `MemoryBelowFloor` makes the page
/// bound the narrower of `check_framable`'s two in every constructible
/// instance, so no test can isolate the memory bound. This one pins the
/// user-visible property.
#[tokio::test]
async fn a_record_wider_than_the_memory_bound_is_refused() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut settings = settings();
    settings.max_memory_usage = 4 * 1024 * 1024;
    settings.mem_page_size = 2 * 1024 * 1024;
    let bound = settings.max_memory_usage;
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("bounded");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let too_wide = Bytes::from(vec![0u8; bound + 1]);
    let refused = fs.enqueue(&queue, too_wide).await;
    assert!(
        matches!(refused, Err(normfs::Error::RecordTooLarge(_))),
        "got {refused:?}"
    );

    // The sequence is untouched: the next record gets the first id.
    let id = fs
        .enqueue(&queue, Bytes::from_static(b"fits"))
        .await
        .unwrap();
    assert_eq!(id, UintN::zero());
    fs.close().await.unwrap();
}
