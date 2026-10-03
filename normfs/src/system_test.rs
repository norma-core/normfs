use super::*;
use normfs_types::QueueIdResolver;

fn queue(name: &str) -> QueueId {
    QueueIdResolver::new("inst").resolve(name)
}

fn closed(name: &str) -> SystemEvent {
    SystemEvent::QueueClosed {
        queue: queue(name),
        last_id: Some(UintN::from(7u64)),
    }
}

fn decode(record: Bytes) -> pb::Event {
    pb::Event::decode(record).unwrap()
}

#[tokio::test]
async fn events_about_the_system_queue_itself_are_not_recorded() {
    let (system, mut rx) = SystemQueue::new(queue(SYSTEM_QUEUE));
    system.emit(closed(SYSTEM_QUEUE));
    system.emit(closed("cam"));
    let stamped = rx.try_recv().unwrap();
    assert_eq!(stamped.event.queue(), &queue("cam"));
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn a_full_backlog_is_counted_on_the_next_recorded_event() {
    let (system, mut rx) = SystemQueue::new(queue(SYSTEM_QUEUE));
    for _ in 0..BACKLOG + 3 {
        system.emit(closed("cam"));
    }
    let first = decode(system.encode(rx.try_recv().unwrap()).0);
    assert_eq!(first.dropped_before, 3);
    let second = decode(system.encode(rx.try_recv().unwrap()).0);
    assert_eq!(second.dropped_before, 0);
    assert_eq!(second.started_unix_ns, first.started_unix_ns);
}

#[tokio::test]
async fn an_event_that_is_not_written_hands_its_count_on() {
    let (system, mut rx) = SystemQueue::new(queue(SYSTEM_QUEUE));
    system.count_dropped(2);
    system.emit(closed("cam"));
    system.emit(closed("cam"));
    let (_, dropped_before) = system.encode(rx.try_recv().unwrap());
    system.count_dropped(dropped_before + 1);
    let next = decode(system.encode(rx.try_recv().unwrap()).0);
    assert_eq!(next.dropped_before, 3);
}

#[test]
fn a_landed_file_keeps_its_ids_and_signature() {
    let facts = FileFacts {
        queue: queue("cam"),
        file_id: UintN::from(0x1234u64),
        first_id: UintN::from(1000u64),
        num_entries: UintN::from(50u64),
        file_bytes: 4096,
        raw_bytes: Some(9000),
        compression: CompressionType::Zstd,
        encryption: EncryptionType::Aes,
        content_signature: [0xAB; 64],
    };
    let event = SystemEvent::FileLanded {
        file: facts,
        key: "prefix/inst/cam/000/012/34.store".to_string(),
        took: Duration::from_millis(250),
        landed_through: UintN::from(0x1234u64),
    };
    let pb::event::Kind::FileLanded(landed) = kind(event) else {
        panic!("wrong kind");
    };
    let file = landed.file.unwrap();
    assert_eq!(file.queue, "/inst/cam");
    assert_eq!(file.file_id, Some(id(&UintN::from(0x1234u64))));
    assert_eq!(file.first_id, Some(id(&UintN::from(1000u64))));
    assert_eq!(file.num_entries, 50);
    assert_eq!(file.raw_bytes, 9000);
    assert_eq!(file.compression, pb::Compression::CZstd as i32);
    assert_eq!(file.encryption, pb::Encryption::EAes as i32);
    assert_eq!(file.content_signature.as_ref(), &[0xAB; 64]);
    assert_eq!(landed.duration_ms, 250);
    assert_eq!(landed.landed_through, file.file_id);
}

#[test]
fn an_upload_failure_carries_its_status_and_a_bounded_message() {
    let event = SystemEvent::UploadFailed {
        queue: queue("cam"),
        file_id: UintN::from(3u64),
        failure: UploadFailure::Status(503),
        message: "ж".repeat(MAX_MESSAGE),
    };
    let pb::event::Kind::UploadFailed(failed) = kind(event) else {
        panic!("wrong kind");
    };
    assert_eq!(failed.reason, pb::upload_failed::Reason::RHttpStatus as i32);
    assert_eq!(failed.http_status, 503);
    assert!(failed.message.len() <= MAX_MESSAGE);
}
