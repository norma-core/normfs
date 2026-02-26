use bytes::Bytes;
use normfs_types::QueueId;
use uintn::UintN;

pub struct OrderedBuffer {
    pub pending: Vec<(UintN, Bytes)>,
    pub last_written_id: Option<UintN>,
    pub queue_id: QueueId,
}

impl OrderedBuffer {
    pub fn new(last_id: Option<UintN>, queue_id: QueueId) -> Self {
        Self {
            pending: Vec::new(),
            last_written_id: last_id,
            queue_id,
        }
    }

    fn check_can_write(&self, id: &UintN) -> bool {
        match &self.last_written_id {
            Some(last_id) => *id == last_id.increment(),
            None => id.is_zero(),
        }
    }

    pub fn can_write(&mut self, id: &UintN) -> bool {
        if self.check_can_write(id) {
            self.last_written_id = Some(id.clone());
            true
        } else {
            false
        }
    }

    pub fn wait_for_order(&mut self, (id, data): (UintN, Bytes)) -> Vec<(UintN, Bytes)> {
        // Log what we're waiting for
        let expected_id = match &self.last_written_id {
            Some(last_id) => last_id.increment(),
            None => UintN::from(0u64),
        };

        log::info!(
            target: "normfs",
            "WAL writer buffer [{}]: waiting for order, buffer size: {}, waiting for: {}, received: {}",
            self.queue_id,
            self.pending.len(),
            expected_id,
            id
        );

        self.pending.push((id, data));
        self.pending
            .sort_unstable_by(|el1, el2| -> std::cmp::Ordering { el1.0.cmp(&el2.0) });

        let mut ready = Vec::new();
        let mut cropped = 0;
        let mut last_id = self.last_written_id.clone();

        for (idx, (id, data)) in self.pending.iter().enumerate() {
            match last_id {
                Some(value) if *id == value.increment() => {
                    ready.push((id.clone(), data.clone()));
                    last_id = Some(id.clone());
                    cropped = idx + 1;
                }
                None if id.is_zero() => {
                    ready.push((id.clone(), data.clone()));
                    last_id = Some(id.clone());
                    cropped = idx + 1;
                }
                _ => break,
            }
        }

        if cropped > 0 {
            self.pending.drain(0..cropped);
            self.last_written_id = last_id;
        }

        // Log when wait is resolved
        if !ready.is_empty() {
            log::info!(
                target: "normfs",
                "WAL writer buffer [{}]: wait resolved, released {} entries from buffer, remaining buffer size: {}",
                self.queue_id,
                ready.len(),
                self.pending.len()
            );
        }

        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uintn::UintN;

    #[test]
    fn test_new_with_last_id() {
        let last_id = UintN::from(42u64);
        let buffer = OrderedBuffer::new(
            Some(last_id.clone()),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        assert_eq!(buffer.last_written_id, Some(last_id));
        assert!(buffer.pending.is_empty());
    }

    #[test]
    fn test_check_can_write_with_last_id() {
        let last_id = UintN::from(10u64);
        let buffer = OrderedBuffer::new(
            Some(last_id.clone()),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        assert!(buffer.check_can_write(&UintN::from(11u64)));
        assert!(!buffer.check_can_write(&UintN::from(10u64)));
        assert!(!buffer.check_can_write(&UintN::from(12u64)));
        assert!(!buffer.check_can_write(&UintN::from(0u64)));
    }

    #[test]
    fn test_check_can_write_without_last_id() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );
        buffer.last_written_id = None;

        assert!(buffer.check_can_write(&UintN::from(0u64)));
        assert!(!buffer.check_can_write(&UintN::from(1u64)));
        assert!(!buffer.check_can_write(&UintN::from(10u64)));
    }

    #[test]
    fn test_can_write_increments_id() {
        let last_id = UintN::from(10u64);
        let mut buffer = OrderedBuffer::new(
            Some(last_id.clone()),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        let next_id = UintN::from(11u64);
        assert!(buffer.can_write(&next_id));
        assert_eq!(buffer.last_written_id, Some(next_id.clone()));

        assert!(buffer.can_write(&UintN::from(12u64)));
        assert_eq!(buffer.last_written_id, Some(UintN::from(12u64)));
    }

    #[test]
    fn test_can_write_does_not_increment_on_failure() {
        let last_id = UintN::from(10u64);
        let mut buffer = OrderedBuffer::new(
            Some(last_id.clone()),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        assert!(!buffer.can_write(&UintN::from(10u64)));
        assert_eq!(buffer.last_written_id, Some(last_id.clone()));

        assert!(!buffer.can_write(&UintN::from(12u64)));
        assert_eq!(buffer.last_written_id, Some(last_id));
    }

    #[test]
    fn test_can_write_from_none() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );
        buffer.last_written_id = None;

        assert!(buffer.can_write(&UintN::from(0u64)));
        assert_eq!(buffer.last_written_id, Some(UintN::from(0u64)));

        assert!(buffer.can_write(&UintN::from(1u64)));
        assert_eq!(buffer.last_written_id, Some(UintN::from(1u64)));
    }

    #[test]
    fn test_wait_for_order_single_in_order() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(5u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );
        let data = Bytes::from("test");

        let ready = buffer.wait_for_order((UintN::from(6u64), data.clone()));

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, UintN::from(6u64));
        assert_eq!(ready[0].1, data);
        assert_eq!(buffer.last_written_id, Some(UintN::from(6u64)));
        assert!(buffer.pending.is_empty());
    }

    #[test]
    fn test_wait_for_order_single_out_of_order() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(5u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );
        let data = Bytes::from("test");

        let ready = buffer.wait_for_order((UintN::from(8u64), data.clone()));

        assert!(ready.is_empty());
        assert_eq!(buffer.pending.len(), 1);
        assert_eq!(buffer.pending[0].0, UintN::from(8u64));
        assert_eq!(buffer.last_written_id, Some(UintN::from(5u64)));
    }

    #[test]
    fn test_wait_for_order_multiple_sequential() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        buffer.wait_for_order((UintN::from(3u64), Bytes::from("3")));
        buffer.wait_for_order((UintN::from(2u64), Bytes::from("2")));
        let ready = buffer.wait_for_order((UintN::from(1u64), Bytes::from("1")));

        assert_eq!(ready.len(), 3);
        assert_eq!(ready[0].0, UintN::from(1u64));
        assert_eq!(ready[0].1, Bytes::from("1"));
        assert_eq!(ready[1].0, UintN::from(2u64));
        assert_eq!(ready[1].1, Bytes::from("2"));
        assert_eq!(ready[2].0, UintN::from(3u64));
        assert_eq!(ready[2].1, Bytes::from("3"));
        assert_eq!(buffer.last_written_id, Some(UintN::from(3u64)));
        assert!(buffer.pending.is_empty());
    }

    #[test]
    fn test_wait_for_order_with_gaps() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        let ready1 = buffer.wait_for_order((UintN::from(1u64), Bytes::from("1")));
        assert_eq!(ready1.len(), 1);
        assert_eq!(ready1[0].0, UintN::from(1u64));

        buffer.wait_for_order((UintN::from(3u64), Bytes::from("3")));
        buffer.wait_for_order((UintN::from(5u64), Bytes::from("5")));
        let ready2 = buffer.wait_for_order((UintN::from(2u64), Bytes::from("2")));

        assert_eq!(ready2.len(), 2);
        assert_eq!(ready2[0].0, UintN::from(2u64));
        assert_eq!(ready2[1].0, UintN::from(3u64));
        assert_eq!(buffer.last_written_id, Some(UintN::from(3u64)));
        assert_eq!(buffer.pending.len(), 1);
        assert_eq!(buffer.pending[0].0, UintN::from(5u64));
    }

    #[test]
    fn test_wait_for_order_starting_from_zero() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );
        buffer.last_written_id = None;

        buffer.wait_for_order((UintN::from(2u64), Bytes::from("2")));
        buffer.wait_for_order((UintN::from(1u64), Bytes::from("1")));
        let ready = buffer.wait_for_order((UintN::from(0u64), Bytes::from("0")));

        assert_eq!(ready.len(), 3);
        assert_eq!(ready[0].0, UintN::from(0u64));
        assert_eq!(ready[1].0, UintN::from(1u64));
        assert_eq!(ready[2].0, UintN::from(2u64));
        assert_eq!(buffer.last_written_id, Some(UintN::from(2u64)));
        assert!(buffer.pending.is_empty());
    }

    #[test]
    fn test_wait_for_order_duplicate_handling() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        let ready1 = buffer.wait_for_order((UintN::from(1u64), Bytes::from("first")));
        assert_eq!(ready1.len(), 1);
        assert_eq!(ready1[0].1, Bytes::from("first"));

        // Duplicate ID should not be processed since last_written_id is already 1
        let ready2 = buffer.wait_for_order((UintN::from(1u64), Bytes::from("duplicate")));
        assert_eq!(ready2.len(), 0);
        assert_eq!(buffer.last_written_id, Some(UintN::from(1u64)));
        assert_eq!(buffer.pending.len(), 1);
        assert_eq!(buffer.pending[0].1, Bytes::from("duplicate"));
    }

    #[test]
    fn test_wait_for_order_large_batch() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        // Add items out of order, starting from 2
        for i in (2..=10).rev() {
            buffer.wait_for_order((UintN::from(i as u64), Bytes::from(format!("{}", i))));
        }

        assert_eq!(buffer.pending.len(), 9);
        assert_eq!(buffer.last_written_id, Some(UintN::from(0u64)));

        // Adding item 1 should trigger processing of 1-10
        let ready = buffer.wait_for_order((UintN::from(1u64), Bytes::from("1")));
        assert_eq!(ready.len(), 10);

        for i in 1..=10 {
            assert_eq!(ready[i - 1].0, UintN::from(i as u64));
            assert_eq!(ready[i - 1].1, Bytes::from(format!("{}", i)));
        }

        assert_eq!(buffer.last_written_id, Some(UintN::from(10u64)));
        assert!(buffer.pending.is_empty());
    }

    #[test]
    fn test_sorting_stability() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        buffer.wait_for_order((UintN::from(5u64), Bytes::from("5")));
        buffer.wait_for_order((UintN::from(3u64), Bytes::from("3")));
        buffer.wait_for_order((UintN::from(7u64), Bytes::from("7")));
        buffer.wait_for_order((UintN::from(2u64), Bytes::from("2")));

        assert_eq!(buffer.pending[0].0, UintN::from(2u64));
        assert_eq!(buffer.pending[1].0, UintN::from(3u64));
        assert_eq!(buffer.pending[2].0, UintN::from(5u64));
        assert_eq!(buffer.pending[3].0, UintN::from(7u64));
    }

    #[test]
    fn test_empty_data() {
        let mut buffer = OrderedBuffer::new(
            Some(UintN::from(0u64)),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );
        let empty_data = Bytes::new();

        let ready = buffer.wait_for_order((UintN::from(1u64), empty_data.clone()));

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, UintN::from(1u64));
        assert_eq!(ready[0].1, empty_data);
        assert!(buffer.pending.is_empty());
    }

    #[test]
    fn test_large_id_values() {
        let large_id = UintN::from(u64::MAX - 1);
        let mut buffer = OrderedBuffer::new(
            Some(large_id.clone()),
            normfs_types::QueueIdResolver::new("test_instance").resolve("test_queue"),
        );

        let next_id = large_id.increment();
        let ready = buffer.wait_for_order((next_id.clone(), Bytes::from("max")));

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, next_id);
        assert_eq!(buffer.last_written_id, Some(next_id));
    }
}
