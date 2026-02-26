use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc::Sender;

use bytes::Bytes;
use normfs_types::{DataSource, QueueId, ReadEntry, SubscriberCallback};
use uintn::UintN;

/// Result of a memory read operation
#[derive(Debug)]
pub struct MemReadResult {
    /// Whether the read was fully satisfied by memory
    pub success: bool,
    /// For negative lookups: the resolved start_id (offset from end)
    pub start_id: Option<UintN>,
    /// For follow/subscribe operations: the subscription ID
    pub subscription_id: Option<usize>,
}

impl MemReadResult {
    pub fn fail() -> Self {
        Self {
            success: false,
            start_id: None,
            subscription_id: None,
        }
    }
}

pub struct MemStore {
    queues: RwLock<HashMap<QueueId, Arc<MemQueue>>>,
    max_memory_usage: usize,
}

struct Entry {
    id: UintN,
    data: Bytes,
}

struct Inner {
    entries: Vec<Entry>,
    last_id: Option<UintN>,
    first_id: Option<UintN>,
    last_acked_id: Option<UintN>,
    memory_usage: usize,
}

struct MemQueue {
    inner: RwLock<Inner>,
    max_memory_usage: usize,
    subscribers: Mutex<HashMap<usize, SubscriberCallback>>,
    next_subscriber_id: Mutex<usize>,
}

impl MemQueue {
    pub fn new(last_id: Option<UintN>, max_memory_usage: usize) -> Self {
        MemQueue {
            inner: RwLock::new(Inner {
                entries: Vec::new(),
                last_id,
                first_id: None,
                last_acked_id: None,
                memory_usage: 0,
            }),
            max_memory_usage,
            subscribers: Mutex::new(HashMap::new()),
            next_subscriber_id: Mutex::new(0),
        }
    }

    pub fn enqueue(&self, data: Bytes) -> UintN {
        let mut inner = self.inner.write().unwrap();
        let id = inner
            .last_id
            .as_ref()
            .map_or(UintN::zero(), |id| id.increment());

        if inner.first_id.is_none() {
            inner.first_id = Some(id.clone());
        }
        inner.last_id = Some(id.clone());
        let data_len = data.len();

        let subscribers_data = if self.subscribers.lock().unwrap().is_empty() {
            None
        } else {
            Some(data.clone())
        };

        inner.entries.push(Entry {
            id: id.clone(),
            data,
        });
        inner.memory_usage += data_len;

        log::debug!(target: "normfs-mem", "Enqueued entry - ID: {}, Data size: {} bytes, Memory usage: {} bytes",
            id, data_len, inner.memory_usage);

        self.cleanup_unlocked(&mut inner);

        drop(inner);

        if let Some(data) = subscribers_data {
            self.notify_subscribers(&[(id.clone(), data)]);
        }

        id
    }

    pub fn enqueue_batch(&self, entries: Vec<Bytes>) -> Vec<UintN> {
        if entries.is_empty() {
            return Vec::new();
        }

        let mut inner = self.inner.write().unwrap();
        let mut ids = Vec::with_capacity(entries.len());
        let mut next_id = inner
            .last_id
            .as_ref()
            .map_or(UintN::zero(), |id| id.increment());

        if inner.first_id.is_none() {
            inner.first_id = Some(next_id.clone());
        }

        let has_subscribers = !self.subscribers.lock().unwrap().is_empty();
        let mut entries_with_ids = if has_subscribers {
            Vec::with_capacity(entries.len())
        } else {
            Vec::new()
        };

        for data in entries {
            ids.push(next_id.clone());
            let data_len = data.len();

            if has_subscribers {
                entries_with_ids.push((next_id.clone(), data.clone()));
            }

            inner.entries.push(Entry {
                id: next_id.clone(),
                data,
            });
            inner.memory_usage += data_len;
            next_id = next_id.increment();
        }

        if let Some(last_id) = ids.last() {
            inner.last_id = Some(last_id.clone());
        }

        self.cleanup_unlocked(&mut inner);

        drop(inner);

        if has_subscribers {
            self.notify_subscribers(&entries_with_ids);
        }

        ids
    }

    pub fn get_last_id(&self) -> Option<UintN> {
        self.inner.read().unwrap().last_id.clone()
    }

    pub fn ack(&self, id: &UintN) {
        let mut inner = self.inner.write().unwrap();
        if inner.last_acked_id.as_ref().is_none_or(|last| id > last) {
            log::debug!(target: "normfs-mem", "Acknowledging entry - ID: {}", id);
            inner.last_acked_id = Some(id.clone());
        }

        self.cleanup_unlocked(&mut inner);
    }

    fn cleanup_unlocked(&self, inner: &mut std::sync::RwLockWriteGuard<Inner>) {
        if inner.memory_usage <= self.max_memory_usage {
            return;
        }

        if let Some(last_acked_id) = &inner.last_acked_id {
            let mut to_remove = 0;
            for entry in &inner.entries {
                if entry.id <= *last_acked_id {
                    to_remove += 1;
                } else {
                    break;
                }
            }

            if to_remove > 0 {
                let memory_removed: usize = inner
                    .entries
                    .iter()
                    .take(to_remove)
                    .map(|e| e.data.len())
                    .sum();
                inner.entries.drain(0..to_remove);
                inner.memory_usage -= memory_removed;
                inner.first_id = inner.entries.first().map(|e| e.id.clone());

                log::debug!(target: "normfs-mem", "Cleaned up {} entries, freed {} bytes, remaining memory: {} bytes",
                    to_remove, memory_removed, inner.memory_usage);
            }
        }
    }

    pub async fn read_full(
        &self,
        start_id: UintN,
        end_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Collect entries to send while holding the lock
        let entries_to_send: Vec<(UintN, Bytes)> = {
            let inner = self.inner.read().unwrap();

            // Check queue's last_id first - if start_id is beyond it, we're done
            let mem_last_id = match &inner.last_id {
                Some(id) => id,
                None => {
                    // Queue exists but is empty (no entries ever enqueued)
                    // Return success with 0 entries
                    return MemReadResult {
                        success: true,
                        start_id: None,
                        subscription_id: None,
                    };
                }
            };

            // If requesting entries beyond what exists, complete with no entries
            if start_id > *mem_last_id {
                return MemReadResult {
                    success: true,
                    start_id: None,
                    subscription_id: None,
                };
            }

            // Check if entries are actually loaded in memory
            let mem_start_id = match &inner.first_id {
                Some(id) => id,
                None => return MemReadResult::fail(), // Entries not in memory, read from files
            };

            // If start_id is before what's in memory, need to read from files
            if start_id < *mem_start_id {
                return MemReadResult::fail();
            }

            let mut current_id = start_id.clone();
            let mut results = Vec::new();

            for entry in inner.entries.iter().skip_while(|e| e.id < start_id) {
                if entry.id > end_id {
                    break;
                }

                while current_id < entry.id {
                    current_id = current_id.step_by(step);
                    if current_id > end_id {
                        break;
                    }
                }

                if current_id == entry.id {
                    results.push((entry.id.clone(), entry.data.clone()));
                    current_id = current_id.step_by(step);
                }
            }

            results
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                break;
            }
        }

        MemReadResult {
            success: true,
            start_id: None,
            subscription_id: None,
        }
    }

    pub async fn read_full_negative(
        &self,
        offset: UintN,
        step: usize,
        limit: u64,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Collect entries to send while holding the lock
        let (entries_to_send, start_id) = {
            let inner = self.inner.read().unwrap();

            let last_id = if let Some(id) = &inner.last_id {
                id
            } else {
                // Queue exists but is empty (no entries ever enqueued)
                // Return success with 0 entries and start_id = 0
                return MemReadResult {
                    success: true,
                    start_id: Some(UintN::zero()),
                    subscription_id: None,
                };
            };

            // Calculate start_id = last_id - offset (before first_id check,
            // so we can return it even when memory has no entries yet)
            let start_id = if offset > *last_id {
                UintN::zero()
            } else {
                last_id.sub(&offset).unwrap_or(UintN::zero())
            };

            let mem_start_id = if let Some(id) = &inner.first_id {
                id
            } else {
                // No entries in memory yet (e.g. after recovery), but last_id exists.
                // Return computed start_id so caller can fall back to file lookup.
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            };

            if start_id < *mem_start_id {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }

            let mut current_id = start_id.clone();
            let mut entries = Vec::new();
            let mut count = 0u64;

            for entry in inner.entries.iter().skip_while(|e| e.id < start_id) {
                if limit > 0 && count >= limit {
                    break;
                }

                while current_id < entry.id {
                    current_id = current_id.step_by(step);
                }

                if current_id == entry.id {
                    entries.push((entry.id.clone(), entry.data.clone()));
                    current_id = current_id.step_by(step);
                    count += 1;
                }
            }

            (entries, start_id)
        };

        if entries_to_send.is_empty() {
            return MemReadResult {
                success: false,
                start_id: Some(start_id),
                subscription_id: None,
            };
        }

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }
        }

        MemReadResult {
            success: true,
            start_id: Some(start_id),
            subscription_id: None,
        }
    }

    pub async fn follow_full(
        self: &Arc<Self>,
        from_id: &UintN,
        start_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Read all existing entries from start_id onwards
        let (entries_to_send, last_sent_id) = {
            let inner = self.inner.read().unwrap();

            if let Some(mem_start_id) = &inner.first_id {
                if start_id < *mem_start_id {
                    return MemReadResult::fail();
                }

                let mut current_id = start_id.clone();
                let mut entries = Vec::new();

                for entry in inner.entries.iter().skip_while(|e| e.id < start_id) {
                    while current_id < entry.id {
                        current_id = current_id.step_by(step);
                    }

                    if current_id == entry.id {
                        entries.push((entry.id.clone(), entry.data.clone()));
                        current_id = current_id.step_by(step);
                    }
                }

                let last_id = entries.last().map(|(id, _)| id.clone());
                (entries, last_id)
            } else {
                (Vec::new(), None)
            }
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult::fail();
            }
        }

        let target_chan_clone = target_chan.clone();
        let from_id_clone = from_id.clone();
        let last_sent_id_clone = last_sent_id.clone();

        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscription_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
            for (id, data) in entries {
                if let Some(ref last_id) = last_sent_id_clone {
                    if id <= last_id {
                        continue;
                    }
                }

                if id.in_step(&from_id_clone, step)
                    && target_chan_clone
                        .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                        .is_err()
                {
                    return false;
                }
            }
            true
        });

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscription_id, callback);

        log::debug!(target: "normfs-mem", "Started follow_full with subscription {} from last_sent_id: {:?}",
            subscription_id, last_sent_id);

        MemReadResult {
            success: true,
            start_id: None,
            subscription_id: Some(subscription_id),
        }
    }

    pub async fn follow_full_negative(
        self: &Arc<Self>,
        offset: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Special case: offset 0 means just subscribe from now on, no existing entries
        if offset == UintN::zero() {
            let (last_id, from_id) = {
                let inner = self.inner.read().unwrap();
                let last = inner.last_id.clone();
                let from = last
                    .as_ref()
                    .map(|id| id.add(&UintN::from(1u64)))
                    .unwrap_or(UintN::zero());
                (last, from)
            };

            let target_chan_clone = target_chan.clone();
            let last_id_clone = last_id.clone();
            let from_id_clone = from_id.clone();

            let mut next_id = self.next_subscriber_id.lock().unwrap();
            let subscription_id = *next_id;
            *next_id += 1;
            drop(next_id);

            let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
                for (id, data) in entries {
                    if let Some(ref last) = last_id_clone {
                        if id <= last {
                            continue;
                        }
                    }

                    if id.in_step(&from_id_clone, step)
                        && target_chan_clone
                            .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                            .is_err()
                    {
                        return false;
                    }
                }
                true
            });

            let mut subscribers = self.subscribers.lock().unwrap();
            subscribers.insert(subscription_id, callback);

            log::debug!(target: "normfs-mem", "Started follow_full_negative with offset 0 (subscribe only), subscription {}, from_id: {}",
                subscription_id, from_id);

            return MemReadResult {
                success: true,
                start_id: Some(from_id),
                subscription_id: Some(subscription_id),
            };
        }

        // Calculate start_id from last_id
        let (start_id, last_sent_id, entries_to_send) = {
            let inner = self.inner.read().unwrap();

            let last_id = if let Some(id) = &inner.last_id {
                id
            } else {
                // Queue exists but is empty (no entries ever enqueued)
                // Return success with 0 entries and start_id = 0
                return MemReadResult {
                    success: true,
                    start_id: Some(UintN::zero()),
                    subscription_id: None,
                };
            };

            let start_id = if offset > *last_id {
                UintN::zero()
            } else {
                last_id.sub(&offset).unwrap_or(UintN::zero())
            };

            let mem_start_id = if let Some(id) = &inner.first_id {
                id
            } else {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            };

            if start_id < *mem_start_id {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }

            let mut current_id = start_id.clone();
            let mut entries = Vec::new();

            for entry in inner.entries.iter().skip_while(|e| e.id < start_id) {
                while current_id < entry.id {
                    current_id = current_id.step_by(step);
                }

                if current_id == entry.id {
                    entries.push((entry.id.clone(), entry.data.clone()));
                    current_id = current_id.step_by(step);
                }
            }

            let last_sent = entries.last().map(|(id, _)| id.clone());
            (start_id, last_sent, entries)
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }
        }

        let target_chan_clone = target_chan.clone();
        let start_id_clone = start_id.clone();
        let last_sent_id_clone = last_sent_id.clone();

        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscription_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
            for (id, data) in entries {
                if let Some(ref last_id) = last_sent_id_clone {
                    if id <= last_id {
                        continue;
                    }
                }

                if id.in_step(&start_id_clone, step)
                    && target_chan_clone
                        .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                        .is_err()
                {
                    return false;
                }
            }
            true
        });

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscription_id, callback);

        log::debug!(target: "normfs-mem", "Started follow_full_negative with subscription {} from last_sent_id: {:?}",
            subscription_id, last_sent_id);

        MemReadResult {
            success: true,
            start_id: Some(start_id),
            subscription_id: Some(subscription_id),
        }
    }

    fn notify_subscribers(&self, entries: &[(UintN, Bytes)]) {
        log::debug!(target: "normfs-mem", "Notifying {} subscribers about {} new entries",
            self.subscribers.lock().unwrap().len(),
            entries.len());

        let to_remove = {
            let subscribers = self.subscribers.lock().unwrap();
            let mut to_remove = Vec::new();

            for (id, callback) in subscribers.iter() {
                if !callback(entries) {
                    to_remove.push(*id);
                }
            }

            to_remove
        }; // Lock released here

        // Clean up failed subscriptions without holding lock
        for id in to_remove {
            self.unsubscribe(id);
        }
    }

    pub fn subscribe(&self, callback: SubscriberCallback) -> usize {
        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscriber_id = *next_id;
        *next_id += 1;

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscriber_id, callback);

        log::debug!(target: "normfs-mem", "Added subscription {}", subscriber_id);
        subscriber_id
    }

    pub fn unsubscribe(&self, subscriber_id: usize) {
        let mut subscribers = self.subscribers.lock().unwrap();
        if subscribers.remove(&subscriber_id).is_some() {
            log::debug!(target: "normfs-mem", "Removed subscription {}", subscriber_id);
        }
    }
}

impl MemStore {
    pub fn new(max_memory_usage: usize) -> Self {
        MemStore {
            queues: RwLock::new(HashMap::new()),
            max_memory_usage,
        }
    }

    pub fn start_queue(&self, queue: &QueueId, last_id: Option<UintN>) {
        let mut queues = self.queues.write().unwrap();
        if !queues.contains_key(queue) {
            let queue_max_memory = self.max_memory_usage / queues.len().max(1);
            log::debug!(target: "normfs-mem", "Starting queue '{}' with last_id: {:?}, max memory: {} bytes",
                queue, last_id, queue_max_memory);
            let new_queue = Arc::new(MemQueue::new(last_id, queue_max_memory));
            queues.insert(queue.clone(), new_queue);
        }
    }

    pub fn enqueue(&self, queue: &QueueId, data: Bytes) -> UintN {
        let queues = self.queues.read().unwrap();
        let mem_queue = queues.get(queue).expect("queue not setup");
        let id = mem_queue.enqueue(data);
        log::debug!(target: "normfs-mem", "Enqueued to queue '{}' - Entry ID: {}", queue, id);
        id
    }

    pub fn enqueue_batch(&self, queue: &QueueId, entries: Vec<Bytes>) -> Vec<UintN> {
        let queues = self.queues.read().unwrap();
        let mem_queue = queues.get(queue).expect("queue not setup");
        let ids = mem_queue.enqueue_batch(entries);
        if let (Some(first), Some(last)) = (ids.first(), ids.last()) {
            log::debug!(target: "normfs-mem", "Enqueued batch to queue '{}' - Count: {}, First ID: {}, Last ID: {}",
                queue, ids.len(), first, last);
        }
        ids
    }

    pub fn get_last_id(&self, queue: &QueueId) -> Option<Option<UintN>> {
        let queues = self.queues.read().unwrap();
        queues.get(queue).map(|q| q.get_last_id())
    }

    pub fn ack(&self, queue: &QueueId, id: &UintN) {
        log::debug!(target: "normfs-mem", "Acknowledging entry in queue '{}' - Entry ID: {}", queue, id);
        let queues = self.queues.read().unwrap();
        if let Some(mem_queue) = queues.get(queue) {
            mem_queue.ack(id);
        }
    }

    pub async fn read_full(
        &self,
        queue: &QueueId,
        start_id: UintN,
        end_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Reading from queue '{}' - Start ID: {}, End ID: {}",
            queue, start_id, end_id);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .read_full(start_id, end_id, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Read from queue '{}' completed - Success: {}", queue, result.success);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for read", queue);
            MemReadResult::fail()
        }
    }

    pub async fn follow_full(
        &self,
        queue: &QueueId,
        from_id: &UintN,
        start_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Starting follow_full from queue '{}' - Start ID: {}",
            queue, start_id);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .follow_full(from_id, start_id, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Follow_full from queue '{}' - Success: {}, Subscription ID: {:?}",
                queue, result.success, result.subscription_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for follow_full", queue);
            MemReadResult::fail()
        }
    }

    pub async fn read_full_negative(
        &self,
        queue: &QueueId,
        offset: UintN,
        step: usize,
        limit: u64,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Reading negative from queue '{}' - Offset: {}, Limit: {}",
            queue, offset, limit);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .read_full_negative(offset, step, limit, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Read negative from queue '{}' completed - Success: {}, Start ID: {:?}",
                queue, result.success, result.start_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for read_full_negative", queue);
            MemReadResult::fail()
        }
    }

    pub async fn follow_full_negative(
        &self,
        queue: &QueueId,
        offset: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Starting follow_full_negative from queue '{}' - Offset: {}",
            queue, offset);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .follow_full_negative(offset, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Follow_full_negative from queue '{}' - Success: {}, Subscription ID: {:?}, Start ID: {:?}",
                queue, result.success, result.subscription_id, result.start_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for follow_full_negative", queue);
            MemReadResult::fail()
        }
    }

    pub fn subscribe(&self, queue: &QueueId, callback: SubscriberCallback) -> Option<usize> {
        log::debug!(target: "normfs-mem", "Subscribing to queue '{}'", queue);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let subscriber_id = mem_queue.subscribe(callback);
            Some(subscriber_id)
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for subscription", queue);
            None
        }
    }

    pub fn unsubscribe(&self, queue: &QueueId, subscriber_id: usize) {
        log::debug!(target: "normfs-mem", "Unsubscribing from queue '{}'", queue);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            mem_queue.unsubscribe(subscriber_id);
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for unsubscription", queue);
        }
    }
}

#[cfg(test)]
mod tests;
