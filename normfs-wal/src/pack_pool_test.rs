use super::*;
use std::time::Duration;

#[test]
fn a_slot_is_held_by_one_file_at_a_time() {
    let pool = PackPool::new(2, 64);
    let a = pool.try_take().unwrap();
    let b = pool.try_take().unwrap();
    assert_ne!(a.index(), b.index());
    assert!(pool.try_take().is_none());
    drop(a);
    assert!(pool.try_take().is_some());
}

#[test]
fn frozen_bytes_hold_the_slot_until_the_last_slice_goes() {
    let pool = PackPool::new(1, 64);
    let mut slot = pool.try_take().unwrap();
    slot.buf()[..3].copy_from_slice(b"abc");
    let whole = slot.freeze();
    let head = whole.slice(..3);
    drop(whole);
    assert!(pool.try_take().is_none());
    assert_eq!(&head[..], b"abc");
    drop(head);
    assert!(pool.try_take().is_some());
}

#[tokio::test]
async fn a_waiting_file_gets_the_slot_given_back() {
    let pool = PackPool::new(1, 64);
    let held = pool.try_take().unwrap();
    let waiter = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { pool.take().await.index() }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());
    let index = held.index();
    drop(held);
    assert_eq!(waiter.await.unwrap(), index);
}
