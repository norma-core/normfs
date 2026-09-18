use super::BoundedMap;
use std::sync::Arc;

#[test]
fn removed_keys_are_released_under_churn() {
    let mut map = BoundedMap::new(4);
    for i in 0..10_000 {
        let key = Arc::new(i);
        map.insert(key.clone(), i);
        assert_eq!(map.remove(&key), Some(i));
        assert_eq!(Arc::strong_count(&key), 1);
    }
    assert!(map.is_empty());
}

#[test]
fn a_removed_then_reinserted_key_is_newest() {
    let mut map = BoundedMap::new(2);
    map.insert("a", 1);
    map.insert("b", 2);
    map.remove(&"a");
    map.insert("a", 3);
    map.insert("c", 4);
    assert_eq!(map.get(&"a"), Some(&3));
    assert_eq!(map.get(&"b"), None);
    assert_eq!(map.get(&"c"), Some(&4));
}

#[test]
fn the_oldest_entry_goes_when_the_cap_is_reached() {
    let mut map = BoundedMap::new(2);
    map.insert("a", 1);
    map.insert("b", 2);
    map.insert("c", 3);
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&"a"), None);
    assert_eq!(map.get(&"b"), Some(&2));
    assert_eq!(map.get(&"c"), Some(&3));
}

#[test]
fn reinserting_a_key_does_not_count_twice() {
    let mut map = BoundedMap::new(2);
    map.insert("a", 1);
    map.insert("a", 2);
    map.insert("b", 3);
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&"a"), Some(&2));
}

#[test]
fn a_removed_key_does_not_evict_a_live_one_later() {
    let mut map = BoundedMap::new(2);
    map.insert("a", 1);
    map.remove(&"a");
    map.insert("b", 2);
    map.insert("c", 3);
    assert_eq!(map.get(&"b"), Some(&2));
    assert_eq!(map.get(&"c"), Some(&3));
}
