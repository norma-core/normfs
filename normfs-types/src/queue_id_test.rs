use super::*;

#[test]
fn test_queue_id_resolver() {
    let instance_id = "abc123def456";
    let resolver = QueueIdResolver::new(instance_id);

    let queue_id = resolver.resolve("my_queue");
    assert_eq!(queue_id.as_str(), "/abc123def456/my_queue");
    assert_eq!(format!("{:?}", queue_id), "/abc123def456/my_queue");

    let prefix = std::path::Path::new("/data");
    assert_eq!(
        queue_id.to_fs_path(prefix),
        std::path::PathBuf::from("/data/abc123def456/my_queue")
    );
}

#[test]
fn test_queue_id_resolver_absolute_path() {
    let instance_id = "abc123def456";
    let resolver = QueueIdResolver::new(instance_id);

    let queue_id = resolver.resolve("/shared/queue");
    assert_eq!(queue_id.as_str(), "/shared/queue");
    assert_eq!(format!("{:?}", queue_id), "/shared/queue");

    let prefix = std::path::Path::new("/data");
    assert_eq!(
        queue_id.to_fs_path(prefix),
        std::path::PathBuf::from("/data/shared/queue")
    );
}

#[test]
fn test_queue_id_resolver_nested_path() {
    let instance_id = "xyz789";
    let resolver = QueueIdResolver::new(instance_id);

    let queue_id = resolver.resolve("path/to/queue");
    assert_eq!(queue_id.as_str(), "/xyz789/path/to/queue");
    assert_eq!(format!("{:?}", queue_id), "/xyz789/path/to/queue");

    let prefix = std::path::Path::new("/data");
    assert_eq!(
        queue_id.to_fs_path(prefix),
        std::path::PathBuf::from("/data/xyz789/path/to/queue")
    );
}

#[test]
fn test_queue_id_as_ref() {
    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("test");
    let s: &str = queue_id.as_str();
    assert_eq!(s, "/test_instance/test");
}

#[test]
fn test_queue_id_resolver_instance_id() {
    let instance_id = "abc123def456";
    let resolver = QueueIdResolver::new(instance_id);

    assert_eq!(resolver.instance_id(), instance_id);
}

#[test]
fn test_queue_id_as_hashmap_key() {
    use std::collections::HashMap;

    let resolver = QueueIdResolver::new("instance1");
    let queue1 = resolver.resolve("queue_a");
    let queue2 = resolver.resolve("queue_b");
    let queue3 = resolver.resolve("queue_a");

    let mut map = HashMap::new();
    map.insert(queue1.clone(), "value1");
    map.insert(queue2, "value2");

    assert_eq!(map.get(&queue1), Some(&"value1"));
    assert_eq!(map.get(&queue3), Some(&"value1"));
    assert_eq!(map.len(), 2);
}

#[test]
fn test_queue_id_ordering() {
    let resolver = QueueIdResolver::new("instance1");
    let queue_a = resolver.resolve("a");
    let queue_b = resolver.resolve("b");
    let queue_z = resolver.resolve("z");

    assert!(queue_a < queue_b);
    assert!(queue_b < queue_z);
    assert!(queue_a < queue_z);
    assert!(queue_z > queue_a);

    let mut queues = vec![queue_z.clone(), queue_a.clone(), queue_b.clone()];
    queues.sort();

    assert_eq!(queues, vec![queue_a, queue_b, queue_z]);
}
