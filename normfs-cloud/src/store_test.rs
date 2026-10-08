use crate::client::S3Client;
use crate::store::S3Store;
use bytes::Bytes;
use normfs_store::{Backend, BackendError, Body};
use normfs_types::QueueIdResolver;
use std::sync::Arc;
use uintn::UintN;

fn store() -> S3Store {
    let client = S3Client::new(
        "http://127.0.0.1:1".parse().unwrap(),
        "bucket".to_string(),
        "us-east-1".to_string(),
        "key".to_string(),
        "secret".to_string(),
    )
    .unwrap();
    S3Store::new(Arc::new(client), "prefix")
}

#[tokio::test]
async fn a_bucket_refuses_to_append() {
    let store = store();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let id = UintN::from(1u64);

    let created = store
        .create(&queue, &id, Bytes::from_static(b"h"), true)
        .await;
    assert!(matches!(created, Err(BackendError::Unsupported(_))));
    let reopened = store.reopen(&queue, &id).await;
    assert!(matches!(reopened, Err(BackendError::Unsupported(_))));
    assert!(matches!(
        store.delete(&queue, &id).await,
        Err(BackendError::Unsupported(_))
    ));
    let io = std::io::Error::from(store.prepare(&queue).await.unwrap_err());
    assert_eq!(io.kind(), std::io::ErrorKind::Unsupported);
}

#[tokio::test]
async fn a_failed_request_does_not_show_the_presigned_query() {
    let store = store();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let id = UintN::from(1u64);

    let put = store
        .put(&queue, &id, Body::Runs(vec![Bytes::from_static(b"data")]))
        .await
        .unwrap_err();
    let size = store.size(&queue, &id).await.unwrap_err();
    for e in [put, size] {
        for shown in [e.to_string(), format!("{e:?}")] {
            assert!(shown.contains("127.0.0.1:1/bucket/prefix"), "{shown}");
            assert!(!shown.contains('?'), "{shown}");
            assert!(!shown.contains("X-Amz-"), "{shown}");
        }
    }
}
