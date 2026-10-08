use crate::layer::Layer;
use crate::offloader_test::Memory;
use crate::sink::{AfterLanding, LandedIndex, LayerSink, SealedFileSink};
use crate::store_file::SealedFile;
use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver, events};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use uintn::UintN;

#[derive(Default)]
struct Index {
    calls: Mutex<Vec<String>>,
}

impl LandedIndex for Index {
    fn mark_landed<'a>(
        &'a self,
        _queue: &'a QueueId,
        last_entry_id: &'a UintN,
        file_id: &'a UintN,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
        let call = format!("landed {last_entry_id} in {file_id}");
        self.calls.lock().unwrap().push(call);
        Box::pin(std::future::ready(Ok(())))
    }

    fn reserve<'a>(
        &'a self,
        _queue: &'a QueueId,
        last_entry_id: &'a UintN,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
        let call = format!("reserved {last_entry_id}");
        self.calls.lock().unwrap().push(call);
        Box::pin(std::future::ready(Ok(())))
    }
}

#[tokio::test]
async fn ids_are_reserved_before_an_upload_that_lands_but_reports_failure() {
    let bucket = Arc::new(Memory::default());
    *bucket.lose.lock().unwrap() = 1;
    let index = Arc::new(Index::default());
    let sink = LayerSink::new(
        Arc::new(Layer::new(bucket.clone(), None, true)),
        AfterLanding::Record(index.clone()),
        events::discard(),
    );
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let header_len = 8;
    let whole = Bytes::from(vec![
        0u8;
        crate::header::FileAuthentication::SIZE + header_len + 8
    ]);
    let file = SealedFile::contiguous(whole, header_len, UintN::from(4u64), UintN::from(3u64), 0);
    let file_id = UintN::from(2u64);

    assert!(sink.land(&queue, &file_id, &file).await.is_err());
    assert_eq!(bucket.files.lock().unwrap().len(), 1, "the object is there");
    assert_eq!(*index.calls.lock().unwrap(), ["reserved 6"]);

    sink.land(&queue, &file_id, &file).await.unwrap();
    assert_eq!(
        *index.calls.lock().unwrap(),
        ["reserved 6", "reserved 6", "landed 6 in 2"]
    );
}
