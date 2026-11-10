use normfs::{Error, NormFS, NormFsSettings};
use tempfile::TempDir;

#[tokio::test]
async fn a_data_dir_takes_one_instance_at_a_time() {
    let dir = TempDir::new().unwrap();
    let first = NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();

    match NormFS::new(dir.path().to_path_buf(), NormFsSettings::default()).await {
        Err(Error::DataDirInUse) => {}
        Err(e) => panic!("expected DataDirInUse, got {e}"),
        Ok(_) => panic!("a second instance opened a data dir in use"),
    }

    first.close().await.unwrap();
    let second = NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();
    drop(second);
    let third = NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();
    third.close().await.unwrap();
}
