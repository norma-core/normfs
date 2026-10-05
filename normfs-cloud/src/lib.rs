mod cache;
mod client;
pub mod downloader;
pub mod errors;
pub mod offloader;
mod paths;
pub mod sink;
pub mod store;

pub use client::S3Client;
pub use downloader::CloudDownloader;
pub use paths::is_id_component;
pub use sink::{CloudSink, LandedIndex};
pub use store::S3Store;

#[derive(Debug, Clone)]
pub struct CloudSettings {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub prefix: String,
}
