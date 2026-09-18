mod cache;
mod client;
pub mod downloader;
pub mod errors;
pub mod offloader;
mod paths;
pub mod sink;

pub use client::S3Client;
pub use downloader::CloudDownloader;
pub use sink::{CloudSink, LandedIndex};

#[derive(Debug, Clone)]
pub struct CloudSettings {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub prefix: String,
}
