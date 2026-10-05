mod client;
pub mod errors;
mod paths;
pub mod store;

pub use client::S3Client;
pub use paths::is_id_component;
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
