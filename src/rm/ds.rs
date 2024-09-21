use std::path::Path;

#[async_trait::async_trait]
pub trait DataStorage {
    /// Get file from storage
    async fn get(&self, name: String, path: Option<&Path>) -> Result<()>;
    /// Put file to storage
    async fn put(&self, name: String, path: &Path) -> Result<String>;
    /// delete file from storage
    async fn del(&self, name: String) -> Result<()>;
}

mod s3;

use anyhow::Result;
pub use s3::S3config;

pub fn build(r#type: &str, config: &str) -> Result<Box<dyn DataStorage>, serde_json::Error> {
    match r#type {
        "s3" => {
            let config: s3::S3config = serde_json::from_str(config).expect("Failed to deserialize");
            Ok(Box::new(s3::S3::new(config)))
        }
        _ => panic!("Unknown type"),
    }
}

mod safe;

pub use safe::SafeDs;
