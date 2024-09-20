use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PutError {
    #[error("Failed to load file: {0}")]
    FileError(String),
    #[error("Failed to put object: {0}")]
    GenericError(String),
}

#[async_trait::async_trait]
pub trait DataStorage {
    async fn get(&self, name: String, path: Option<&Path>) -> Result<()>;
    async fn put(&self, name: String, path: &Path) -> Result<String, PutError>;
}

#[derive(Clone)]
pub struct SafeDs(Arc<Mutex<Box<dyn DataStorage>>>);

impl SafeDs {
    pub fn new(ds: Box<dyn DataStorage>) -> Self {
        Self(Arc::new(Mutex::new(ds)))
    }
}

impl Deref for SafeDs {
    type Target = Arc<Mutex<Box<dyn DataStorage>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SafeDs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

mod s3;

pub use s3::S3config;
use tokio::sync::Mutex;

use crate::error::Result;

pub fn build(r#type: &str, config: &str) -> Result<Box<dyn DataStorage>, serde_json::Error> {
    match r#type {
        "s3" => {
            let config: s3::S3config = serde_json::from_str(config).expect("Failed to deserialize");
            Ok(Box::new(s3::S3::new(config)))
        }
        _ => panic!("Unknown type"),
    }
}
