use std::{fs::File, io::Write, path::Path};

use anyhow::{Context, Result};

use crate::error::Error;

use super::DataStorage;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct S3config {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
}

pub struct S3 {
    pub client: aws_sdk_s3::Client,
    pub config: S3config,
}

impl S3 {
    pub fn new(config: S3config) -> S3 {
        let backup = config.clone();
        let cred = aws_sdk_s3::config::Credentials::new(
            config.access_key,
            config.secret_key,
            None,
            None,
            "manul",
        );
        let region = aws_sdk_s3::config::Region::new(config.region);
        let cfg = aws_sdk_s3::Config::builder()
            .region(region)
            .endpoint_url(config.endpoint)
            .credentials_provider(cred)
            .build();
        Self {
            client: aws_sdk_s3::Client::from_conf(cfg),
            config: backup,
        }
    }
}
#[async_trait::async_trait]
impl DataStorage for S3 {
    async fn get(&self, name: String, path: Option<&Path>) -> Result<()> {
        let mut file = File::create(path.unwrap_or(Path::new(&name)))
            .map_err(|err| Error::FileError(format!("Failed to create local file: {err:?}")))?;

        let mut object = self
            .client
            .get_object()
            .bucket(self.config.bucket.clone())
            .key(name)
            .send()
            .await
            .with_context(|| "Failed to get object from S3")?;
        while let Some(bytes) = object
            .body
            .try_next()
            .await
            .with_context(|| "Failed to read from S3 download stream")?
        {
            file.write_all(&bytes)
                .with_context(|| "Failed to write from S3 download stream to local file")?;
        }
        Ok(())
    }
    async fn put(&self, name: String, path: &Path) -> Result<String> {
        let body = aws_sdk_s3::primitives::ByteStream::from_path(path)
            .await
            .map_err(|e| Error::FileError(e.to_string()))?;
        let file_link =
            self.config.endpoint.clone() + "/" + self.config.bucket.as_str() + "/" + name.as_str();
        let _ = self
            .client
            .put_object()
            .bucket(self.config.bucket.clone())
            .key(name)
            .body(body)
            .send()
            .await
            .with_context(|| "Failed to put object to S3")?;
        Ok(file_link)
    }
    async fn del(&self, name: String) -> Result<()> {
        let _ = self
            .client
            .delete_object()
            .bucket(self.config.bucket.clone())
            .key(name)
            .send()
            .await
            .with_context(|| "Failed to delete object from S3")?;
        Ok(())
    }
}
