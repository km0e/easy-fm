use std::{fs::File, io::Write, path::Path};

use crate::error::{Error, Result};

use super::{DataStorage, PutError};

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
            .map_err(|err| {
                Error::OperationFailed(format!("Failed to get object from S3: {err:?}"))
            })?;
        while let Some(bytes) = object.body.try_next().await.map_err(|err| {
            Error::OperationFailed(format!("Failed to read from S3 download stream: {err:?}"))
        })? {
            file.write_all(&bytes).map_err(|err| {
                Error::FileError(format!(
                    "Failed to write from S3 download stream to local file: {err:?}"
                ))
            })?;
        }
        Ok(())
    }
    async fn put(&self, name: String, path: &Path) -> Result<String, PutError> {
        let body = aws_sdk_s3::primitives::ByteStream::from_path(path)
            .await
            .map_err(|e| PutError::FileError(e.to_string()))?;
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
            .map_err(|e| PutError::GenericError(e.to_string()))?;
        Ok(file_link)
    }
}
