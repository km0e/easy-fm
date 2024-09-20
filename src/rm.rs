mod ds;
mod meta;

pub use meta::DataStorageRecord;
use std::path::Path;
use tokio::sync::Mutex;

use crate::error::{Error, Result};
pub use ds::{build, DataStorage, S3config};

pub struct RM {
    meta: Mutex<Box<dyn meta::Meta>>,
}

pub fn init(r#type: &str, cfg: &str) {
    meta::init(r#type, cfg);
}

impl RM {
    pub fn new(r#type: &str, cfg: &str) -> Self {
        let meta = meta::build(r#type, cfg).expect("Failed to build");
        Self {
            meta: Mutex::new(meta),
        }
    }

    pub async fn put(&mut self, r#type: &str, cfg: &str) {
        self.meta.lock().await.put(r#type, cfg);
    }

    pub async fn upload(&mut self, dsid: i32, path: &Path, raw: &str) -> Result<meta::MetaRecord> {
        let name = path
            .file_name()
            .and_then(|x| x.to_str())
            .map(|x| x.to_string())
            .unwrap();
        let mut meta = self.meta.lock().await;
        let uuid = uuid::Uuid::new_v4().to_string();
        let raw_name = match raw {
            "raw" => name.clone(),
            "gid" => uuid.clone(),
            "gide" => uuid.clone() + "." + path.extension().and_then(|x| x.to_str()).unwrap_or(""),
            _ => Err(Error::OperationFailed("Unknown raw type".to_string()))?,
        };
        let desc = meta
            .get(dsid)?
            .lock()
            .await
            .put(raw_name.clone(), path)
            .await
            .map_err(|x| Error::OperationFailed(x.to_string()))?;
        let mr = meta::MetaRecord {
            gid: uuid,
            dsid,
            name,
            raw: raw_name,
            desc,
        };
        meta.upload(mr.clone());
        Ok(mr)
    }

    pub async fn download(
        &mut self,
        gid: Option<&str>,
        dsid: Option<i32>,
        name: Option<&str>,
        path: Option<&Path>,
    ) -> Result<()> {
        let mut meta = self.meta.lock().await;
        let mr = meta.find(gid, dsid, name);
        let mr = mr
            .first()
            .ok_or(Error::OperationFailed("Not found".to_string()))?;

        meta.get(mr.dsid)
            .unwrap()
            .lock()
            .await
            .get(mr.raw.clone(), path)
            .await
            .map_err(|x| Error::OperationFailed(x.to_string()))?;
        Ok(())
    }

    pub async fn list(&self) -> Vec<DataStorageRecord> {
        self.meta.lock().await.list()
    }
    pub async fn find(&mut self, dsid: i32) -> Vec<meta::MetaRecord> {
        self.meta.lock().await.find(None, Some(dsid), None)
    }
}
