mod ds;
mod meta;

use anyhow::{Context, Result};
pub use meta::{DataStorageRecord, MetaRecord};
use std::path::Path;

pub use ds::{build, DataStorage, S3config};

pub struct RM {
    meta: Box<dyn meta::Meta>,
}

pub fn init(r#type: &str, cfg: &str) {
    meta::init(r#type, cfg);
}

impl RM {
    pub fn new(r#type: &str, cfg: &str) -> Self {
        let meta = meta::build(r#type, cfg).expect("Failed to build");
        Self { meta }
    }

    pub async fn ds_put(&mut self, r#type: &str, cfg: &str) {
        self.meta.ds_put(r#type, cfg);
    }

    pub async fn ds_del(&mut self, dsid: &str) {
        self.meta.ds_del(dsid);
    }

    pub async fn ds_ls(&self) -> Vec<DataStorageRecord> {
        self.meta.ds_ls()
    }

    pub async fn put(&mut self, dsid: &str, path: &Path, raw: &str) -> Result<MetaRecord> {
        let name = path
            .file_name()
            .and_then(|x| x.to_str())
            .map(|x| x.to_string())
            .unwrap();
        let uuid = uuid::Uuid::new_v4().to_string();
        let raw_name = match raw {
            "raw" => name.clone(),
            "gid" => uuid.clone(),
            "gide" => uuid.clone() + "." + path.extension().and_then(|x| x.to_str()).unwrap_or(""),
            _ => Err(anyhow::anyhow!("Unknown raw type"))?,
        };
        let desc = self
            .meta
            .ds_get(dsid)?
            .lock()
            .await
            .put(raw_name.clone(), path)
            .await
            .with_context(|| "Failed to put")?;
        let mr = MetaRecord {
            gid: uuid,
            dsid: dsid.to_string(),
            name,
            raw: raw_name,
            desc,
        };
        self.meta.put(mr.clone());
        Ok(mr)
    }

    pub async fn get(
        &mut self,
        gid: Option<&str>,
        dsid: Option<&str>,
        name: Option<&str>,
        path: Option<&Path>,
    ) -> Result<()> {
        let mr = self.meta.ls(gid, dsid, name);
        let mr = mr.first().with_context(|| "Not found")?;

        self.meta
            .ds_get(&mr.dsid)
            .unwrap()
            .lock()
            .await
            .get(mr.raw.clone(), path)
            .await
            .with_context(|| "Failed to get")?;
        Ok(())
    }

    pub async fn del(&mut self, gid: &str) -> Result<()> {
        let mr = self.meta.ls(Some(gid), None, None);
        let mr = mr.first().with_context(|| "Not found")?;

        self.meta
            .ds_get(&mr.dsid)
            .unwrap()
            .lock()
            .await
            .del(mr.raw.clone())
            .await
            .with_context(|| "Failed to del")?;
        self.meta.del(gid);
        Ok(())
    }
    pub async fn ls(
        &mut self,
        gid: Option<&str>,
        dsid: Option<&str>,
        name: Option<&str>,
    ) -> Vec<MetaRecord> {
        self.meta.ls(gid, dsid, name)
    }
}
