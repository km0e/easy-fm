use anyhow::Result;

use super::ds::SafeDs;

pub struct DataStorageRecord {
    pub id: String,
    pub r#type: String,
    pub cfg: String,
}

#[derive(Clone)]
pub struct MetaRecord {
    pub gid: String,
    pub dsid: String,
    pub name: String,
    pub raw: String,
    pub desc: String,
}

#[async_trait::async_trait]
pub trait Meta {
    fn ds_get(&mut self, dsid: &str) -> Result<SafeDs>;
    fn ds_put(&mut self, r#type: &str, config: &str);
    fn ds_ls(&self) -> Vec<DataStorageRecord>;

    fn put(&mut self, meta: MetaRecord);
    fn del(&mut self, gid: &str);
    fn ls(&mut self, gid: Option<&str>, dsid: Option<&str>, name: Option<&str>) -> Vec<MetaRecord>;
}

pub fn build(r#type: &str, config: &str) -> Result<Box<dyn Meta>, serde_json::Error> {
    match r#type {
        "local" => Ok(Box::new(local::Local::new(config))),
        _ => panic!("Unknown type"),
    }
}

pub fn init(r#type: &str, config: &str) {
    match r#type {
        "local" => local::init(config),
        _ => panic!("Unknown type"),
    }
}
mod local;
