use crate::error::Result;

use super::ds::SafeDs;

pub struct DataStorageRecord {
    pub id: i32,
    pub r#type: String,
    pub cfg: String,
}

#[derive(Clone)]
pub struct MetaRecord {
    pub gid: String,
    pub dsid: i32,
    pub name: String,
    pub raw: String,
    pub desc: String,
}

#[async_trait::async_trait]
pub trait Meta {
    fn get(&mut self, dsid: i32) -> Result<SafeDs>;
    fn put(&mut self, r#type: &str, config: &str);
    fn list(&self) -> Vec<DataStorageRecord>;
    fn upload(&mut self, meta: MetaRecord);
    fn find(&mut self, gid: Option<&str>, dsid: Option<i32>, name: Option<&str>)
        -> Vec<MetaRecord>;
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
