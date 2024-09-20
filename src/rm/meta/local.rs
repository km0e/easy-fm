use crate::{
    error::{Error, Result},
    rm::{build, ds::SafeDs},
};

use super::{DataStorageRecord, Meta, MetaRecord};

pub struct Local {
    gid_conn: rusqlite::Connection,
    datastore_conn: std::collections::HashMap<i32, SafeDs>,
}

pub fn init(path: &str) {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    )
    .expect("Failed to open database");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS rm (
                id INTEGER PRIMARY KEY,
                type TEXT NOT NULL,
                cfg TEXT NOT NULL
            )",
        [],
    )
    .expect("Failed to create table");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS map (
                gid TEXT NOT NULL,
                dsid INTEGER NOT NULL,
                name TEXT NOT NULL,
                raw TEXT NOT NULL,
                discription TEXT NOT NULL
            )",
        [],
    )
    .expect("Failed to create table");
}
impl Local {
    pub fn new(path: &str) -> Self {
        let conn = rusqlite::Connection::open(path).expect("Failed to open database");
        Self {
            gid_conn: conn,
            datastore_conn: std::collections::HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl Meta for Local {
    fn get(&mut self, dsid: i32) -> Result<SafeDs> {
        if let Some(cli) = self.datastore_conn.get(&dsid) {
            return Ok(cli.clone());
        }
        let mut stmt = self
            .gid_conn
            .prepare("SELECT * FROM rm WHERE id = ?")
            .expect("Failed to prepare statement");
        let (r#type, cfg) = stmt
            .query_map([dsid], |row| {
                let r#type: String = row.get(1)?;
                let cfg: String = row.get(2)?;
                Ok((r#type, cfg))
            })
            .expect("Failed to query map")
            .next()
            .ok_or(Error::NotFound("Datastore not found".to_string()))?
            .map_err(|x| Error::OperationFailed(x.to_string()))?;
        let cli = build(&r#type, &cfg).expect("Failed to build");
        Ok(self
            .datastore_conn
            .entry(dsid)
            .or_insert(SafeDs::new(cli))
            .clone())
    }
    fn put(&mut self, r#type: &str, cfg: &str) {
        self.gid_conn
            .execute("INSERT INTO rm (type, cfg) VALUES (?, ?)", [r#type, cfg])
            .expect("Failed to insert");
    }
    fn list(&self) -> Vec<DataStorageRecord> {
        let mut stmt = self
            .gid_conn
            .prepare("SELECT * FROM rm")
            .expect("Failed to prepare statement");
        stmt.query_map([], |row| {
            Ok(DataStorageRecord {
                id: row.get(0)?,
                r#type: row.get(1)?,
                cfg: row.get(2)?,
            })
        })
        .expect("Failed to query map")
        .map(|row| row.expect("Failed to get row"))
        .collect()
    }
    fn upload(&mut self, meta: MetaRecord) {
        self.gid_conn
            .execute(
                "INSERT INTO map (gid, dsid, name, raw, discription) VALUES (?, ?, ?, ?, ?)",
                [
                    meta.gid,
                    meta.dsid.to_string(),
                    meta.name,
                    meta.raw,
                    meta.desc,
                ],
            )
            .expect("Failed to insert");
    }

    fn find(
        &mut self,
        gid: Option<&str>,
        dsid: Option<i32>,
        name: Option<&str>,
    ) -> Vec<MetaRecord> {
        // let mut stmt = self
        //     .gid_conn
        //     .prepare("SELECT * FROM map WHERE gid = ?")
        //     .expect("Failed to prepare statement");
        // stmt.query_map([dsid], |row| {
        //     Ok(MetaRecord {
        //         gid: row.get(0)?,
        //         dsid: row.get(1)?,
        //         name: row.get(2)?,
        //         desc: row.get(3)?,
        //     })
        // })
        // .expect("Failed to query map")
        // .map(|row| row.expect("Failed to get row"))
        // .collect()

        let mut query = "SELECT * FROM map WHERE 1=1".to_string(); // 基础查询

        // 根据可选参数构建查询和参数列表
        if let Some(gid) = gid {
            query = format!("{} AND gid = {}", query, gid);
        }
        if let Some(dsid) = dsid {
            query = format!("{} AND dsid = {}", query, dsid);
        }
        if let Some(name) = name {
            query = format!("{} AND name = {}", query, name);
        }

        let mut stmt = self
            .gid_conn
            .prepare(&query)
            .expect("Failed to prepare statement");

        // 使用参数查询并映射结果
        let records = stmt
            .query_map([], |row| {
                Ok(MetaRecord {
                    gid: row.get(0)?,
                    dsid: row.get(1)?,
                    name: row.get(2)?,
                    raw: row.get(3)?,
                    desc: row.get(4)?,
                })
            })
            .expect("Failed to query map")
            .map(|row| row.expect("Failed to get row"))
            .collect();

        records
    }
}
