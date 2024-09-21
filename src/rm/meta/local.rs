use anyhow::{Context, Result};

use crate::{
    error::Error,
    rm::{build, ds::SafeDs},
};

use super::{DataStorageRecord, Meta, MetaRecord};

pub struct Local {
    gid_conn: rusqlite::Connection,
    datastore_conn: std::collections::HashMap<String, SafeDs>,
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
    fn ds_get(&mut self, dsid: &str) -> Result<SafeDs> {
        if let Some(cli) = self.datastore_conn.get(dsid) {
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
            .with_context(|| "Failed to get row")?;
        let cli = build(&r#type, &cfg).expect("Failed to build");
        Ok(self
            .datastore_conn
            .entry(dsid.to_string())
            .or_insert(SafeDs::new(cli))
            .clone())
    }
    fn ds_put(&mut self, r#type: &str, cfg: &str) {
        self.gid_conn
            .execute("INSERT INTO rm (type, cfg) VALUES (?, ?)", [r#type, cfg])
            .expect("Failed to insert");
    }
    fn ds_del(&mut self, dsid: &str) {
        self.gid_conn
            .execute("DELETE FROM rm WHERE id = ?", [dsid])
            .expect("Failed to delete");
    }
    fn ds_ls(&self) -> Vec<DataStorageRecord> {
        let mut stmt = self
            .gid_conn
            .prepare("SELECT * FROM rm")
            .expect("Failed to prepare statement");
        stmt.query_map([], |row| {
            Ok(DataStorageRecord {
                id: row.get::<usize, i32>(0)?.to_string(),
                r#type: row.get(1)?,
                cfg: row.get(2)?,
            })
        })
        .expect("Failed to query map")
        .map(|row| row.expect("Failed to get row"))
        .collect()
    }
    fn put(&mut self, meta: MetaRecord) {
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
    fn del(&mut self, gid: &str) {
        self.gid_conn
            .execute("DELETE FROM map WHERE gid = ?", [gid])
            .expect("Failed to delete");
    }

    fn ls(&mut self, gid: Option<&str>, dsid: Option<&str>, name: Option<&str>) -> Vec<MetaRecord> {
        let mut q = "SELECT * FROM map".to_string();
        if gid.is_some() || dsid.is_some() || name.is_some() {
            q = q
                + " WHERE "
                + &[("gid", gid), ("dsid", dsid), ("name", name)]
                    .iter()
                    .flat_map(|(k, v)| v.map(|v| format!("{} = '{}'", k, v)))
                    .collect::<Vec<String>>()
                    .join(" AND ");
        }
        q.push(';');
        let mut stmt = self
            .gid_conn
            .prepare(&q)
            .expect("Failed to prepare statement");
        let records = stmt
            .query_map([], |row| {
                Ok(MetaRecord {
                    gid: row.get(0)?,
                    dsid: row.get::<usize, i32>(1)?.to_string(),
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
