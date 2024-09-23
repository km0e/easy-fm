use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

use clap::{arg, command, Command};
use easy_fm::prelude::*;
use serde::{Deserialize, Serialize};

static HOME: LazyLock<PathBuf> =
    LazyLock::new(|| home::home_dir().expect("Failed to get home directory"));

static DEFAULT_CONFIG_DIR: LazyLock<PathBuf> = LazyLock::new(|| HOME.join(".config/easy-fm"));
static DEFAULT_CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| DEFAULT_CONFIG_DIR.join("config.toml"));

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Config {
    pub r#type: String,
    pub config: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            r#type: "local".to_string(),
            config: DEFAULT_CONFIG_DIR
                .join("local.sqlite3")
                .to_str()
                .unwrap()
                .to_string(),
        }
    }
}

fn load_or_default(path: &Path) -> Config {
    let mut f = xcfg::File::default().path(path.to_str().unwrap());
    if f.load().is_err() {
        Config::default()
    } else {
        f.inner
    }
}

fn print_meta(meta: &Vec<MetaRecord>) {
    println!(
        "{: <40} {: <10} {: <10} {: <40} {: <10}",
        "gid", "dsid", "name", "raw", "desc"
    );
    for MetaRecord {
        gid,
        dsid,
        name,
        raw,
        desc,
    } in meta
    {
        println!(
            "{: <40} {: <10} {: <10} {: <40} {: <10}",
            gid, dsid, name, raw, desc
        );
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cmd = command!()
        .version("0.1")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommands(&[
            Command::new("init")
                .about("Initialize the configuration")
                .subcommand(
                    Command::new("default_config")
                        .visible_alias("dc")
                        .about("Print the default configuration"),
                ),
            Command::new("ds")
                .about("Data storage commands")
                .subcommands(&[
                    Command::new("list")
                        .visible_alias("ls")
                        .about("List data storages"),
                    Command::new("put")
                        .visible_alias("p")
                        .about("Put a data storage")
                        .subcommands(&[Command::new("s3").about("Put an S3 data storage").args(&[
                            arg!(<region> "The S3 region"),
                            arg!(<endpoint> "The S3 endpoint"),
                            arg!(<access_key> "The S3 access key"),
                            arg!(<secret_key> "The S3 secret key"),
                            arg!(<bucket> "The S3 bucket"),
                        ])])
                        .subcommand_required(true),
                    Command::new("del")
                        .visible_alias("d")
                        .about("Delete a data storage")
                        .args(&[arg!(<datastore_id> "The datastore ID")]),
                ])
                .arg_required_else_help(true)
                .subcommand_required(true),
            Command::new("put")
                .visible_alias("p")
                .about("Put something")
                .args(&[
                    arg!(-r --raw [raw] "The raw data").value_parser(["gid", "gide"]),
                    arg!(<datastore_id> "The datastore ID"),
                    arg!(<path> "The path to the file")
                        .value_hint(clap::ValueHint::AnyPath)
                        .value_parser(clap::value_parser!(std::path::PathBuf)),
                ]),
            Command::new("get")
                .visible_alias("g")
                .about("Get a file")
                .args(&[
                    arg!(-g --gid [gid] "The gid of the file"),
                    arg!(-d --datastore_id [datastore_id] "The datastore ID"),
                    arg!(-n --name [name] "The name of the file"),
                    arg!(-p --path [path] "The output path")
                        .value_hint(clap::ValueHint::AnyPath)
                        .value_parser(clap::value_parser!(std::path::PathBuf)),
                ])
                .group(
                    clap::ArgGroup::new("download")
                        .args(["datastore_id", "gid", "path"])
                        .multiple(true),
                )
                .arg_required_else_help(true),
            Command::new("del")
                .visible_alias("d")
                .about("Delete a file")
                .args(&[
                    arg!(-g --gid [gid] "The gid of the file"),
                    arg!(-d --datastore_id [datastore_id] "The datastore ID"),
                    arg!(-n --name [name] "The name of the file"),
                ])
                .group(
                    clap::ArgGroup::new("delete")
                        .args(["datastore_id", "gid", "name"])
                        .multiple(true),
                )
                .arg_required_else_help(true),
            Command::new("list")
                .visible_alias("l")
                .about("List files")
                .arg(arg!(-i [datastore_id]"The datastore ID")),
        ])
        .arg(
            arg!(-c [config] "The configuration file")
                .default_value(DEFAULT_CONFIG_PATH.as_os_str())
                .value_hint(clap::ValueHint::FilePath)
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .get_matches();
    let config = load_or_default(
        cmd.get_one::<PathBuf>("config")
            .expect("Failed to get config"),
    );

    if let Some(("init", cmd)) = cmd.subcommand() {
        if let Some(("default_config", _)) = cmd.subcommand() {
            eprintln!(
                "default_path: {}",
                DEFAULT_CONFIG_PATH.as_os_str().to_str().unwrap()
            );
            println!();
            println!("{}", toml::to_string(&Config::default()).unwrap());
            return;
        }
        init(&config.r#type, &config.config);
        return;
    }

    let mut rm = RM::new(&config.r#type, &config.config);
    match cmd.subcommand() {
        Some(("ds", ds)) => match ds.subcommand() {
            Some(("list", _)) => {
                println!("{: <10} {: <10} {: <10}", "id", "type", "config");
                for DataStorageRecord { id, r#type, cfg } in rm.ds_ls().await {
                    println!("{: <10} {: <10} {: <10}", id, r#type, cfg);
                }
            }
            Some(("put", put)) => {
                if let Some(("s3", s3)) = put.subcommand() {
                    rm.ds_put(
                        "s3",
                        &serde_json::to_string(&S3config {
                            region: s3.get_one::<String>("region").cloned().unwrap(),
                            endpoint: s3.get_one::<String>("endpoint").cloned().unwrap(),
                            access_key: s3.get_one::<String>("access_key").cloned().unwrap(),
                            secret_key: s3.get_one::<String>("secret_key").cloned().unwrap(),
                            bucket: s3.get_one::<String>("bucket").cloned().unwrap(),
                        })
                        .expect("Failed to serialize"),
                    )
                    .await;
                }
            }
            Some(("del", del)) => {
                rm.ds_del(del.get_one::<String>("datastore_id").unwrap())
                    .await;
            }
            _ => {}
        },
        Some(("put", put)) => {
            let datastore_id = put.get_one::<String>("datastore_id").unwrap();
            let path = put.get_one::<std::path::PathBuf>("path").unwrap();
            let info = rm
                .put(
                    datastore_id,
                    path,
                    put.get_one::<String>("raw")
                        .map(|x| x.as_str())
                        .unwrap_or("raw"),
                )
                .await
                .unwrap();
            println!("name: {}, discription: {}", info.name, info.desc);
        }
        Some(("get", get)) => {
            let mrv = rm
                .ls(
                    get.get_one::<String>("gid").map(|x| x.as_str()),
                    get.get_one::<String>("datastore_id").map(|x| x.as_str()),
                    get.get_one::<String>("name").map(|x| x.as_str()),
                )
                .await;
            if mrv.is_empty() {
                println!("No such file");
            } else if mrv.len() > 1 {
                print_meta(&mrv);
            } else {
                let info = &mrv[0];
                rm.get(
                    Some(&info.gid),
                    Some(&info.dsid),
                    Some(&info.name),
                    get.get_one::<std::path::PathBuf>("path")
                        .map(|x| x.as_path()),
                )
                .await
                .expect("Failed to get");
            }
        }
        Some(("del", del)) => {
            let mrv = rm
                .ls(
                    del.get_one::<String>("gid").map(|x| x.as_str()),
                    del.get_one::<String>("datastore_id").map(|x| x.as_str()),
                    del.get_one::<String>("name").map(|x| x.as_str()),
                )
                .await;
            if mrv.is_empty() {
                println!("No such file");
            } else if mrv.len() > 1 {
                print_meta(&mrv);
            } else {
                rm.del(&mrv[0].gid).await.expect("Failed to delete");
            }
        }
        Some(("list", list)) => {
            let datastore_id = list.get_one::<String>("datastore_id");
            print_meta(&rm.ls(None, datastore_id.map(|x| x.as_str()), None).await);
        }
        _ => {}
    }
}
