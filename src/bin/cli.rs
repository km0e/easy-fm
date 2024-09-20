use std::sync::LazyLock;

use clap::{arg, command, Command};
use easy_fm::prelude::*;
use serde::{Deserialize, Serialize};

static HOME: LazyLock<String> = LazyLock::new(|| {
    let home = home::home_dir().expect("Failed to get home directory");
    home.to_str()
        .expect("Failed to convert home directory to string")
        .to_string()
});

#[derive(Default, Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Config {
    pub r#type: String,
    pub config: String,
}

fn load_or_default(path: &str) -> Config {
    let mut f = xcfg::File::default().path(path);
    if f.load().is_err() {
        Config {
            r#type: "local".to_string(),
            config: HOME.clone() + "/.config/rm/local.sqlite3",
        }
    } else {
        f.inner
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
            Command::new("init").about("Initialize the database"),
            Command::new("put")
                .visible_alias("p")
                .about("Put something")
                .subcommands(&[Command::new("s3").about("Put an S3 datastore").args(&[
                    arg!(<region> "The S3 region"),
                    arg!(<endpoint> "The S3 endpoint"),
                    arg!(<access_key> "The S3 access key"),
                    arg!(<secret_key> "The S3 secret key"),
                    arg!(<bucket> "The S3 bucket"),
                ])])
                .subcommand_required(true),
            Command::new("upload")
                .visible_alias("u")
                .about("Put a file")
                .args(&[
                    arg!(-r --raw [raw] "The raw data").value_parser(["gid", "gide"]),
                    arg!(<datastore_id> "The datastore ID"),
                    arg!(<path> "The path to the file")
                        .value_hint(clap::ValueHint::AnyPath)
                        .value_parser(clap::value_parser!(std::path::PathBuf)),
                ]),
            Command::new("download")
                .visible_alias("d")
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
                ),
            Command::new("list")
                .visible_alias("ls")
                .about("List datastores")
                .help_expected(true)
                .arg(arg!(-i [datastore_id]"The datastore ID")),
        ])
        .arg(arg!(-c [config] "The configuration file"))
        .get_matches();
    let config = load_or_default(
        &cmd.get_one::<String>("config")
            .cloned()
            .unwrap_or(HOME.clone() + "/.config/rm/config.json"),
    );

    if cmd.subcommand_matches("init").is_some() {
        init(&config.r#type, &config.config);
        return;
    }

    let mut rm = RM::new(&config.r#type, &config.config);
    match cmd.subcommand() {
        Some(("put", put)) => {
            if let Some(("s3", s3)) = put.subcommand() {
                rm.put(
                    "s3",
                    &serde_json::to_string(&S3config {
                        region: s3.get_one::<String>("region").unwrap().to_string(),
                        endpoint: s3.get_one::<String>("endpoint").unwrap().to_string(),
                        access_key: s3.get_one::<String>("access_key").unwrap().to_string(),
                        secret_key: s3.get_one::<String>("secret_key").unwrap().to_string(),
                        bucket: s3.get_one::<String>("bucket").unwrap().to_string(),
                    })
                    .expect("Failed to serialize"),
                )
                .await;
            }
        }
        Some(("upload", file)) => {
            let datastore_id = file.get_one::<String>("datastore_id").unwrap();
            let path = file.get_one::<std::path::PathBuf>("path").unwrap();
            let info = rm
                .upload(
                    datastore_id
                        .parse()
                        .expect("Failed to parse datastore ID as i32"),
                    path,
                    file.get_one::<String>("raw")
                        .map(|x| x.as_str())
                        .unwrap_or("raw"),
                )
                .await
                .unwrap();
            println!("name: {}, discription: {}", info.name, info.desc);
        }
        Some(("download", file)) => {
            rm.download(
                file.get_one::<String>("gid").map(|x| x.as_str()),
                file.get_one::<String>("datastore_id")
                    .map(|x| x.parse().unwrap()),
                file.get_one::<String>("name").map(|x| x.as_str()),
                file.get_one::<std::path::PathBuf>("path")
                    .map(|x| x.as_path()),
            )
            .await
            .unwrap();
        }
        Some(("list", list)) => match list.get_one::<String>("datastore_id") {
            Some(datastore_id) => {
                println!("{: <10} {: <10}", "name", "discription");
                for info in rm
                    .find(
                        datastore_id
                            .parse()
                            .expect("Failed to parse datastore ID as i32"),
                    )
                    .await
                {
                    println!("{: <10} {: <10}", info.name, info.desc);
                }
            }
            _ => {
                println!("{: <10} {: <10} {: <10}", "id", "type", "config");
                for DataStorageRecord { id, r#type, cfg } in rm.list().await {
                    println!("{: <10} {: <10} {: <10}", id, r#type, cfg);
                }
            }
        },
        _ => {}
    }
}
