# easy-fm
a simple tool about file store, share ...

## 1. Features

### 1.1. File Store
You can store your files in the server, and you can get them back by the `[gid, dsid, name]`.

#### 1.1.1. Server
The server is not implemented yet.

#### 1.1.2. Client
The client is implemented by `fm-cli`.
```shell
Usage: fm-cli [OPTIONS] <COMMAND>

Commands:
  init  Initialize the configuration
  ds    Data storage commands
  put   Put something [aliases: p]
  get   Get a file [aliases: g]
  del   Delete a file [aliases: d]
  list  List files [aliases: l]
  help  Print this message or the help of the given subcommand(s)

Options:
  -c [<config>]      The configuration file
  -h, --help         Print help
  -V, --version      Print version
```

#### 1.1.3. Data Storage
These are the supported data storage types.
| Type | Need Config | Description |
| ---- | ----------- | ----------- |
| S3   | access_key, secret_key, region, bucket | Store files in the S3 |

### 1.1. File Share
This feature is not implemented yet. But S3 can be used to share files. Just create a public bucket and put files in it.

## 2. Usage

### 2.1. Server
The server is not implemented yet.

### 2.2. Client

#### 2.2.1. Install
The client is not published yet. But you can install it by `cargo`.

#### 2.2.2. Configuration
You can use the `init` command to initialize the configuration.
```shell
fm-cli init
```
Or dump the configuration to a file.
```shell
fm-cli init dc > ./config.toml
```

#### 2.2.3. Data Storage
You can use the `ds` command to manage the data storage.
```shell
Data storage commands

Usage: fm-cli ds <COMMAND>

Commands:
  list  List data storages [aliases: ls]
  put   Put a data storage [aliases: p]
  del   Delete a data storage [aliases: d]
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```
