use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Record not found: {0}")]
    NotFound(String),
    #[error("Failed to deal with file: {0}")]
    FileError(String),
    #[error("Operation failed: {0}")]
    OperationFailed(String),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;
