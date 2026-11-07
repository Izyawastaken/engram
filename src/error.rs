use thiserror::Error;

/// Failures returned by Engram operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to serialize cached value: {0}")]
    Serialize(#[source] bincode::Error),
    #[error("failed to deserialize cached value: {0}")]
    Deserialize(#[source] bincode::Error),
    #[error("SQLite operation failed: {0}")]
    Sqlite(#[from] sqlx::Error),
    #[error("Zstandard operation failed: {0}")]
    Compression(#[source] std::io::Error),
    #[error("cached value for key `{key}` has type `{actual}`, not requested type `{requested}`")]
    TypeMismatch {
        key: String,
        requested: &'static str,
        actual: &'static str,
    },
    #[error("background compression task failed: {0}")]
    BackgroundTask(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, Error>;
