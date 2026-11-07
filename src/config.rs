use std::path::{Path, PathBuf};

/// The encoding used for values written to persistent storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Compression {
    /// Store serialized bytes as-is.
    Disabled,
    /// Store serialized bytes compressed with Zstandard at the supplied level.
    Zstd { level: i32 },
}

impl Default for Compression {
    fn default() -> Self {
        Self::Zstd { level: 3 }
    }
}

/// Builds an [`crate::Engram`] instance.
#[derive(Clone, Debug)]
pub struct EngramBuilder {
    pub(crate) storage: PathBuf,
    pub(crate) memory_capacity: u64,
    pub(crate) compression: Compression,
    pub(crate) sqlite_connections: u32,
}

impl EngramBuilder {
    pub(crate) fn new() -> Self {
        Self {
            storage: PathBuf::from("engram.db"),
            memory_capacity: 64 * 1024 * 1024,
            compression: Compression::default(),
            sqlite_connections: 8,
        }
    }

    /// Selects the SQLite database file.
    pub fn storage(mut self, path: impl AsRef<Path>) -> Self {
        self.storage = path.as_ref().to_owned();
        self
    }

    /// Sets the approximate maximum L1 capacity in serialized bytes.
    pub fn memory_capacity(mut self, bytes: u64) -> Self {
        self.memory_capacity = bytes.max(1);
        self
    }

    /// Selects compression for newly persisted values.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the maximum number of SQLite connections used by this cache.
    pub fn sqlite_connections(mut self, connections: u32) -> Self {
        self.sqlite_connections = connections.max(1);
        self
    }

    /// Opens the database and initializes its schema.
    pub async fn build(self) -> crate::Result<crate::Engram> {
        crate::Engram::from_builder(self).await
    }
}
