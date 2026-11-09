use std::{
    path::{Path, PathBuf},
    time::Duration,
};

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

/// SQLite's durability/performance trade-off in WAL mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SqliteSynchronous {
    /// Request the strongest SQLite durability setting.
    #[default]
    Full,
    /// Usually faster, but the last committed transactions can be lost after an OS
    /// crash or power failure. SQLite remains internally consistent.
    Normal,
}

impl SqliteSynchronous {
    pub(crate) const fn as_pragma(self) -> &'static str {
        match self {
            Self::Full => "FULL",
            Self::Normal => "NORMAL",
        }
    }
}

/// When a write becomes durable in SQLite.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WriteDurability {
    /// Wait for the SQLite transaction to commit before returning. This is the default.
    #[default]
    Synchronous,
    /// Update L1 immediately and group the L2 write with other buffered writes.
    /// Call [`crate::Engram::flush`] before shutdown or when persistence is required.
    Buffered,
}

/// Builds an [`crate::Engram`] instance.
#[derive(Clone, Debug)]
pub struct EngramBuilder {
    pub(crate) storage: PathBuf,
    pub(crate) memory_capacity: u64,
    pub(crate) compression: Compression,
    pub(crate) compression_threshold: usize,
    pub(crate) compress_only_if_smaller: bool,
    pub(crate) sqlite_connections: u32,
    pub(crate) sqlite_synchronous: SqliteSynchronous,
    pub(crate) cleanup_interval: Option<Duration>,
    pub(crate) write_durability: WriteDurability,
}

impl EngramBuilder {
    pub(crate) fn new() -> Self {
        Self {
            storage: PathBuf::from("engram.db"),
            memory_capacity: 64 * 1024 * 1024,
            compression: Compression::default(),
            compression_threshold: 1024,
            compress_only_if_smaller: true,
            sqlite_connections: 8,
            sqlite_synchronous: SqliteSynchronous::Full,
            cleanup_interval: None,
            write_durability: WriteDurability::Synchronous,
        }
    }

    /// Selects the SQLite database file.
    pub fn storage(mut self, path: impl AsRef<Path>) -> Self {
        self.storage = path.as_ref().to_owned();
        self
    }

    /// Sets the approximate maximum L1 capacity in weighted bytes.
    pub fn memory_capacity(mut self, bytes: u64) -> Self {
        self.memory_capacity = bytes.max(1);
        self
    }

    /// Selects compression for newly persisted values.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Compresses only values at least this large. The default is 1 KiB.
    pub fn compression_threshold(mut self, bytes: usize) -> Self {
        self.compression_threshold = bytes;
        self
    }

    /// When enabled (the default), stores raw bytes when Zstd does not shrink them.
    pub fn compress_only_if_smaller(mut self, enabled: bool) -> Self {
        self.compress_only_if_smaller = enabled;
        self
    }

    /// Sets the maximum number of SQLite connections used by this cache.
    pub fn sqlite_connections(mut self, connections: u32) -> Self {
        self.sqlite_connections = connections.max(1);
        self
    }

    /// Selects SQLite's WAL durability setting. [`SqliteSynchronous::Full`] is the default.
    pub fn sqlite_synchronous(mut self, synchronous: SqliteSynchronous) -> Self {
        self.sqlite_synchronous = synchronous;
        self
    }

    /// Selects the default durability for ordinary `set` calls.
    pub fn write_durability(mut self, durability: WriteDurability) -> Self {
        self.write_durability = durability;
        self
    }

    /// Enables periodic deletion of expired L2 rows. Disabled by default.
    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = (!interval.is_zero()).then_some(interval);
        self
    }

    /// Opens the database and initializes its schema.
    pub async fn build(self) -> crate::Result<crate::Engram> {
        crate::Engram::from_builder(self).await
    }
}
