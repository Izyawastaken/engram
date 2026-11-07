//! Engram is an asynchronous two-tier cache.
//!
//! Values are kept deserialized in an in-memory Moka cache and persisted as
//! serialized blobs in SQLite. L2 rows may be Zstandard-compressed.

mod compression;
mod config;
mod engine;
mod error;
mod memory;
mod storage;

pub use config::{Compression, EngramBuilder, SqliteSynchronous};
pub use engine::{CacheEntry, CacheMetadata, CacheTier, Engram};
pub use error::{Error, Result};
