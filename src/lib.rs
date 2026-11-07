//! # Engram
//!
//! Engram is an asynchronous, two-tier cache for Rust applications. It keeps
//! deserialized values in a weighted [Moka] memory cache (L1) and persists
//! Serde-serialized bytes in a SQLite WAL database (L2). An L1 eviction never
//! deletes L2, so values can be recovered after eviction or restart.
//!
//! ## Installation
//!
//! The crates.io package is named `engram-cache`; its Rust library name is
//! `engram`:
//!
//! ```toml
//! [dependencies]
//! engram = { package = "engram-cache", version = "0.1" }
//! serde = { version = "1", features = ["derive"] }
//! tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
//! ```
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use engram::{Compression, Engram};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Deserialize, Serialize)]
//! struct Project {
//!     id: u64,
//!     name: String,
//! }
//!
//! # async fn example() -> engram::Result<()> {
//! let cache = Engram::builder()
//!     .storage("./cache/engram.db")
//!     .memory_capacity(64 * 1024 * 1024)
//!     .compression(Compression::Zstd { level: 3 })
//!     .build()
//!     .await?;
//!
//! let project = Project { id: 123, name: "Engram".into() };
//! cache.set("project:123", &project, Some(Duration::from_secs(300))).await?;
//! let cached: Option<Project> = cache.get("project:123").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## How the two tiers behave
//!
//! Reads consult L1 first. A miss loads, decompresses, and deserializes the
//! L2 row, then promotes that value to L1. The [`CacheMetadata`] returned by
//! [`Engram::get_with_metadata`] identifies the tier that served the read.
//! [`Engram::get_arc`] and [`Engram::set_arc`] retain an [`std::sync::Arc`] in
//! L1 and are preferable for large values that should not be cloned.
//!
//! A successful write has committed its SQLite transaction to the WAL. A
//! failed single write invalidates the just-written L1 value before returning
//! its error. [`Engram::set_many`] writes its entire batch in one transaction
//! and only updates L1 after that commit succeeds.
//!
//! ## Lifetimes and invalidation
//!
//! A TTL is optional on every write. `Some(duration)` gives an entry an
//! absolute expiry; expired entries are never returned and are removed lazily.
//! `None` makes an entry permanent: it remains in L2 across restarts until
//! [`Engram::remove`] or [`Engram::clear`] is called. L1 still has a memory
//! budget, so a permanent entry can be evicted from memory and later restored
//! from L2.
//!
//! Use [`Engram::metadata`] to inspect the tier and expiration without
//! deserializing L2. Use [`Engram::purge_expired`] for on-demand physical
//! deletion, or [`EngramBuilder::cleanup_interval`] for periodic cleanup.
//!
//! ## Stable schemas for persisted values
//!
//! The string-key APIs use Rust's `type_name::<T>()` to reject requests for an
//! incompatible in-process type. That tag can change after a type rename or a
//! module move. For cache data intended to survive releases, use
//! [`CacheKey<T>`] and supply your own stable schema tag. Keep the tag while
//! the serialized representation remains compatible; bump it for an
//! intentionally incompatible change.
//!
//! ```no_run
//! use engram::{CacheKey, Engram};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Deserialize, Serialize)]
//! struct Project { id: u64 }
//!
//! # async fn example(cache: Engram) -> engram::Result<()> {
//! let key = CacheKey::<Project>::new("project:123", "project-record@1");
//! cache.set_key(&key, &Project { id: 123 }, None).await?;
//! let project: Option<Project> = cache.get_key(&key).await?;
//! # Ok(())
//! # }
//! ```
//!
//! A mismatched tag returns [`Error::TypeMismatch`] rather than attempting to
//! decode the bytes. The same tag is checked in both L1 and L2.
//!
//! ## Concurrency and fetch-through
//!
//! Operations for one key are serialized, while unrelated keys can proceed in
//! parallel. Bulk writes acquire their key locks in sorted order, avoiding
//! deadlock between overlapping batches. [`Engram::clear`] takes a global
//! write gate: it waits for active operations, including L2-to-L1 promotions,
//! and prevents new operations until both tiers are cleared.
//!
//! [`Engram::get_or_try_init`] is an opt-in fetch-through helper. It invokes
//! the initializer only after a miss and coalesces simultaneous
//! initializations for the same key. Initializer errors are not cached.
//!
//! ## Durability and performance
//!
//! SQLite uses WAL mode, a five-second busy timeout, and a partial expiry
//! index. [`SqliteSynchronous::Full`] is the default and asks SQLite for its
//! strongest synchronization behavior. [`SqliteSynchronous::Normal`] is often
//! faster, but the most recent committed WAL transactions can be lost after a
//! power failure or OS crash; SQLite's internal consistency is preserved.
//!
//! Compression defaults to Zstandard level 3. Values below 1 KiB are not
//! compressed, and a larger compressed result is discarded by default. Tune
//! these behaviors with [`EngramBuilder::compression_threshold`] and
//! [`EngramBuilder::compress_only_if_smaller`]. L1 capacity is weighted by
//! serialized byte length unless you use [`Engram::set_with_weight`] or
//! [`Engram::set_arc_with_weight`] to provide a more representative memory
//! cost.
//!
//! [Moka]: https://docs.rs/moka

mod compression;
mod config;
mod engine;
mod error;
mod memory;
mod storage;

pub use config::{Compression, EngramBuilder, SqliteSynchronous};
pub use engine::{CacheEntry, CacheKey, CacheMetadata, CacheTier, Engram};
pub use error::{Error, Result};
