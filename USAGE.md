# Using Engram

Engram is an async, application-independent cache. It keeps deserialized values in Moka (L1) and persists Serde-serialized, optionally Zstandard-compressed values in SQLite (L2). Successful writes always reach L2.

## Setup

```toml
[dependencies]
engram = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use std::time::Duration;
use engram::{Compression, Engram};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Project { id: u64, name: String }

let cache = Engram::builder()
    .storage("./cache/engram.db")
    .memory_capacity(256 * 1024 * 1024)
    .compression(Compression::Zstd { level: 3 })
    .build()
    .await?;

let project = Project { id: 123, name: "Engram".into() };
cache.set("project:123", &project, Some(Duration::from_secs(300))).await?;
let cached: Option<Project> = cache.get("project:123").await?;
cache.remove("project:123").await?;
# Ok::<(), engram::Error>(())
```

`get` and `set` require `Clone` because they return and retain owned deserialized values. For large values, use `Arc` APIs to avoid cloning:

```rust
use std::sync::Arc;

let project = Arc::new(Project { id: 123, name: "Engram".into() });
cache.set_arc("project:123", project, None).await?;
let cached: Option<Arc<Project>> = cache.get_arc("project:123").await?;
# Ok::<(), engram::Error>(())
```

## Expiration, permanent entries, and metadata

Pass `Some(Duration)` for a TTL. Pass `None` to keep an entry indefinitely: it remains in SQLite across restarts until `remove` or `clear` is called. Moka may evict a permanent entry from L1 when its RAM budget is reached, but that never deletes its L2 copy.

```rust
cache.set("account:42", &project, None).await?; // no automatic expiry

if let Some(entry) = cache.get_with_metadata::<Project>("project:123").await? {
    println!("from {:?}; expires at {:?}", entry.metadata.tier, entry.metadata.expires_at);
    println!("remaining TTL: {:?}", entry.metadata.remaining_ttl());
}

// Check expiry/source without deserializing an L2 value.
let metadata = cache.metadata("project:123").await?;
# Ok::<(), engram::Error>(())
```

`expires_at: None` and `remaining_ttl(): None` mean the entry is permanent. A missing key is returned as `None` by `metadata`. Expired entries are never returned and are removed lazily; call `purge_expired()` yourself or configure scheduled cleanup.

```rust
let cache = Engram::builder()
    .storage("./cache/engram.db")
    .cleanup_interval(Duration::from_secs(300))
    .build()
    .await?;

let deleted_rows = cache.purge_expired().await?;
# Ok::<(), engram::Error>(())
```

## Throughput options

Small values do not benefit from compression. Engram defaults to compressing values of at least 1 KiB and retains uncompressed bytes if Zstd is not smaller. Both policies are configurable.

```rust
use engram::SqliteSynchronous;

let cache = Engram::builder()
    .storage("./cache/engram.db")
    .compression_threshold(4 * 1024)
    .compress_only_if_smaller(true)
    // Faster WAL commits, with a small power-loss/OS-crash durability trade-off.
    .sqlite_synchronous(SqliteSynchronous::Normal)
    .build()
    .await?;
# Ok::<(), engram::Error>(())
```

Use `set_many` for bulk loading; its writes share one SQLite transaction.

```rust
cache.set_many(
    [("project:1", Project { id: 1, name: "One".into() }),
     ("project:2", Project { id: 2, name: "Two".into() })],
    None,
).await?;
# Ok::<(), engram::Error>(())
```

L1 capacity is weighted by serialized byte length by default. When that does not approximate an object’s resident memory cost, provide an explicit weight; it is persisted and reused when the entry is restored from L2.

```rust
cache.set_with_weight("project:123", &project, None, 2 * 1024 * 1024).await?;
# Ok::<(), engram::Error>(())
```

## Fetch-through without a stampede

`get` never invokes external work. `get_or_try_init` is an opt-in fetch-through helper: simultaneous initializations for one missing key are coalesced, and failures are not cached.

```rust
let project: Project = cache
    .get_or_try_init("project:123", Some(Duration::from_secs(300)), || async {
        // Fetch from your own database, service, or file source here.
        Ok(Project { id: 123, name: "Engram".into() })
    })
    .await?;
# Ok::<(), engram::Error>(())
```

## Operational notes

- SQLite uses WAL mode, a five-second busy timeout, and a partial expiry index.
- Unrelated keys can read and write concurrently; operations on one key are serialized so invalidated L2 data cannot revive L1.
- A key is associated with its Rust type. A mismatched type returns `Error::TypeMismatch` instead of decoding incompatible bytes. Use new keys or clear the cache when changing a cached type across releases.
- `remove` deletes both tiers; `clear` deletes both tiers globally.
