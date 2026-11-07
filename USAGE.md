# Using Engram

Engram is an async, application-independent cache. It retains deserialized values in Moka (L1) and persists serialized values in SQLite (L2). A successful `set` always reaches SQLite; there is no silent memory-only success mode.

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

Values need `Serialize`, `Deserialize`, and `Clone`: L1 intentionally keeps a deserialized owned copy. A key is associated with its Rust type; asking for another type returns `Error::TypeMismatch` rather than attempting to decode incompatible bytes.
Changing a cached type between application releases should use new keys or clear the old database; type metadata intentionally favors a safe mismatch over decoding stale bytes.

## Expiration and invalidation

Pass `None` for no expiration, or `Some(Duration)` for a TTL. Expired values are never returned from either tier. They are deleted from SQLite when encountered; use `purge_expired()` from an application-managed maintenance schedule to reclaim rows proactively.

```rust
let deleted_rows = cache.purge_expired().await?;
cache.clear().await?; // clears L1 and L2
```

## Fetch-through without a stampede

`get` never invokes external work. If your application wants fetch-through behavior, use `get_or_try_init`. Simultaneous initializations for one missing key are coalesced; failed initialization is not cached.

```rust
let project: Project = cache
    .get_or_try_init("project:123", Some(Duration::from_secs(300)), || async {
        // Fetch from your own database, service, or file source here.
        Ok(Project { id: 123, name: "Engram".into() })
    })
    .await?;
```

## Operational notes

- SQLite uses WAL mode and a five-second busy timeout for practical reader/writer concurrency.
- L1 capacity is weighted by serialized byte length. Moka owns eviction; evicting L1 never deletes L2.
- L2 records retain compression and type metadata, so entries remain readable after a compression configuration change.
- Compression runs outside Tokio async worker threads. Use `Compression::Disabled` for tiny values or workloads where disk space is not a concern.
- `remove` and `clear` serialize with L2-to-L1 promotion, preventing an invalidated L2 value from reviving in memory.
