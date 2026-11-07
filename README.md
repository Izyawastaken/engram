# Engram

Engram is an asynchronous Rust cache with a deserialized Moka L1 and a SQLite WAL-backed L2. It is designed for applications that want low-latency reads without giving up cache contents on restart.

## Highlights

- Typed Serde values, with mismatch protection before decoding.
- Moka L1 stores deserialized `Arc` values; SQLite L2 stores serialized blobs.
- Optional Zstandard compression, TTLs, permanent entries, expiry cleanup, and weighted memory capacity.
- Per-key serialization, transactional `set_many`, global `clear` exclusion, and fetch-through single-flight initialization.
- `CacheKey<T>` supplies a stable application-defined schema tag for cache entries that survive Rust refactors.

```rust,no_run
use engram::{Compression, Engram};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Deserialize, Serialize)]
struct Project {
    id: u64,
    name: String,
}

# async fn example() -> engram::Result<()> {
let cache = Engram::builder()
    .storage("./cache/engram.db")
    .compression(Compression::Zstd { level: 3 })
    .build()
    .await?;

cache.set("project:123", &Project { id: 123, name: "Engram".into() }, Some(Duration::from_secs(300))).await?;
let project: Option<Project> = cache.get("project:123").await?;
# Ok(())
# }
```

The default `SqliteSynchronous::Full` asks SQLite for its strongest sync behavior. `Normal` is available when speed matters more than retaining the most recent WAL commits through a power loss or OS crash.

See [USAGE.md](USAGE.md) for API examples, expiration, maintenance, stable schema tags, and fetch-through caching.
