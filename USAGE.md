# Using Engram

Engram is an async, application-independent cache. It keeps deserialized values in Moka (L1) and persists Serde-serialized, optionally Zstandard-compressed values in SQLite (L2). A successful write has committed its SQLite transaction to the WAL. The default `SqliteSynchronous::Full` asks SQLite for its strongest sync behavior; `Normal` is faster but a recent commit may be lost after a power failure or OS crash.

## Setup

```toml
[dependencies]
engram = { package = "engram-cache", version = "0.1" }
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

`set_many` locks its keys in sorted order and only updates L1 after that transaction commits. If the transaction fails, neither tier is changed by the batch.

L1 capacity is weighted by serialized byte length by default. When that does not approximate an object’s resident memory cost, provide an explicit weight; it is persisted and reused when the entry is restored from L2.

```rust
cache.set_with_weight("project:123", &project, None, 2 * 1024 * 1024).await?;
# Ok::<(), engram::Error>(())
```

### Buffered group commits

By default, a successful `set` waits for SQLite to commit. For write-heavy caches, choose `WriteDurability::Buffered`: Engram updates L1 immediately and its single dedicated SQLite writer drains queued writes in group transactions. This lets one `FULL` synchronization cover multiple writes, while read connections remain independent of the write connection.

Buffered writes may be lost if the process crashes before `flush()`. Call `flush()` during a controlled shutdown or before an operation that requires durable persistence. You can choose this globally or per write.

```rust
use engram::WriteDurability;

let cache = Engram::builder()
    .storage("./cache/engram.db")
    .write_durability(WriteDurability::Buffered)
    .build()
    .await?;

cache.set("project:123", &project, None).await?; // queued L2 write, immediate L1 update
cache.set_buffered("project:124", &project, None).await?;
cache.flush().await?; // waits for all prior buffered writes to commit
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

## V1 validation and R1 retrieval

V1 and R1 operate around L1/L2; they are not additional storage tiers. Implement `Source<T>` in your application to describe how a resource is checked and fetched. The adapter retains ownership of HTTP, authentication, Modrinth, files, or any other origin-specific behavior.

- V1 returns `Fresh`, `Changed`, `Unverifiable`, or `Unavailable`.
- R1 returns `Retrieved`, `NotFound`, or `Invalid`; adapter transport errors are returned separately.
- `ValidationMetadata` persists an ETag, a last-modified Unix timestamp, and application-defined opaque bytes in L2.
- An unavailable origin never deletes an existing value. `ResolveOptions` determines whether that value may be served stale.

Use `CacheKey<T>` for source-managed values so the persisted schema tag remains stable across Rust refactors.

```rust
use engram::{
    CacheKey, ResolveOptions, Source, SourceFuture, ValidationOutcome,
    ValidationPolicy, ValidationRequest, RetrievalOutcome, RetrievalRequest,
};

struct ProjectSource;

impl Source<Project> for ProjectSource {
    type Error = std::io::Error;

    fn validate<'a>(
        &'a self,
        request: ValidationRequest<'a, Project>,
    ) -> SourceFuture<'a, Result<ValidationOutcome, Self::Error>> {
        Box::pin(async move {
            // For HTTP, send request.validation's ETag as If-None-Match.
            let _ = request;
            Ok(ValidationOutcome::Fresh { validation: None })
        })
    }

    fn retrieve<'a>(
        &'a self,
        request: RetrievalRequest<'a>,
    ) -> SourceFuture<'a, Result<RetrievalOutcome<Project>, Self::Error>> {
        Box::pin(async move {
            let _ = request;
            Ok(RetrievalOutcome::NotFound)
        })
    }
}

let key = CacheKey::<Project>::new("project:123", "modrinth-project@1");
let source = ProjectSource;
let resolved = cache
    .resolve(
        &key,
        ResolveOptions {
            validation: ValidationPolicy::After(Duration::from_secs(300)),
            serve_stale_on_unavailable: true,
            ..ResolveOptions::default()
        },
        &source,
    )
    .await?;

println!("{:?}", resolved.status); // CacheHit, ValidatedFresh, Retrieved, or a stale status
# Ok::<(), Box<dyn std::error::Error>>(())
```

`ValidationPolicy::Never` is the default: a cache hit is served immediately. `Always` validates every hit. `After(duration)` validates only when no successful V1 check has occurred within the interval. Set `force_retrieve` for an explicit full refresh, and `retrieve_on_unverifiable` when your policy requires a complete download if V1 cannot determine freshness.

For prefetching or non-blocking refreshes, pass an `Arc<Source>` to `resolve_in_background`. Its `JoinHandle` can be aborted if a page or resource is no longer relevant; aborting before its commit does not populate the cache and releases coalesced callers. Limit concurrent tasks with your application's semaphore or scheduler.

## Namespaces and observability

Declare policy once per representation family instead of repeating TTLs, schema tags, and validation options at every call site. A namespace produces canonical `namespace/id` keys and supplies its own typed facade.

```rust
use engram::{NamespaceConfig, ResolveOptions, ValidationPolicy};

let versions = cache.namespace::<Vec<String>>(
    NamespaceConfig::new("modrinth-version-list", "modrinth-version-list@1")
        .resolve_options(ResolveOptions {
            ttl: Some(Duration::from_secs(15 * 60)),
            validation: ValidationPolicy::After(Duration::from_secs(5 * 60)),
            ..ResolveOptions::default()
        }),
);

versions.set("fabric/1.21.1", &vec!["1.0.0".to_owned()]).await?;
let metrics = cache.metrics();
println!("L1 hits: {}, L2 hits: {}", metrics.l1_hits, metrics.l2_hits);
# Ok::<(), engram::Error>(())
```

Metrics are process-local counters for L1/L2 hits, misses, validation outcomes, retrievals, stale serves, coalesced V1/R1 work, aggregate lookup latency, and per-tier hit latency. They are intended for application monitoring and benchmark evidence.

## Operational notes

- SQLite uses WAL mode, a five-second busy timeout, and a partial expiry index.
- Reads and origin I/O do not hold a per-key write lock. Concurrent V1/R1 resolutions for one key coalesce; a generation check makes a late origin response lose to a newer `set`, `remove`, or `clear`. `clear` gates the short snapshot and commit sections, empties both tiers, and makes an older source response return `ResolveError::Conflict` instead of resurrecting data. A caller may retry explicitly.
- String-key APIs use Rust's `type_name::<T>()` as the stored type tag. That protects against decoding the wrong in-process type, but a type rename or module move changes the tag. For persisted values that must survive releases, use `CacheKey` with an application-defined schema tag and only change that tag for an intentionally incompatible serialized format.
- `remove` deletes both tiers; `clear` deletes both tiers globally.

## Stable schema tags

`CacheKey<T>` separates the storage key from the stable schema identifier. This avoids tying durable cache compatibility to a Rust module path. The schema string is stored in L2 and also checked in L1.

```rust
use engram::CacheKey;

let project_key = CacheKey::<Project>::new("project:123", "modrinth-project@1");
cache.set_key(&project_key, &project, None).await?;
let cached: Option<Project> = cache.get_key(&project_key).await?;

// After an incompatible serialization change, use a new tag. Existing bytes
// return Error::TypeMismatch instead of being decoded as the new layout.
let next_project_key = CacheKey::<Project>::new("project:123", "modrinth-project@2");
# Ok::<(), engram::Error>(())
```
