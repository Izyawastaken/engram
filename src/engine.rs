use std::{
    any::{type_name, Any},
    collections::HashMap,
    future::Future,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex, Weak,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    compression,
    config::EngramBuilder,
    memory::{self, MemoryEntry},
    metrics::{Metrics, SharedMetrics},
    storage::{SqliteStore, StoredWrite},
    writer::BufferedWriter,
    Compression, Error, ResolveError, ResolveOptions, ResolveResult, ResolveStatus, Result,
    RetrievalOutcome, RetrievalReason, RetrievalRequest, Source, ValidationMetadata,
    ValidationOutcome, ValidationPolicy, ValidationRequest, WriteDurability,
};

/// The tier that satisfied a cache read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheTier {
    Memory,
    Persistent,
}

/// Expiration and source information for a cached value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheMetadata {
    /// `None` means the value does not expire automatically.
    pub expires_at: Option<SystemTime>,
    /// The tier in which the entry was found for this lookup.
    pub tier: CacheTier,
    /// Source validation metadata retained with the representation, if any.
    pub validation: Option<ValidationMetadata>,
    /// When V1 most recently confirmed this representation's freshness.
    pub validated_at: Option<SystemTime>,
}

impl CacheMetadata {
    /// Returns the remaining TTL. `None` means the entry is non-expiring.
    pub fn remaining_ttl(&self) -> Option<Duration> {
        self.expires_at.map(|expires_at| {
            expires_at
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO)
        })
    }
}

/// A cached value returned together with its metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheEntry<T> {
    pub value: T,
    pub metadata: CacheMetadata,
}

/// A cache key with a caller-controlled, stable schema tag.
///
/// Use this when values are intended to survive releases. Keep `schema` stable
/// across Rust type/module renames, and change it when the serialized format is
/// intentionally incompatible.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CacheKey<T> {
    key: String,
    schema: String,
    marker: PhantomData<fn() -> T>,
}

impl<T> CacheKey<T> {
    /// Creates a key with a durable, application-defined schema tag.
    pub fn new(key: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            schema: schema.into(),
            marker: PhantomData,
        }
    }

    /// The storage key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The application-defined schema tag persisted with this key.
    pub fn schema(&self) -> &str {
        &self.schema
    }
}

/// A reusable asynchronous cache with Moka memory and SQLite persistence tiers.
/// `Engram` is cheap to clone; clones share memory, persistence, and synchronization.
#[derive(Clone)]
pub struct Engram {
    memory: memory::Memory,
    storage: Arc<SqliteStore>,
    compression: Compression,
    compression_threshold: usize,
    compress_only_if_smaller: bool,
    // `clear` takes this write lock; all other operations take a shared lock.
    clear_gate: Arc<RwLock<()>>,
    // Fixed stripes avoid per-key allocation and unbounded bookkeeping growth.
    key_locks: Arc<Vec<Mutex<()>>>,
    in_flight: FlightMap,
    resolving: FlightMap,
    generations: Arc<Vec<AtomicU64>>,
    clear_epoch: Arc<AtomicU64>,
    pending_writes: Arc<StdMutex<HashMap<String, PendingWrite>>>,
    pending_sequence: Arc<AtomicU64>,
    metrics: SharedMetrics,
    buffered_writer: Arc<BufferedWriter>,
    write_durability: WriteDurability,
}

struct Flight {
    notify: Arc<Notify>,
    completed: AtomicBool,
    published: AtomicBool,
}

type FlightMap = Arc<StdMutex<HashMap<String, Arc<Flight>>>>;

struct FlightGuard {
    map: FlightMap,
    key: String,
    flight: Arc<Flight>,
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        let mut map = self.map.lock().expect("flight map mutex poisoned");
        if map
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            map.remove(&self.key);
            self.flight.completed.store(true, Ordering::Release);
            self.flight.notify.notify_waiters();
        }
    }
}

#[derive(Clone)]
struct PendingWrite {
    id: u64,
    active: Arc<AtomicBool>,
    write: StoredWrite,
    value: Arc<dyn Any + Send + Sync>,
    expires_at: Option<SystemTime>,
    validation: Option<ValidationMetadata>,
    validated_at: Option<SystemTime>,
}

struct WriteDetails {
    type_tag: String,
    validation: Option<ValidationMetadata>,
    validated_at: Option<SystemTime>,
}

struct Loaded<T> {
    value: Arc<T>,
    metadata: CacheMetadata,
}

#[derive(Clone, Copy)]
struct Generation {
    clear_epoch: u64,
    key: u64,
}

enum ResolveAttempt<T> {
    Complete(ResolveResult<T>),
    Conflict,
}

enum FlightRole {
    Leader(FlightGuard),
    Follower(Arc<Flight>),
}

const STRIPES: usize = 256;
const MAX_RESOLVE_RETRIES: usize = 3;

impl Engram {
    /// Starts configuration of an Engram cache.
    pub fn builder() -> EngramBuilder {
        EngramBuilder::new()
    }

    /// Returns process-local hit, validation, retrieval, and latency counters.
    pub fn metrics(&self) -> crate::CacheMetrics {
        self.metrics.snapshot()
    }

    /// Creates a typed namespace with canonical keys and one declared policy.
    pub fn namespace<T>(&self, config: crate::NamespaceConfig) -> crate::Namespace<T> {
        crate::Namespace::new(self.clone(), config)
    }

    /// Waits until all previously buffered L2 writes have committed.
    pub async fn flush(&self) -> Result<()> {
        self.buffered_writer.flush().await
    }

    pub(crate) async fn from_builder(config: EngramBuilder) -> Result<Self> {
        let storage = Arc::new(
            SqliteStore::open(
                &config.storage,
                config.sqlite_connections,
                config.sqlite_synchronous,
            )
            .await?,
        );
        let clear_gate = Arc::new(RwLock::new(()));
        let buffered_writer = BufferedWriter::new(storage.clone());
        let metrics = Arc::new(Metrics::default());
        if let Some(interval) = config.cleanup_interval {
            spawn_expiry_cleaner(
                Arc::downgrade(&storage),
                Arc::downgrade(&clear_gate),
                Arc::downgrade(&metrics),
                interval,
            );
        }
        Ok(Self {
            memory: memory::new(config.memory_capacity),
            storage,
            compression: config.compression,
            compression_threshold: config.compression_threshold,
            compress_only_if_smaller: config.compress_only_if_smaller,
            clear_gate,
            key_locks: Arc::new((0..STRIPES).map(|_| Mutex::new(())).collect()),
            in_flight: Arc::new(StdMutex::new(HashMap::new())),
            resolving: Arc::new(StdMutex::new(HashMap::new())),
            generations: Arc::new((0..STRIPES).map(|_| AtomicU64::new(0)).collect()),
            clear_epoch: Arc::new(AtomicU64::new(0)),
            pending_writes: Arc::new(StdMutex::new(HashMap::new())),
            pending_sequence: Arc::new(AtomicU64::new(1)),
            metrics,
            buffered_writer,
            write_durability: config.write_durability,
        })
    }

    /// Retrieves a value, checking Moka before SQLite.
    pub async fn get<T>(&self, key: impl AsRef<str>) -> Result<Option<T>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.as_ref(), type_name::<T>())
            .await?
            .map(|loaded| (*loaded.value).clone()))
    }

    /// Retrieves a value using a caller-controlled schema tag.
    pub async fn get_key<T>(&self, key: &CacheKey<T>) -> Result<Option<T>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.key(), key.schema())
            .await?
            .map(|loaded| (*loaded.value).clone()))
    }

    /// Retrieves a value with expiration/source metadata.
    pub async fn get_with_metadata<T>(&self, key: impl AsRef<str>) -> Result<Option<CacheEntry<T>>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.as_ref(), type_name::<T>())
            .await?
            .map(|loaded| CacheEntry {
                value: (*loaded.value).clone(),
                metadata: loaded.metadata,
            }))
    }

    /// Retrieves a value and metadata using a caller-controlled schema tag.
    pub async fn get_key_with_metadata<T>(&self, key: &CacheKey<T>) -> Result<Option<CacheEntry<T>>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.key(), key.schema())
            .await?
            .map(|loaded| CacheEntry {
                value: (*loaded.value).clone(),
                metadata: loaded.metadata,
            }))
    }

    /// Retrieves a shared L1 value without cloning it. This is useful for large values.
    pub async fn get_arc<T>(&self, key: impl AsRef<str>) -> Result<Option<Arc<T>>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.as_ref(), type_name::<T>())
            .await?
            .map(|loaded| loaded.value))
    }

    /// Retrieves a shared value using a caller-controlled schema tag.
    pub async fn get_arc_key<T>(&self, key: &CacheKey<T>) -> Result<Option<Arc<T>>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.key(), key.schema())
            .await?
            .map(|loaded| loaded.value))
    }

    /// Returns expiry and source information without deserializing an L2 value.
    pub async fn metadata(&self, key: impl AsRef<str>) -> Result<Option<CacheMetadata>> {
        let key = key.as_ref();
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(key).lock().await;
        if let Some(entry) = self.memory.get(key).await {
            if !expired(entry.expires_at) {
                return Ok(Some(CacheMetadata {
                    expires_at: entry.expires_at,
                    tier: CacheTier::Memory,
                    validation: entry.validation.clone(),
                    validated_at: entry.validated_at,
                }));
            }
            self.memory.invalidate(key).await;
        }
        let Some(stored) = self.storage.get(key).await? else {
            return Ok(None);
        };
        if expired_ms(stored.expires_at_ms) {
            self.storage.remove(key).await?;
            return Ok(None);
        }
        Ok(Some(CacheMetadata {
            expires_at: stored.expires_at_ms.map(system_time_from_ms),
            tier: CacheTier::Persistent,
            validation: decode_validation(stored.validation_metadata)?,
            validated_at: stored.validated_at_ms.map(system_time_from_ms),
        }))
    }

    /// Stores a value in both tiers using serialized byte length as its L1 weight.
    pub async fn set<T>(&self, key: impl AsRef<str>, value: &T, ttl: Option<Duration>) -> Result<()>
    where
        T: Serialize + Clone + Send + Sync + 'static,
    {
        self.set_arc(key, Arc::new(value.clone()), ttl).await
    }

    /// Stores a shared value without cloning it for L1.
    pub async fn set_arc<T>(
        &self,
        key: impl AsRef<str>,
        value: Arc<T>,
        ttl: Option<Duration>,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.set_arc_inner(
            key.as_ref().to_owned(),
            value,
            ttl,
            None,
            type_name::<T>().to_owned(),
        )
        .await
    }

    /// Stores a value in L1 immediately and queues its L2 write for group commit.
    ///
    /// Buffered writes can be lost in a process crash before [`Self::flush`].
    pub async fn set_buffered<T>(
        &self,
        key: impl AsRef<str>,
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<()>
    where
        T: Serialize + Clone + Send + Sync + 'static,
    {
        self.set_arc_with_durability(key, Arc::new(value.clone()), ttl, WriteDurability::Buffered)
            .await
    }

    /// Buffered variant of [`Self::set_arc`].
    pub async fn set_arc_buffered<T>(
        &self,
        key: impl AsRef<str>,
        value: Arc<T>,
        ttl: Option<Duration>,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.set_arc_with_durability(key, value, ttl, WriteDurability::Buffered)
            .await
    }

    /// Stores an `Arc` with a per-write durability choice.
    pub async fn set_arc_with_durability<T>(
        &self,
        key: impl AsRef<str>,
        value: Arc<T>,
        ttl: Option<Duration>,
        durability: WriteDurability,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.set_arc_inner_with_durability(
            key.as_ref().to_owned(),
            value,
            ttl,
            None,
            type_name::<T>().to_owned(),
            durability,
        )
        .await
    }

    /// Stores a value with a caller-controlled schema tag.
    pub async fn set_key<T>(
        &self,
        key: &CacheKey<T>,
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<()>
    where
        T: Serialize + Clone + Send + Sync + 'static,
    {
        self.set_arc_key(key, Arc::new(value.clone()), ttl).await
    }

    /// Stores a shared value with a caller-controlled schema tag.
    pub async fn set_arc_key<T>(
        &self,
        key: &CacheKey<T>,
        value: Arc<T>,
        ttl: Option<Duration>,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.set_arc_inner(
            key.key().to_owned(),
            value,
            ttl,
            None,
            key.schema().to_owned(),
        )
        .await
    }

    /// Stores a value with an explicit L1 weight, useful when serialized size is not
    /// representative of its in-memory footprint.
    pub async fn set_with_weight<T>(
        &self,
        key: impl AsRef<str>,
        value: &T,
        ttl: Option<Duration>,
        weight: u32,
    ) -> Result<()>
    where
        T: Serialize + Clone + Send + Sync + 'static,
    {
        self.set_arc_inner(
            key.as_ref().to_owned(),
            Arc::new(value.clone()),
            ttl,
            Some(weight),
            type_name::<T>().to_owned(),
        )
        .await
    }

    /// Stores an `Arc` with an explicit L1 weight.
    pub async fn set_arc_with_weight<T>(
        &self,
        key: impl AsRef<str>,
        value: Arc<T>,
        ttl: Option<Duration>,
        weight: u32,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.set_arc_inner(
            key.as_ref().to_owned(),
            value,
            ttl,
            Some(weight),
            type_name::<T>().to_owned(),
        )
        .await
    }

    /// Persists many values in one SQLite transaction.
    pub async fn set_many<T, I, K>(&self, values: I, ttl: Option<Duration>) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
        I: IntoIterator<Item = (K, T)>,
        K: Into<String>,
    {
        let mut pending = Vec::new();
        for (key, value) in values {
            pending.push(
                self.prepare_write(
                    key.into(),
                    Arc::new(value),
                    ttl,
                    None,
                    WriteDetails {
                        type_tag: type_name::<T>().to_owned(),
                        validation: None,
                        validated_at: None,
                    },
                )
                .await?,
            );
        }
        if pending.is_empty() {
            return Ok(());
        }

        let _clear_guard = self.clear_gate.read().await;
        let mut keys: Vec<String> = pending
            .iter()
            .map(|pending| pending.write.key.clone())
            .collect();
        keys.sort_unstable();
        keys.dedup();
        let mut stripes: Vec<usize> = keys.iter().map(|key| stripe_index(key)).collect();
        stripes.sort_unstable();
        stripes.dedup();
        let mut _key_guards = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            _key_guards.push(self.key_locks[stripe].lock().await);
        }

        let writes: Vec<StoredWrite> = pending
            .iter()
            .map(|pending| pending.write.clone())
            .collect();
        for key in &keys {
            self.cancel_pending(key);
        }
        self.storage.set_many(&writes).await?;
        for pending_write in &pending {
            self.insert_memory(pending_write).await;
            self.bump_generation(&pending_write.write.key);
        }
        Ok(())
    }

    /// Removes a key from both memory and persistent storage.
    pub async fn remove(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(key).lock().await;
        self.cancel_pending(key);
        self.memory.invalidate(key).await;
        self.bump_generation(key);
        self.storage.remove(key).await
    }

    /// Clears both cache tiers.
    pub async fn clear(&self) -> Result<()> {
        let _clear_guard = self.clear_gate.write().await;
        // Odd epochs mean a clear is in progress. Lock-free L1 readers retry
        // instead of returning a value from the interval before invalidation.
        self.clear_epoch.fetch_add(1, Ordering::AcqRel);
        self.cancel_all_pending();
        self.memory.invalidate_all();
        self.memory.run_pending_tasks().await;
        let result = self.storage.clear().await;
        self.clear_epoch.fetch_add(1, Ordering::AcqRel);
        result
    }

    /// Deletes expired SQLite rows and returns the number removed.
    pub async fn purge_expired(&self) -> Result<u64> {
        let _clear_guard = self.clear_gate.read().await;
        self.storage
            .purge_expired(system_time_ms(SystemTime::now()))
            .await
    }

    /// Reads a key, calling `init` only on a miss and coalescing concurrent calls for
    /// the same key. A failed initialization is not cached.
    pub async fn get_or_try_init<T, F, Fut>(
        &self,
        key: impl AsRef<str>,
        ttl: Option<Duration>,
        init: F,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let key = key.as_ref().to_owned();
        let mut init = Some(init);
        loop {
            if let Some(value) = self.get(&key).await? {
                return Ok(value);
            }
            match self.join_flight(&self.in_flight, &key) {
                FlightRole::Follower(flight) => {
                    let notified = flight.notify.clone().notified_owned();
                    if !flight.completed.load(Ordering::Acquire) {
                        notified.await;
                    }
                }
                FlightRole::Leader(_guard) => {
                    let result = init
                        .take()
                        .expect("only the initialization leader invokes its closure")(
                    )
                    .await;
                    if let Ok(value) = &result {
                        self.set(&key, value, ttl).await?;
                    }
                    return result;
                }
            }
        }
    }

    /// Resolves a resource through the V1 validation and R1 retrieval pipeline.
    ///
    /// A cache miss invokes [`Source::retrieve`]. For a cached value,
    /// [`ResolveOptions`] decides whether [`Source::validate`] runs, whether an
    /// unverifiable value is refetched, and whether an unavailable origin may
    /// serve the existing value. The source adapter remains responsible for
    /// protocol, authentication, and timeouts.
    ///
    /// Source I/O runs without an Engram key lock. Concurrent calls for one key
    /// coalesce through an in-flight flight; a short generation-checked commit
    /// rejects a late response when another operation changed the key or called
    /// [`Self::clear`] while the source was in flight. Consequently ordinary
    /// reads remain responsive during a slow validation or retrieval.
    pub async fn resolve<T, S>(
        &self,
        key: &CacheKey<T>,
        options: ResolveOptions,
        source: &S,
    ) -> std::result::Result<ResolveResult<T>, ResolveError<S::Error>>
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        S: Source<T>,
    {
        for _ in 0..MAX_RESOLVE_RETRIES {
            let (cached, generation) = self.snapshot_loaded(key).await?;
            if let Some(cached) = &cached {
                if !options.force_retrieve
                    && !should_validate(options.validation, cached.metadata.validated_at)
                {
                    return Ok(ResolveResult {
                        value: (*cached.value).clone(),
                        status: ResolveStatus::CacheHit,
                    });
                }
            }

            match self.join_flight(&self.resolving, key.key()) {
                FlightRole::Follower(flight) => {
                    self.metrics
                        .resolution_coalesced
                        .fetch_add(1, Ordering::Relaxed);
                    let notified = flight.notify.clone().notified_owned();
                    if !flight.completed.load(Ordering::Acquire) {
                        notified.await;
                    }
                    if flight.published.load(Ordering::Acquire) {
                        if let Some(loaded) = self
                            .get_arc_with_metadata::<T>(key.key(), key.schema())
                            .await?
                        {
                            return Ok(ResolveResult {
                                value: (*loaded.value).clone(),
                                status: ResolveStatus::CacheHit,
                            });
                        }
                    }
                }
                FlightRole::Leader(guard) => {
                    let result = self
                        .resolve_leader(key, cached, generation, &options, source)
                        .await?;
                    match result {
                        ResolveAttempt::Complete(result) => {
                            guard.flight.published.store(true, Ordering::Release);
                            return Ok(result);
                        }
                        ResolveAttempt::Conflict => return Err(ResolveError::Conflict),
                    }
                }
            }
        }
        Err(ResolveError::Conflict)
    }

    async fn resolve_leader<T, S>(
        &self,
        key: &CacheKey<T>,
        cached: Option<Loaded<T>>,
        generation: Generation,
        options: &ResolveOptions,
        source: &S,
    ) -> std::result::Result<ResolveAttempt<T>, ResolveError<S::Error>>
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        S: Source<T>,
    {
        if options.force_retrieve {
            return self
                .retrieve_optimistic(
                    key,
                    cached,
                    generation,
                    options,
                    source,
                    RetrievalReason::Explicit,
                )
                .await;
        }
        let Some(cached) = cached else {
            return self
                .retrieve_optimistic(
                    key,
                    None,
                    generation,
                    options,
                    source,
                    RetrievalReason::CacheMiss,
                )
                .await;
        };
        if !should_validate(options.validation, cached.metadata.validated_at) {
            return Ok(ResolveAttempt::Complete(ResolveResult {
                value: (*cached.value).clone(),
                status: ResolveStatus::CacheHit,
            }));
        }
        match source
            .validate(ValidationRequest {
                key: key.key(),
                value: cached.value.as_ref(),
                validation: cached.metadata.validation.as_ref(),
            })
            .await
            .map_err(ResolveError::Source)?
        {
            ValidationOutcome::Fresh { validation } => {
                let validation = validation.or(cached.metadata.validation.clone());
                if self
                    .commit_validation_if_current(key.key(), generation, validation)
                    .await?
                {
                    self.metrics
                        .validations_fresh
                        .fetch_add(1, Ordering::Relaxed);
                    Ok(ResolveAttempt::Complete(ResolveResult {
                        value: (*cached.value).clone(),
                        status: ResolveStatus::ValidatedFresh,
                    }))
                } else {
                    self.current_after_conflict(key).await.map_err(Into::into)
                }
            }
            ValidationOutcome::Changed => {
                self.metrics
                    .validations_changed
                    .fetch_add(1, Ordering::Relaxed);
                self.retrieve_optimistic(
                    key,
                    Some(cached),
                    generation,
                    options,
                    source,
                    RetrievalReason::Changed,
                )
                .await
            }
            ValidationOutcome::Unverifiable if options.retrieve_on_unverifiable => {
                self.metrics
                    .validations_unverifiable
                    .fetch_add(1, Ordering::Relaxed);
                self.retrieve_optimistic(
                    key,
                    Some(cached),
                    generation,
                    options,
                    source,
                    RetrievalReason::Unverifiable,
                )
                .await
            }
            ValidationOutcome::Unverifiable => {
                self.metrics
                    .validations_unverifiable
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                Ok(ResolveAttempt::Complete(ResolveResult {
                    value: (*cached.value).clone(),
                    status: ResolveStatus::StaleUnverifiable,
                }))
            }
            ValidationOutcome::Unavailable if options.serve_stale_on_unavailable => {
                self.metrics
                    .validations_unavailable
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                Ok(ResolveAttempt::Complete(ResolveResult {
                    value: (*cached.value).clone(),
                    status: ResolveStatus::StaleUnavailable,
                }))
            }
            ValidationOutcome::Unavailable => {
                self.metrics
                    .validations_unavailable
                    .fetch_add(1, Ordering::Relaxed);
                Err(ResolveError::Unavailable)
            }
        }
    }

    /// Starts [`Self::resolve`] without blocking the caller.
    ///
    /// Abort the returned handle when a prefetch is no longer useful. Consumers
    /// can apply their own semaphore around calls to bound background work.
    pub fn resolve_in_background<T, S>(
        &self,
        key: CacheKey<T>,
        options: ResolveOptions,
        source: Arc<S>,
    ) -> tokio::task::JoinHandle<std::result::Result<ResolveResult<T>, ResolveError<S::Error>>>
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        S: Source<T> + 'static,
    {
        let cache = self.clone();
        tokio::spawn(async move { cache.resolve(&key, options, source.as_ref()).await })
    }

    async fn retrieve_optimistic<T, S>(
        &self,
        key: &CacheKey<T>,
        cached: Option<Loaded<T>>,
        generation: Generation,
        options: &ResolveOptions,
        source: &S,
        reason: RetrievalReason,
    ) -> std::result::Result<ResolveAttempt<T>, ResolveError<S::Error>>
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        S: Source<T>,
    {
        let validation = cached
            .as_ref()
            .and_then(|cached| cached.metadata.validation.as_ref());
        match source
            .retrieve(RetrievalRequest {
                key: key.key(),
                validation,
                reason,
            })
            .await
            .map_err(ResolveError::Source)?
        {
            RetrievalOutcome::Retrieved(retrieved) => {
                let value = Arc::new(retrieved.value);
                if self
                    .commit_retrieved_if_current(
                        key,
                        generation,
                        value.clone(),
                        options.ttl,
                        retrieved.validation,
                    )
                    .await?
                {
                    self.metrics.retrievals.fetch_add(1, Ordering::Relaxed);
                    Ok(ResolveAttempt::Complete(ResolveResult {
                        value: (*value).clone(),
                        status: ResolveStatus::Retrieved,
                    }))
                } else {
                    self.current_after_conflict(key).await.map_err(Into::into)
                }
            }
            RetrievalOutcome::NotFound => {
                if cached.is_some() {
                    let _ = self.remove_if_current(key.key(), generation).await?;
                }
                Err(ResolveError::NotFound)
            }
            RetrievalOutcome::Invalid { reason } => Err(ResolveError::Invalid(reason)),
        }
    }

    async fn commit_retrieved_if_current<T>(
        &self,
        key: &CacheKey<T>,
        generation: Generation,
        value: Arc<T>,
        ttl: Option<Duration>,
        validation: Option<ValidationMetadata>,
    ) -> Result<bool>
    where
        T: Serialize + Send + Sync + 'static,
    {
        // Serialization and compression are intentionally outside the short
        // generation-checked commit section.
        let pending = self
            .prepare_write(
                key.key().to_owned(),
                value,
                ttl,
                None,
                WriteDetails {
                    type_tag: key.schema().to_owned(),
                    validation,
                    validated_at: Some(SystemTime::now()),
                },
            )
            .await?;
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(key.key()).lock().await;
        if !self.generation_matches(key.key(), generation) {
            return Ok(false);
        }
        self.cancel_pending(key.key());
        self.insert_memory(&pending).await;
        if let Err(error) = self.storage.set(&pending.write).await {
            self.memory.invalidate(key.key()).await;
            return Err(error);
        }
        self.bump_generation(key.key());
        Ok(true)
    }

    async fn remove_if_current(&self, key: &str, generation: Generation) -> Result<bool> {
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(key).lock().await;
        if !self.generation_matches(key, generation) {
            return Ok(false);
        }
        self.cancel_pending(key);
        self.memory.invalidate(key).await;
        self.storage.remove(key).await?;
        self.bump_generation(key);
        Ok(true)
    }

    async fn current_after_conflict<T>(&self, key: &CacheKey<T>) -> Result<ResolveAttempt<T>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Ok(
            match self
                .get_arc_with_metadata::<T>(key.key(), key.schema())
                .await?
            {
                Some(loaded) => ResolveAttempt::Complete(ResolveResult {
                    value: (*loaded.value).clone(),
                    status: ResolveStatus::CacheHit,
                }),
                None => ResolveAttempt::Conflict,
            },
        )
    }

    async fn commit_validation_if_current(
        &self,
        key: &str,
        generation: Generation,
        validation: Option<ValidationMetadata>,
    ) -> Result<bool> {
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(key).lock().await;
        if !self.generation_matches(key, generation) {
            return Ok(false);
        }
        let validated_at = SystemTime::now();
        let encoded = encode_validation(&validation)?;
        self.cancel_pending(key);
        self.storage
            .update_validation(key, encoded.as_deref(), system_time_ms(validated_at))
            .await?;
        if let Some(entry) = self.memory.get(key).await {
            self.memory
                .insert(
                    key.to_owned(),
                    Arc::new(MemoryEntry {
                        value: entry.value.clone(),
                        type_name: entry.type_name.clone(),
                        expires_at: entry.expires_at,
                        weight: entry.weight,
                        validation,
                        validated_at: Some(validated_at),
                    }),
                )
                .await;
        }
        self.bump_generation(key);
        Ok(true)
    }

    async fn get_arc_with_metadata<T>(&self, key: &str, type_tag: &str) -> Result<Option<Loaded<T>>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let started = Instant::now();
        let epoch_before = self.clear_epoch.load(Ordering::Acquire);
        if epoch_before % 2 == 0 {
            if let Some(loaded) = self.memory_loaded(key, type_tag).await? {
                if self.clear_epoch.load(Ordering::Acquire) == epoch_before {
                    self.record_lookup_latency(started);
                    return Ok(Some(loaded));
                }
            }
            if let Some(loaded) = self.pending_loaded(key, type_tag)? {
                if self.clear_epoch.load(Ordering::Acquire) == epoch_before {
                    self.record_lookup_latency(started);
                    return Ok(Some(loaded));
                }
            }
        }
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(key).lock().await;
        let result = self.get_arc_with_metadata_locked(key, type_tag).await;
        self.record_lookup_latency(started);
        result
    }

    fn record_lookup_latency(&self, started: Instant) {
        self.metrics.lookup_latency_ns.fetch_add(
            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .lookup_latency_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    async fn memory_loaded<T>(&self, key: &str, type_tag: &str) -> Result<Option<Loaded<T>>>
    where
        T: Send + Sync + 'static,
    {
        let tier_started = Instant::now();
        let Some(entry) = self.memory.get(key).await else {
            return Ok(None);
        };
        if expired(entry.expires_at) {
            self.memory.invalidate(key).await;
            return Ok(None);
        }
        if entry.type_name != type_tag {
            return Err(Error::TypeMismatch {
                key: key.to_owned(),
                requested: type_tag.to_owned(),
                actual: entry.type_name.clone(),
            });
        }
        let value = downcast_arc(key, entry.value.clone(), &entry.type_name)?;
        self.metrics.l1_hits.fetch_add(1, Ordering::Relaxed);
        self.metrics.l1_latency_ns.fetch_add(
            tier_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .l1_latency_samples
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(Loaded {
            value,
            metadata: CacheMetadata {
                expires_at: entry.expires_at,
                tier: CacheTier::Memory,
                validation: entry.validation.clone(),
                validated_at: entry.validated_at,
            },
        }))
    }

    async fn get_arc_with_metadata_locked<T>(
        &self,
        key: &str,
        type_tag: &str,
    ) -> Result<Option<Loaded<T>>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let tier_started = Instant::now();
        if let Some(loaded) = self.memory_loaded(key, type_tag).await? {
            return Ok(Some(loaded));
        }
        if let Some(loaded) = self.pending_loaded(key, type_tag)? {
            return Ok(Some(loaded));
        }

        let Some(stored) = self.storage.get(key).await? else {
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if expired_ms(stored.expires_at_ms) {
            self.storage.remove(key).await?;
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        if stored.type_name != type_tag {
            return Err(Error::TypeMismatch {
                key: key.to_owned(),
                requested: type_tag.to_owned(),
                actual: stored.type_name,
            });
        }
        let bytes = compression::decode(stored.bytes, stored.encoding).await?;
        let value = Arc::new(bincode::deserialize::<T>(&bytes).map_err(Error::Deserialize)?);
        let expires_at = stored.expires_at_ms.map(system_time_from_ms);
        let validation = decode_validation(stored.validation_metadata)?;
        let validated_at = stored.validated_at_ms.map(system_time_from_ms);
        let memory_value: Arc<dyn Any + Send + Sync> = value.clone();
        self.memory
            .insert(
                key.to_owned(),
                Arc::new(MemoryEntry {
                    value: memory_value,
                    type_name: type_tag.to_owned(),
                    expires_at,
                    weight: stored.memory_weight,
                    validation: validation.clone(),
                    validated_at,
                }),
            )
            .await;
        self.metrics.l2_hits.fetch_add(1, Ordering::Relaxed);
        self.metrics.l2_latency_ns.fetch_add(
            tier_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .l2_latency_samples
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(Loaded {
            value,
            metadata: CacheMetadata {
                expires_at,
                tier: CacheTier::Persistent,
                validation,
                validated_at,
            },
        }))
    }

    async fn set_arc_inner<T>(
        &self,
        key: String,
        value: Arc<T>,
        ttl: Option<Duration>,
        weight: Option<u32>,
        type_tag: String,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.set_arc_inner_with_durability(key, value, ttl, weight, type_tag, self.write_durability)
            .await
    }

    async fn set_arc_inner_with_durability<T>(
        &self,
        key: String,
        value: Arc<T>,
        ttl: Option<Duration>,
        weight: Option<u32>,
        type_tag: String,
        durability: WriteDurability,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let pending = self
            .prepare_write(
                key.clone(),
                value,
                ttl,
                weight,
                WriteDetails {
                    type_tag,
                    validation: None,
                    validated_at: None,
                },
            )
            .await?;
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(&key).lock().await;
        self.insert_memory(&pending).await;
        match durability {
            WriteDurability::Synchronous => {
                self.cancel_pending(&key);
                if let Err(error) = self.storage.set(&pending.write).await {
                    self.memory.invalidate(&key).await;
                    return Err(error);
                }
            }
            WriteDurability::Buffered => {
                self.activate_pending(pending.clone());
                let receiver = self
                    .buffered_writer
                    .enqueue(pending.write.clone(), pending.active.clone())?;
                self.watch_buffered_write(key.clone(), pending.clone(), receiver);
            }
        }
        self.bump_generation(&key);
        Ok(())
    }

    async fn prepare_write<T>(
        &self,
        key: String,
        value: Arc<T>,
        ttl: Option<Duration>,
        requested_weight: Option<u32>,
        details: WriteDetails,
    ) -> Result<PendingWrite>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let serialized = bincode::serialize(value.as_ref()).map_err(Error::Serialize)?;
        let weight = requested_weight
            .unwrap_or_else(|| cache_weight(serialized.len()))
            .max(1);
        let (bytes, encoding) = compression::encode(
            serialized,
            &self.compression,
            self.compression_threshold,
            self.compress_only_if_smaller,
        )
        .await?;
        let expires_at = ttl.map(|ttl| SystemTime::now() + ttl);
        let memory_value: Arc<dyn Any + Send + Sync> = value;
        Ok(PendingWrite {
            id: self.pending_sequence.fetch_add(1, Ordering::Relaxed),
            active: Arc::new(AtomicBool::new(true)),
            write: StoredWrite {
                key,
                bytes,
                expires_at_ms: expires_at.map(system_time_ms),
                encoding,
                type_name: details.type_tag,
                memory_weight: weight,
                created_at_ms: system_time_ms(SystemTime::now()),
                validation_metadata: encode_validation(&details.validation)?,
                validated_at_ms: details.validated_at.map(system_time_ms),
            },
            value: memory_value,
            expires_at,
            validation: details.validation,
            validated_at: details.validated_at,
        })
    }

    async fn insert_memory(&self, pending: &PendingWrite) {
        let entry = Arc::new(MemoryEntry {
            value: pending.value.clone(),
            type_name: pending.write.type_name.clone(),
            expires_at: pending.expires_at,
            weight: pending.write.memory_weight,
            validation: pending.validation.clone(),
            validated_at: pending.validated_at,
        });
        self.memory.insert(pending.write.key.clone(), entry).await;
    }

    fn activate_pending(&self, pending: PendingWrite) {
        let mut writes = self
            .pending_writes
            .lock()
            .expect("pending-write mutex poisoned");
        if let Some(previous) = writes.insert(pending.write.key.clone(), pending) {
            previous.active.store(false, Ordering::Release);
        }
    }

    fn cancel_pending(&self, key: &str) {
        if let Some(previous) = self
            .pending_writes
            .lock()
            .expect("pending-write mutex poisoned")
            .remove(key)
        {
            previous.active.store(false, Ordering::Release);
        }
    }

    fn cancel_all_pending(&self) {
        let mut writes = self
            .pending_writes
            .lock()
            .expect("pending-write mutex poisoned");
        for pending in writes.values() {
            pending.active.store(false, Ordering::Release);
        }
        writes.clear();
    }

    fn pending_loaded<T>(&self, key: &str, type_tag: &str) -> Result<Option<Loaded<T>>>
    where
        T: Send + Sync + 'static,
    {
        let pending = self
            .pending_writes
            .lock()
            .expect("pending-write mutex poisoned")
            .get(key)
            .cloned();
        let Some(pending) = pending else {
            return Ok(None);
        };
        if expired(pending.expires_at) {
            return Ok(None);
        }
        if pending.write.type_name != type_tag {
            return Err(Error::TypeMismatch {
                key: key.to_owned(),
                requested: type_tag.to_owned(),
                actual: pending.write.type_name,
            });
        }
        let value = downcast_arc(key, pending.value, type_tag)?;
        Ok(Some(Loaded {
            value,
            metadata: CacheMetadata {
                expires_at: pending.expires_at,
                tier: CacheTier::Memory,
                validation: pending.validation,
                validated_at: pending.validated_at,
            },
        }))
    }

    fn watch_buffered_write(
        &self,
        key: String,
        pending: PendingWrite,
        receiver: tokio::sync::oneshot::Receiver<std::result::Result<(), String>>,
    ) {
        let writes = self.pending_writes.clone();
        tokio::spawn(async move {
            if receiver.await.is_ok_and(|result| result.is_ok()) {
                let mut writes = writes.lock().expect("pending-write mutex poisoned");
                if writes
                    .get(&key)
                    .is_some_and(|current| current.id == pending.id)
                {
                    writes.remove(&key);
                }
            }
        });
    }

    fn key_lock(&self, key: &str) -> &Mutex<()> {
        &self.key_locks[stripe_index(key)]
    }

    async fn snapshot_loaded<T>(&self, key: &CacheKey<T>) -> Result<(Option<Loaded<T>>, Generation)>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let _clear_guard = self.clear_gate.read().await;
        let _key_guard = self.key_lock(key.key()).lock().await;
        let loaded = self
            .get_arc_with_metadata_locked(key.key(), key.schema())
            .await?;
        let key_generation = self.generations[stripe_index(key.key())].load(Ordering::Acquire);
        Ok((
            loaded,
            Generation {
                clear_epoch: self.clear_epoch.load(Ordering::Acquire),
                key: key_generation,
            },
        ))
    }

    fn generation_matches(&self, key: &str, generation: Generation) -> bool {
        self.clear_epoch.load(Ordering::Acquire) == generation.clear_epoch
            && self.generations[stripe_index(key)].load(Ordering::Acquire) == generation.key
    }

    fn bump_generation(&self, key: &str) {
        self.generations[stripe_index(key)].fetch_add(1, Ordering::AcqRel);
    }

    fn join_flight(&self, map: &FlightMap, key: &str) -> FlightRole {
        let mut entries = map.lock().expect("flight map mutex poisoned");
        if let Some(flight) = entries.get(key) {
            return FlightRole::Follower(flight.clone());
        }
        let flight = Arc::new(Flight {
            notify: Arc::new(Notify::new()),
            completed: AtomicBool::new(false),
            published: AtomicBool::new(false),
        });
        entries.insert(key.to_owned(), flight.clone());
        FlightRole::Leader(FlightGuard {
            map: map.clone(),
            key: key.to_owned(),
            flight,
        })
    }
}

fn spawn_expiry_cleaner(
    storage: Weak<SqliteStore>,
    clear_gate: Weak<RwLock<()>>,
    metrics: Weak<Metrics>,
    interval: Duration,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let (Some(storage), Some(clear_gate)) = (storage.upgrade(), clear_gate.upgrade())
            else {
                return;
            };
            let _clear_guard = clear_gate.read().await;
            if storage
                .purge_expired(system_time_ms(SystemTime::now()))
                .await
                .is_err()
            {
                if let Some(metrics) = metrics.upgrade() {
                    metrics.cleanup_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
}

fn downcast_arc<T>(key: &str, value: Arc<dyn Any + Send + Sync>, actual: &str) -> Result<Arc<T>>
where
    T: Send + Sync + 'static,
{
    value.downcast::<T>().map_err(|_| Error::TypeMismatch {
        key: key.to_owned(),
        requested: type_name::<T>().to_owned(),
        actual: actual.to_owned(),
    })
}

fn cache_weight(serialized_len: usize) -> u32 {
    serialized_len.clamp(1, u32::MAX as usize) as u32
}

fn stripe_index(key: &str) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % STRIPES
}

fn expired(expires_at: Option<SystemTime>) -> bool {
    expires_at.is_some_and(|expires_at| expires_at <= SystemTime::now())
}

fn expired_ms(expires_at_ms: Option<i64>) -> bool {
    expires_at_ms.is_some_and(|expires_at| expires_at <= system_time_ms(SystemTime::now()))
}

fn should_validate(policy: ValidationPolicy, validated_at: Option<SystemTime>) -> bool {
    match policy {
        ValidationPolicy::Never => false,
        ValidationPolicy::Always => true,
        ValidationPolicy::After(interval) => validated_at
            .and_then(|validated_at| SystemTime::now().duration_since(validated_at).ok())
            .map_or(true, |elapsed| elapsed >= interval),
    }
}

fn system_time_ms(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn system_time_from_ms(value: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(value.max(0) as u64)
}

fn encode_validation(validation: &Option<ValidationMetadata>) -> Result<Option<Vec<u8>>> {
    validation
        .as_ref()
        .map(|validation| bincode::serialize(validation).map_err(Error::Serialize))
        .transpose()
}

fn decode_validation(bytes: Option<Vec<u8>>) -> Result<Option<ValidationMetadata>> {
    bytes
        .map(|bytes| bincode::deserialize(&bytes).map_err(Error::Deserialize))
        .transpose()
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    async fn cache(directory: &TempDir, capacity: u64) -> Engram {
        Engram::builder()
            .storage(directory.path().join("engram.db"))
            .memory_capacity(capacity)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn l1_hit_does_not_need_a_persistent_row() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        cache
            .set("key", &"memory value".to_owned(), None)
            .await
            .unwrap();
        cache.storage.remove("key").await.unwrap();
        assert_eq!(
            cache.get::<String>("key").await.unwrap(),
            Some("memory value".to_owned())
        );
    }

    #[tokio::test]
    async fn l2_hit_promotes_an_entry_back_to_l1() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        cache.set("key", &99_u64, None).await.unwrap();
        cache.memory.invalidate("key").await;
        assert_eq!(cache.get::<u64>("key").await.unwrap(), Some(99));
        cache.storage.remove("key").await.unwrap();
        assert_eq!(cache.get::<u64>("key").await.unwrap(), Some(99));
    }

    #[tokio::test]
    async fn l1_eviction_never_removes_l2() {
        let directory = TempDir::new().unwrap();
        // Each value exceeds L1's capacity and is evicted by Moka.
        let cache = cache(&directory, 1).await;
        cache
            .set("first", &"first entry".to_owned(), None)
            .await
            .unwrap();
        cache
            .set("second", &"second entry".to_owned(), None)
            .await
            .unwrap();
        cache.memory.run_pending_tasks().await;
        let loaded = cache
            .get_with_metadata::<String>("first")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.value, "first entry");
        assert_eq!(loaded.metadata.tier, CacheTier::Persistent);
    }

    #[tokio::test]
    async fn clear_waits_for_an_active_key_operation() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        let clear_read_guard = cache.clear_gate.read().await;
        let key_guard = cache.key_lock("promoting").lock().await;
        let clearing_cache = cache.clone();
        let clearing = tokio::spawn(async move { clearing_cache.clear().await });

        tokio::task::yield_now().await;
        assert!(
            !clearing.is_finished(),
            "clear must wait until the operation-wide gate is released"
        );

        drop(key_guard);
        drop(clear_read_guard);
        clearing.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_batch_does_not_partially_update_l1() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        cache.set("good", &"old".to_owned(), None).await.unwrap();
        cache.storage.abort_writes_for_test().await.unwrap();

        assert!(cache
            .set_many(
                [("good", "new".to_owned()), ("bad", "new".to_owned())],
                None,
            )
            .await
            .is_err());
        assert_eq!(
            cache.get::<String>("good").await.unwrap(),
            Some("old".to_owned())
        );
        assert_eq!(cache.get::<String>("bad").await.unwrap(), None);
    }

    #[tokio::test]
    async fn set_many_deduplicates_colliding_stripe_locks() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        let first = "collision:0".to_owned();
        let second = (1_u64..)
            .map(|index| format!("collision:{index}"))
            .find(|candidate| stripe_index(candidate) == stripe_index(&first))
            .expect("a stripe collision must exist");
        tokio::time::timeout(
            Duration::from_secs(1),
            cache.set_many(
                [
                    (first.clone(), "first".to_owned()),
                    (second.clone(), "second".to_owned()),
                ],
                None,
            ),
        )
        .await
        .expect("a colliding stripe must not self-deadlock")
        .unwrap();
        assert_eq!(
            cache.get::<String>(&first).await.unwrap(),
            Some("first".to_owned())
        );
        assert_eq!(
            cache.get::<String>(&second).await.unwrap(),
            Some("second".to_owned())
        );
    }
}
