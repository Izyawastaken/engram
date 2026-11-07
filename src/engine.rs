use std::{
    any::{type_name, Any},
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    compression,
    config::EngramBuilder,
    memory::{self, MemoryEntry},
    storage::{SqliteStore, StoredWrite},
    Compression, Error, Result,
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
}

impl CacheMetadata {
    /// Returns the remaining TTL. `None` means the entry is non-expiring.
    pub fn remaining_ttl(&self) -> Option<Duration> {
        self.expires_at
            .and_then(|expires_at| expires_at.duration_since(SystemTime::now()).ok())
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
    // Operations on the same key serialize, while unrelated keys proceed concurrently.
    key_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    in_flight: Arc<Mutex<HashMap<String, Arc<Flight>>>>,
}

struct Flight {
    notify: Arc<Notify>,
    completed: AtomicBool,
}

struct PendingWrite {
    write: StoredWrite,
    value: Arc<dyn Any + Send + Sync>,
    expires_at: Option<SystemTime>,
}

impl Engram {
    /// Starts configuration of an Engram cache.
    pub fn builder() -> EngramBuilder {
        EngramBuilder::new()
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
        if let Some(interval) = config.cleanup_interval {
            spawn_expiry_cleaner(
                Arc::downgrade(&storage),
                Arc::downgrade(&clear_gate),
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
            key_locks: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
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
            .map(|(value, _)| (*value).clone()))
    }

    /// Retrieves a value using a caller-controlled schema tag.
    pub async fn get_key<T>(&self, key: &CacheKey<T>) -> Result<Option<T>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.key(), key.schema())
            .await?
            .map(|(value, _)| (*value).clone()))
    }

    /// Retrieves a value with expiration/source metadata.
    pub async fn get_with_metadata<T>(&self, key: impl AsRef<str>) -> Result<Option<CacheEntry<T>>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.as_ref(), type_name::<T>())
            .await?
            .map(|(value, metadata)| CacheEntry {
                value: (*value).clone(),
                metadata,
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
            .map(|(value, metadata)| CacheEntry {
                value: (*value).clone(),
                metadata,
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
            .map(|(value, _)| value))
    }

    /// Retrieves a shared value using a caller-controlled schema tag.
    pub async fn get_arc_key<T>(&self, key: &CacheKey<T>) -> Result<Option<Arc<T>>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        Ok(self
            .get_arc_with_metadata::<T>(key.key(), key.schema())
            .await?
            .map(|(value, _)| value))
    }

    /// Returns expiry and source information without deserializing an L2 value.
    pub async fn metadata(&self, key: impl AsRef<str>) -> Result<Option<CacheMetadata>> {
        let key = key.as_ref();
        let _clear_guard = self.clear_gate.read().await;
        let key_lock = self.key_lock(key).await;
        let _key_guard = key_lock.lock().await;
        if let Some(entry) = self.memory.get(key).await {
            if !expired(entry.expires_at) {
                return Ok(Some(CacheMetadata {
                    expires_at: entry.expires_at,
                    tier: CacheTier::Memory,
                }));
            }
            self.memory.invalidate(key).await;
        }
        let Some(expires_at_ms) = self.storage.metadata(key).await? else {
            return Ok(None);
        };
        if expired_ms(expires_at_ms) {
            self.storage.remove(key).await?;
            return Ok(None);
        }
        Ok(Some(CacheMetadata {
            expires_at: expires_at_ms.map(system_time_from_ms),
            tier: CacheTier::Persistent,
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
                    type_name::<T>().to_owned(),
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
        let mut _key_guards = Vec::with_capacity(keys.len());
        for key in &keys {
            _key_guards.push(self.key_lock(key).await.lock_owned().await);
        }

        let writes: Vec<StoredWrite> = pending
            .iter()
            .map(|pending| pending.write.clone())
            .collect();
        self.storage.set_many(&writes).await?;
        for pending_write in &pending {
            self.insert_memory(pending_write).await;
        }
        Ok(())
    }

    /// Removes a key from both memory and persistent storage.
    pub async fn remove(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();
        let _clear_guard = self.clear_gate.read().await;
        let key_lock = self.key_lock(key).await;
        let _key_guard = key_lock.lock().await;
        self.memory.invalidate(key).await;
        self.storage.remove(key).await
    }

    /// Clears both cache tiers.
    pub async fn clear(&self) -> Result<()> {
        let _clear_guard = self.clear_gate.write().await;
        self.memory.invalidate_all();
        self.memory.run_pending_tasks().await;
        self.storage.clear().await
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
            let waiting_on = {
                let mut in_flight = self.in_flight.lock().await;
                if let Some(flight) = in_flight.get(&key) {
                    Some(flight.clone())
                } else {
                    in_flight.insert(
                        key.clone(),
                        Arc::new(Flight {
                            notify: Arc::new(Notify::new()),
                            completed: AtomicBool::new(false),
                        }),
                    );
                    None
                }
            };
            if let Some(flight) = waiting_on {
                let notified = flight.notify.clone().notified_owned();
                if !flight.completed.load(Ordering::Acquire) {
                    notified.await;
                }
                continue;
            }

            let result =
                init.take()
                    .expect("only the initialization leader invokes its closure")()
                .await;
            if let Ok(value) = &result {
                if let Err(error) = self.set(&key, value, ttl).await {
                    self.finish_initialization(&key).await;
                    return Err(error);
                }
            }
            self.finish_initialization(&key).await;
            return result;
        }
    }

    async fn get_arc_with_metadata<T>(
        &self,
        key: &str,
        type_tag: &str,
    ) -> Result<Option<(Arc<T>, CacheMetadata)>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let _clear_guard = self.clear_gate.read().await;
        let key_lock = self.key_lock(key).await;
        let _key_guard = key_lock.lock().await;
        if let Some(entry) = self.memory.get(key).await {
            if !expired(entry.expires_at) {
                if entry.type_name != type_tag {
                    return Err(Error::TypeMismatch {
                        key: key.to_owned(),
                        requested: type_tag.to_owned(),
                        actual: entry.type_name.clone(),
                    });
                }
                let value = downcast_arc(key, entry.value.clone(), &entry.type_name)?;
                return Ok(Some((
                    value,
                    CacheMetadata {
                        expires_at: entry.expires_at,
                        tier: CacheTier::Memory,
                    },
                )));
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
        let memory_value: Arc<dyn Any + Send + Sync> = value.clone();
        self.memory
            .insert(
                key.to_owned(),
                Arc::new(MemoryEntry {
                    value: memory_value,
                    type_name: type_tag.to_owned(),
                    expires_at,
                    weight: stored.memory_weight,
                }),
            )
            .await;
        Ok(Some((
            value,
            CacheMetadata {
                expires_at,
                tier: CacheTier::Persistent,
            },
        )))
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
        let pending = self
            .prepare_write(key.clone(), value, ttl, weight, type_tag)
            .await?;
        let _clear_guard = self.clear_gate.read().await;
        let key_lock = self.key_lock(&key).await;
        let _key_guard = key_lock.lock().await;
        self.insert_memory(&pending).await;
        if let Err(error) = self.storage.set(&pending.write).await {
            self.memory.invalidate(&key).await;
            return Err(error);
        }
        Ok(())
    }

    async fn prepare_write<T>(
        &self,
        key: String,
        value: Arc<T>,
        ttl: Option<Duration>,
        requested_weight: Option<u32>,
        type_tag: String,
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
            write: StoredWrite {
                key,
                bytes,
                expires_at_ms: expires_at.map(system_time_ms),
                encoding,
                type_name: type_tag,
                memory_weight: weight,
                created_at_ms: system_time_ms(SystemTime::now()),
            },
            value: memory_value,
            expires_at,
        })
    }

    async fn insert_memory(&self, pending: &PendingWrite) {
        let entry = Arc::new(MemoryEntry {
            value: pending.value.clone(),
            type_name: pending.write.type_name.clone(),
            expires_at: pending.expires_at,
            weight: pending.write.memory_weight,
        });
        self.memory.insert(pending.write.key.clone(), entry).await;
    }

    async fn key_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.key_locks.lock().await;
        if locks.len() >= 1024 {
            locks.retain(|_, lock| lock.strong_count() > 0);
        }
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key.to_owned(), Arc::downgrade(&lock));
        lock
    }

    async fn finish_initialization(&self, key: &str) {
        if let Some(flight) = self.in_flight.lock().await.remove(key) {
            flight.completed.store(true, Ordering::Release);
            flight.notify.notify_waiters();
        }
    }
}

fn spawn_expiry_cleaner(
    storage: Weak<SqliteStore>,
    clear_gate: Weak<RwLock<()>>,
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
            let _ = storage
                .purge_expired(system_time_ms(SystemTime::now()))
                .await;
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

fn expired(expires_at: Option<SystemTime>) -> bool {
    expires_at.is_some_and(|expires_at| expires_at <= SystemTime::now())
}

fn expired_ms(expires_at_ms: Option<i64>) -> bool {
    expires_at_ms.is_some_and(|expires_at| expires_at <= system_time_ms(SystemTime::now()))
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
        assert_eq!(
            cache.get::<String>("first").await.unwrap(),
            Some("first entry".to_owned())
        );
    }

    #[tokio::test]
    async fn clear_waits_for_an_active_key_operation() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        let clear_read_guard = cache.clear_gate.read().await;
        let key_lock = cache.key_lock("promoting").await;
        let key_guard = key_lock.lock().await;
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
}
