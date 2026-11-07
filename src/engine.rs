use std::{
    any::{type_name, Any},
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    compression,
    config::EngramBuilder,
    memory::{self, MemoryEntry},
    storage::SqliteStore,
    Compression, Error, Result,
};

/// A reusable asynchronous cache with Moka memory and SQLite persistence tiers.
/// `Engram` is cheap to clone; clones share memory, persistence, and synchronization.
#[derive(Clone)]
pub struct Engram {
    memory: memory::Memory,
    storage: Arc<SqliteStore>,
    compression: Compression,
    // Prevents L2 reads from promoting an entry while it is being invalidated.
    gate: Arc<RwLock<()>>,
    // Used only by the optional fetch-through helper.
    in_flight: Arc<Mutex<HashMap<String, Arc<Flight>>>>,
}

struct Flight {
    notify: Arc<Notify>,
    completed: AtomicBool,
}

impl Engram {
    /// Starts configuration of an Engram cache.
    pub fn builder() -> EngramBuilder {
        EngramBuilder::new()
    }

    pub(crate) async fn from_builder(config: EngramBuilder) -> Result<Self> {
        Ok(Self {
            memory: memory::new(config.memory_capacity),
            storage: Arc::new(SqliteStore::open(&config.storage, config.sqlite_connections).await?),
            compression: config.compression,
            gate: Arc::new(RwLock::new(())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Retrieves a value, checking Moka before SQLite.
    ///
    /// A missing or expired entry returns `Ok(None)`. The `Clone` bound lets L1 keep
    /// a deserialized owned value while returning an owned value to the caller.
    pub async fn get<T>(&self, key: impl AsRef<str>) -> Result<Option<T>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let key = key.as_ref();
        let _guard = self.gate.read().await;

        if let Some(entry) = self.memory.get(key).await {
            if !expired(entry.expires_at) {
                return downcast_value(key, entry.value, entry.type_name);
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
        if stored.type_name != type_name::<T>() {
            return Err(Error::TypeMismatch {
                key: key.to_owned(),
                requested: type_name::<T>(),
                actual: stored.type_name,
            });
        }

        let bytes = compression::decode(stored.bytes, stored.encoding).await?;
        let value: T = bincode::deserialize(&bytes).map_err(Error::Deserialize)?;
        let entry = Arc::new(MemoryEntry {
            value: Arc::new(value.clone()),
            type_name: type_name::<T>(),
            expires_at: stored.expires_at_ms.map(system_time_from_ms),
            weight: cache_weight(bytes.len()),
        });
        self.memory.insert(key.to_owned(), entry).await;
        Ok(Some(value))
    }

    /// Stores a value in both tiers.
    ///
    /// SQLite write failures remove the new L1 value and are returned to the caller.
    /// A successful call therefore always represents persisted data.
    pub async fn set<T>(&self, key: impl AsRef<str>, value: &T, ttl: Option<Duration>) -> Result<()>
    where
        T: Serialize + Clone + Send + Sync + 'static,
    {
        let key = key.as_ref().to_owned();
        let serialized = bincode::serialize(value).map_err(Error::Serialize)?;
        let weight = cache_weight(serialized.len());
        let (compressed, encoding) = compression::encode(serialized, &self.compression).await?;
        let expires_at = ttl.map(|ttl| SystemTime::now() + ttl);
        let expires_at_ms = expires_at.map(system_time_ms);
        let now_ms = system_time_ms(SystemTime::now());
        let entry = Arc::new(MemoryEntry {
            value: Arc::new(value.clone()),
            type_name: type_name::<T>(),
            expires_at,
            weight,
        });

        let _guard = self.gate.write().await;
        self.memory.insert(key.clone(), entry).await;
        if let Err(error) = self
            .storage
            .set(
                &key,
                &compressed,
                expires_at_ms,
                encoding,
                type_name::<T>(),
                now_ms,
            )
            .await
        {
            self.memory.invalidate(&key).await;
            return Err(error);
        }
        Ok(())
    }

    /// Removes a key from both memory and persistent storage.
    pub async fn remove(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();
        let _guard = self.gate.write().await;
        self.memory.invalidate(key).await;
        self.storage.remove(key).await
    }

    /// Clears both cache tiers.
    pub async fn clear(&self) -> Result<()> {
        let _guard = self.gate.write().await;
        self.memory.invalidate_all();
        self.memory.run_pending_tasks().await;
        self.storage.clear().await
    }

    /// Deletes expired SQLite rows and returns the number removed.
    ///
    /// L1 expiration is checked on every read. This explicit method leaves periodic
    /// cleanup policy to the host application instead of creating background work.
    pub async fn purge_expired(&self) -> Result<u64> {
        let _guard = self.gate.write().await;
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
                if let Some(notify) = in_flight.get(&key) {
                    Some(notify.clone())
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
                // Register interest before checking completion so a notification cannot
                // be missed between the check and the await.
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

    async fn finish_initialization(&self, key: &str) {
        if let Some(flight) = self.in_flight.lock().await.remove(key) {
            flight.completed.store(true, Ordering::Release);
            flight.notify.notify_waiters();
        }
    }
}

fn downcast_value<T>(
    key: &str,
    value: Arc<dyn Any + Send + Sync>,
    actual: &'static str,
) -> Result<Option<T>>
where
    T: Clone + Send + Sync + 'static,
{
    match value.downcast::<T>() {
        Ok(value) => Ok(Some((*value).clone())),
        Err(_) => Err(Error::TypeMismatch {
            key: key.to_owned(),
            requested: type_name::<T>(),
            actual: actual.to_owned(),
        }),
    }
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
    async fn l1_hit_needs_no_persistent_row() {
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
    async fn l2_hit_promotes_back_to_l1() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        cache.set("key", &99_u64, None).await.unwrap();
        cache.memory.invalidate("key").await;
        assert_eq!(cache.get::<u64>("key").await.unwrap(), Some(99));
        cache.storage.remove("key").await.unwrap();
        assert_eq!(cache.get::<u64>("key").await.unwrap(), Some(99));
    }

    #[tokio::test]
    async fn l1_eviction_does_not_delete_l2() {
        let directory = TempDir::new().unwrap();
        // Both values are larger than this L1 capacity and therefore evicted by Moka.
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
    async fn purging_expired_rows_reports_deletions() {
        let directory = TempDir::new().unwrap();
        let cache = cache(&directory, 1_024).await;
        cache
            .set(
                "expired",
                &"value".to_owned(),
                Some(Duration::from_millis(1)),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(cache.purge_expired().await.unwrap(), 1);
        assert_eq!(cache.get::<String>("expired").await.unwrap(), None);
    }
}
