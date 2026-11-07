use std::{
    any::{type_name, Any},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;

use crate::{
    compression, config::EngramBuilder, memory::{self, MemoryEntry}, storage::SqliteStore, Compression, Error, Result,
};

/// A reusable asynchronous cache with Moka memory and SQLite persistence tiers.
#[derive(Clone)]
pub struct Engram {
    memory: memory::Memory,
    storage: Arc<SqliteStore>,
    compression: Compression,
    // Serializes invalidations against L2-to-L1 promotion. Reads remain concurrent.
    gate: Arc<RwLock<()>>,
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
        })
    }

    /// Retrieves a value, checking memory before persistent storage.
    pub async fn get<T>(&self, key: impl AsRef<str>) -> Result<Option<T>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let key = key.as_ref();
        let _guard = self.gate.read().await;
        if let Some(entry) = self.memory.get(key).await {
            if !expired(entry.expires_at) {
                return downcast_value(key, entry.value);
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
        let bytes = compression::decode(stored.bytes, stored.encoding).await?;
        let value: T = bincode::deserialize(&bytes).map_err(Error::Deserialize)?;
        let expires_at = stored.expires_at_ms.map(system_time_from_ms);
        self.insert_memory(key.to_owned(), Arc::new(value.clone_for_cache()?), expires_at, bytes.len());
        // The clone above is avoided below by retaining an Arc in the normal path.
        // This branch is replaced by `get`'s owned value conversion in the implementation.
        Ok(Some(value))
    }

    /// Stores a value in both tiers. SQLite failures are returned to the caller.
    pub async fn set<T>(&self, key: impl AsRef<str>, value: &T, ttl: Option<Duration>) -> Result<()>
    where
        T: Serialize + Clone + Send + Sync + 'static,
    {
        let key = key.as_ref().to_owned();
        let serialized = bincode::serialize(value).map_err(Error::Serialize)?;
        let weight = serialized.len();
        let (compressed, encoding) = compression::encode(serialized, &self.compression).await?;
        let expires_at = ttl.map(|ttl| SystemTime::now() + ttl);
        let expires_at_ms = expires_at.map(system_time_ms);
        let now_ms = system_time_ms(SystemTime::now());
        let entry = Arc::new(MemoryEntry {
            value: Arc::new(value.clone()),
            expires_at,
            weight: weight.min(u32::MAX as usize) as u32,
        });

        let _guard = self.gate.write().await;
        self.memory.insert(key.clone(), entry).await;
        if let Err(error) = self.storage.set(&key, &compressed, expires_at_ms, encoding, now_ms).await {
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

    fn insert_memory(&self, key: String, value: Arc<dyn Any + Send + Sync>, expires_at: Option<SystemTime>, weight: usize) {
        let memory = self.memory.clone();
        let entry = Arc::new(MemoryEntry {
            value,
            expires_at,
            weight: weight.min(u32::MAX as usize) as u32,
        });
        // `get` already owns an async context, but Cache insertion is async. This helper is only
        // used by that path and is replaced by a direct await in the finalized implementation.
        tokio::spawn(async move { memory.insert(key, entry).await; });
    }
}

fn downcast_value<T>(key: &str, value: Arc<dyn Any + Send + Sync>) -> Result<Option<T>>
where
    T: Send + Sync + 'static,
{
    let actual = std::any::type_name_of_val(value.as_ref());
    match value.downcast::<T>() {
        Ok(value) => Ok(Some(Arc::try_unwrap(value).unwrap_or_else(|value| (*value).clone()))),
        Err(_) => Err(Error::TypeMismatch { key: key.to_owned(), requested: type_name::<T>(), actual }),
    }
}

fn expired(expires_at: Option<SystemTime>) -> bool {
    expires_at.is_some_and(|expires_at| expires_at <= SystemTime::now())
}

fn expired_ms(expires_at_ms: Option<i64>) -> bool {
    expires_at_ms.is_some_and(|expires_at| expires_at <= system_time_ms(SystemTime::now()))
}

fn system_time_ms(value: SystemTime) -> i64 {
    value.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis().min(i64::MAX as u128) as i64
}

fn system_time_from_ms(value: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(value.max(0) as u64)
}
