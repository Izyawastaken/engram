use std::{any::Any, sync::Arc, time::SystemTime};

use moka::future::Cache;

pub(crate) struct MemoryEntry {
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) type_name: &'static str,
    pub(crate) expires_at: Option<SystemTime>,
    pub(crate) weight: u32,
}

pub(crate) type Memory = Cache<String, Arc<MemoryEntry>>;

pub(crate) fn new(capacity: u64) -> Memory {
    Cache::builder()
        .max_capacity(capacity)
        .weigher(|_, entry: &Arc<MemoryEntry>| entry.weight)
        .build()
}
