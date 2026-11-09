use std::{
    any::Any,
    sync::Arc,
    time::{Duration, SystemTime},
};

use moka::{future::Cache, Expiry};

use crate::ValidationMetadata;

pub(crate) struct MemoryEntry {
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) type_name: String,
    pub(crate) expires_at: Option<SystemTime>,
    pub(crate) weight: u32,
    pub(crate) validation: Option<ValidationMetadata>,
    pub(crate) validated_at: Option<SystemTime>,
}

pub(crate) type Memory = Cache<String, Arc<MemoryEntry>>;

struct EntryExpiry;

impl Expiry<String, Arc<MemoryEntry>> for EntryExpiry {
    fn expire_after_create(
        &self,
        _: &String,
        entry: &Arc<MemoryEntry>,
        _: std::time::Instant,
    ) -> Option<Duration> {
        entry.expires_at.map(remaining)
    }

    fn expire_after_update(
        &self,
        _: &String,
        entry: &Arc<MemoryEntry>,
        _: std::time::Instant,
        _: Option<Duration>,
    ) -> Option<Duration> {
        entry.expires_at.map(remaining)
    }
}

fn remaining(expires_at: SystemTime) -> Duration {
    expires_at
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO)
}

pub(crate) fn new(capacity: u64) -> Memory {
    Cache::builder()
        .max_capacity(capacity)
        .weigher(|_, entry: &Arc<MemoryEntry>| entry.weight)
        .expire_after(EntryExpiry)
        .build()
}
