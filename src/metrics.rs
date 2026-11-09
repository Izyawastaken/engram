use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// A point-in-time snapshot of Engram's process-local counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheMetrics {
    /// Values returned directly from the deserialized Moka tier.
    pub l1_hits: u64,
    /// Values loaded from SQLite and promoted to memory.
    pub l2_hits: u64,
    /// Lookups for which neither tier held a usable value.
    pub misses: u64,
    /// Successful V1 freshness confirmations.
    pub validations_fresh: u64,
    /// V1 outcomes that required R1 replacement.
    pub validations_changed: u64,
    /// V1 outcomes that could not establish freshness.
    pub validations_unverifiable: u64,
    /// V1 origin outages.
    pub validations_unavailable: u64,
    /// R1 representations successfully cached.
    pub retrievals: u64,
    /// Cached values served while V1 could not establish freshness or reach origin.
    pub stale_served: u64,
    /// Number of source requests that joined already-running work for their key.
    pub resolution_coalesced: u64,
    /// Cumulative cache-lookup latency in nanoseconds.
    pub lookup_latency_ns: u64,
    /// Number of cache lookups represented by `lookup_latency_ns`.
    pub lookup_latency_samples: u64,
    /// Cumulative time spent serving L1 hits, in nanoseconds.
    pub l1_latency_ns: u64,
    /// Number of L1 hits represented by `l1_latency_ns`.
    pub l1_latency_samples: u64,
    /// Cumulative time spent loading L2 hits, in nanoseconds.
    pub l2_latency_ns: u64,
    /// Number of L2 hits represented by `l2_latency_ns`.
    pub l2_latency_samples: u64,
    /// Failures from the optional background expiry cleaner.
    pub cleanup_errors: u64,
}

#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) l1_hits: AtomicU64,
    pub(crate) l2_hits: AtomicU64,
    pub(crate) misses: AtomicU64,
    pub(crate) validations_fresh: AtomicU64,
    pub(crate) validations_changed: AtomicU64,
    pub(crate) validations_unverifiable: AtomicU64,
    pub(crate) validations_unavailable: AtomicU64,
    pub(crate) retrievals: AtomicU64,
    pub(crate) stale_served: AtomicU64,
    pub(crate) resolution_coalesced: AtomicU64,
    pub(crate) lookup_latency_ns: AtomicU64,
    pub(crate) lookup_latency_samples: AtomicU64,
    pub(crate) l1_latency_ns: AtomicU64,
    pub(crate) l1_latency_samples: AtomicU64,
    pub(crate) l2_latency_ns: AtomicU64,
    pub(crate) l2_latency_samples: AtomicU64,
    pub(crate) cleanup_errors: AtomicU64,
}

impl Metrics {
    pub(crate) fn snapshot(&self) -> CacheMetrics {
        CacheMetrics {
            l1_hits: self.l1_hits.load(Ordering::Relaxed),
            l2_hits: self.l2_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            validations_fresh: self.validations_fresh.load(Ordering::Relaxed),
            validations_changed: self.validations_changed.load(Ordering::Relaxed),
            validations_unverifiable: self.validations_unverifiable.load(Ordering::Relaxed),
            validations_unavailable: self.validations_unavailable.load(Ordering::Relaxed),
            retrievals: self.retrievals.load(Ordering::Relaxed),
            stale_served: self.stale_served.load(Ordering::Relaxed),
            resolution_coalesced: self.resolution_coalesced.load(Ordering::Relaxed),
            lookup_latency_ns: self.lookup_latency_ns.load(Ordering::Relaxed),
            lookup_latency_samples: self.lookup_latency_samples.load(Ordering::Relaxed),
            l1_latency_ns: self.l1_latency_ns.load(Ordering::Relaxed),
            l1_latency_samples: self.l1_latency_samples.load(Ordering::Relaxed),
            l2_latency_ns: self.l2_latency_ns.load(Ordering::Relaxed),
            l2_latency_samples: self.l2_latency_samples.load(Ordering::Relaxed),
            cleanup_errors: self.cleanup_errors.load(Ordering::Relaxed),
        }
    }
}

pub(crate) type SharedMetrics = Arc<Metrics>;
