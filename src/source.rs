use std::{
    error::Error as StdError, future::Future, marker::PhantomData, pin::Pin, time::Duration,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CacheKey, Engram, Error, Result};

/// Metadata retained with a cached representation for future source validation.
///
/// `etag` and `last_modified_unix_ms` cover common HTTP validators. `opaque`
/// is application-defined bytes for non-HTTP sources or additional validators.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ValidationMetadata {
    /// An opaque entity tag supplied by an origin, such as an HTTP `ETag`.
    pub etag: Option<String>,
    /// An origin last-modified instant, expressed as Unix milliseconds.
    pub last_modified_unix_ms: Option<i64>,
    /// Application-defined validation state for custom source adapters.
    pub opaque: Vec<u8>,
}

/// The result of checking a cached representation with its origin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationOutcome {
    /// The origin confirmed the cached representation is still current.
    ///
    /// An adapter may return replacement validator metadata from a `304`-style
    /// response. `None` retains the previous metadata.
    Fresh {
        /// Updated validation metadata, if the origin supplied it.
        validation: Option<ValidationMetadata>,
    },
    /// The origin confirmed that the cached representation is outdated.
    Changed,
    /// The adapter cannot determine whether this representation is current.
    Unverifiable,
    /// The origin could not be contacted or could not complete validation.
    /// This is deliberately distinct from [`Self::Changed`].
    Unavailable,
}

/// Why Engram asked a source adapter to retrieve a representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetrievalReason {
    /// Neither cache tier held the resource.
    CacheMiss,
    /// Validation established that the cached representation changed.
    Changed,
    /// Validation could not establish freshness and policy requested a refill.
    Unverifiable,
    /// The caller explicitly requested a full refresh.
    Explicit,
}

/// A successful source retrieval and the validators to retain with it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Retrieved<T> {
    /// The representation to cache and return.
    pub value: T,
    /// Validators for future V1 checks.
    pub validation: Option<ValidationMetadata>,
}

/// The non-error outcomes of a source retrieval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetrievalOutcome<T> {
    /// The source returned a replacement representation.
    Retrieved(Retrieved<T>),
    /// The source authoritatively reports that the resource does not exist.
    NotFound,
    /// The returned representation failed source-specific integrity checks.
    Invalid {
        /// A consumer-safe explanation of the failed check.
        reason: String,
    },
}

/// A request made to a source adapter's validation implementation.
pub struct ValidationRequest<'a, T> {
    /// The logical cache key.
    pub key: &'a str,
    /// The cached, deserialized representation.
    pub value: &'a T,
    /// Validator metadata retained with this representation, if any.
    pub validation: Option<&'a ValidationMetadata>,
}

/// A request made to a source adapter's retrieval implementation.
pub struct RetrievalRequest<'a> {
    /// The logical cache key.
    pub key: &'a str,
    /// Existing validator metadata, if a cached representation was available.
    pub validation: Option<&'a ValidationMetadata>,
    /// The event that made retrieval necessary.
    pub reason: RetrievalReason,
}

/// A boxed future returned by a [`Source`] adapter.
pub type SourceFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Consumer-provided origin logic used by Engram's V1 and R1 pipeline.
///
/// Engram intentionally does not prescribe HTTP, a platform, authentication,
/// or a remote API. An adapter can use HTTP conditional requests, files,
/// object storage, or any other source. Return [`ValidationOutcome::Unavailable`]
/// for an origin outage when callers may choose to serve a stale value; return
/// `Err` for source failures that should be propagated.
pub trait Source<T>: Send + Sync {
    /// The adapter's transport or source-specific error type.
    type Error: StdError + Send + Sync + 'static;

    /// Performs a lightweight freshness check for a cached representation.
    fn validate<'a>(
        &'a self,
        request: ValidationRequest<'a, T>,
    ) -> SourceFuture<'a, std::result::Result<ValidationOutcome, Self::Error>>;

    /// Retrieves a complete representation from the origin.
    fn retrieve<'a>(
        &'a self,
        request: RetrievalRequest<'a>,
    ) -> SourceFuture<'a, std::result::Result<RetrievalOutcome<T>, Self::Error>>;
}

/// Controls when a cached value is revalidated before it is served.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ValidationPolicy {
    /// Serve a cached value without contacting its origin.
    #[default]
    Never,
    /// Validate every cache hit.
    Always,
    /// Validate only when the last successful validation is at least this old.
    After(Duration),
}

/// Consumer-controlled policy for [`crate::Engram::resolve`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveOptions {
    /// TTL assigned to a representation returned by R1. `None` is permanent.
    pub ttl: Option<Duration>,
    /// When V1 should contact the origin for an existing entry.
    pub validation: ValidationPolicy,
    /// Retrieve a full representation after [`ValidationOutcome::Unverifiable`].
    pub retrieve_on_unverifiable: bool,
    /// Return an existing value when V1 reports [`ValidationOutcome::Unavailable`].
    pub serve_stale_on_unavailable: bool,
    /// Skip V1 and invoke R1 even when a cached representation is present.
    pub force_retrieve: bool,
}

impl Default for ResolveOptions {
    fn default() -> Self {
        Self {
            ttl: None,
            validation: ValidationPolicy::Never,
            retrieve_on_unverifiable: false,
            serve_stale_on_unavailable: true,
            force_retrieve: false,
        }
    }
}

/// Declares the stable identity and default source policy for one cache namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceConfig {
    /// Canonical prefix prepended to every logical key in this namespace.
    pub name: String,
    /// Stable, application-defined schema tag for all values in this namespace.
    pub schema: String,
    /// Default V1/R1 policy used by [`Namespace::resolve`].
    pub resolve: ResolveOptions,
}

impl NamespaceConfig {
    /// Creates a namespace declaration with default resolution policy.
    pub fn new(name: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: schema.into(),
            resolve: ResolveOptions::default(),
        }
    }

    /// Assigns the policy used by [`Namespace::resolve`].
    pub fn resolve_options(mut self, options: ResolveOptions) -> Self {
        self.resolve = options;
        self
    }
}

/// A typed cache facade that makes keys, schema tags, and V1/R1 policy declarative.
#[derive(Clone)]
pub struct Namespace<T> {
    cache: Engram,
    config: NamespaceConfig,
    marker: PhantomData<fn() -> T>,
}

impl<T> Namespace<T> {
    pub(crate) fn new(cache: Engram, config: NamespaceConfig) -> Self {
        Self {
            cache,
            config,
            marker: PhantomData,
        }
    }

    /// Builds a canonical typed key from this namespace and a logical identifier.
    pub fn key(&self, id: impl AsRef<str>) -> CacheKey<T> {
        let name = self.config.name.trim_matches('/');
        let id = id.as_ref().trim_matches('/');
        CacheKey::new(format!("{name}/{id}"), self.config.schema.clone())
    }

    /// Returns this namespace's declaration.
    pub fn config(&self) -> &NamespaceConfig {
        &self.config
    }

    /// Reads a typed value from this namespace.
    pub async fn get(&self, id: impl AsRef<str>) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
    {
        self.cache.get_key(&self.key(id)).await
    }

    /// Stores a typed value using the namespace's default TTL.
    pub async fn set(&self, id: impl AsRef<str>, value: &T) -> Result<()>
    where
        T: serde::Serialize + Clone + Send + Sync + 'static,
    {
        self.cache
            .set_key(&self.key(id), value, self.config.resolve.ttl)
            .await
    }

    /// Resolves this namespace's key with its declared V1/R1 policy.
    pub async fn resolve<S>(
        &self,
        id: impl AsRef<str>,
        source: &S,
    ) -> std::result::Result<ResolveResult<T>, ResolveError<S::Error>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
        S: Source<T>,
    {
        self.cache
            .resolve(&self.key(id), self.config.resolve.clone(), source)
            .await
    }
}

/// How [`crate::Engram::resolve`] satisfied a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolveStatus {
    /// A cached value was served without V1 work.
    CacheHit,
    /// V1 confirmed that a cached value is fresh.
    ValidatedFresh,
    /// R1 retrieved and cached a representation.
    Retrieved,
    /// V1 could not establish freshness and policy allowed the cached value.
    StaleUnverifiable,
    /// V1 could not reach the origin and policy allowed the cached value.
    StaleUnavailable,
}

/// A value returned by [`crate::Engram::resolve`] and the path that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveResult<T> {
    /// The cached or retrieved representation.
    pub value: T,
    /// Whether it was cached, validated, retrieved, or served stale by policy.
    pub status: ResolveStatus,
}

/// Errors from the cache/source resolution pipeline.
#[derive(Debug, Error)]
pub enum ResolveError<E> {
    /// A cache serialization, persistence, or decoding operation failed.
    #[error(transparent)]
    Cache(#[from] Error),
    /// The source adapter returned an error.
    #[error("source operation failed: {0}")]
    Source(#[source] E),
    /// R1 reported that the requested resource does not exist.
    #[error("source reports that the resource does not exist")]
    NotFound,
    /// R1 rejected the representation before it could be cached.
    #[error("source returned an invalid representation: {0}")]
    Invalid(String),
    /// V1 was unavailable and policy did not permit serving the cached value.
    #[error("source validation is unavailable and stale serving is disabled")]
    Unavailable,
    /// The key changed repeatedly while V1/R1 was in flight.
    #[error("resolution was superseded repeatedly; retry the operation")]
    Conflict,
}
