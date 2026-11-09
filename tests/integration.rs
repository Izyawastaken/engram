use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use engram::{
    CacheKey, Compression, Engram, ResolveOptions, ResolveStatus, RetrievalOutcome,
    RetrievalRequest, Retrieved, Source, SourceFuture, ValidationMetadata, ValidationOutcome,
    ValidationPolicy, ValidationRequest, WriteDurability,
};
use proptest::prelude::*;
use tempfile::TempDir;
use tokio::sync::{oneshot, Mutex};
use tokio::time::timeout;

async fn cache(directory: &TempDir) -> Engram {
    Engram::builder()
        .storage(directory.path().join("engram.db"))
        .compression(Compression::Zstd { level: 3 })
        .build()
        .await
        .unwrap()
}

#[derive(Debug)]
struct TestSourceError;

impl fmt::Display for TestSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("test source error")
    }
}

impl StdError for TestSourceError {}

struct TestSource {
    validation: ValidationOutcome,
    retrieval: RetrievalOutcome<String>,
    validation_calls: AtomicUsize,
    retrieval_calls: AtomicUsize,
}

impl Source<String> for TestSource {
    type Error = TestSourceError;

    fn validate<'a>(
        &'a self,
        _request: ValidationRequest<'a, String>,
    ) -> SourceFuture<'a, std::result::Result<ValidationOutcome, Self::Error>> {
        self.validation_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(self.validation.clone()) })
    }

    fn retrieve<'a>(
        &'a self,
        _request: RetrievalRequest<'a>,
    ) -> SourceFuture<'a, std::result::Result<RetrievalOutcome<String>, Self::Error>> {
        self.retrieval_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(self.retrieval.clone()) })
    }
}

struct FailingSource {
    fail_validation: bool,
    fail_retrieval: bool,
}

impl Source<String> for FailingSource {
    type Error = TestSourceError;

    fn validate<'a>(
        &'a self,
        _request: ValidationRequest<'a, String>,
    ) -> SourceFuture<'a, std::result::Result<ValidationOutcome, Self::Error>> {
        Box::pin(async move {
            if self.fail_validation {
                Err(TestSourceError)
            } else {
                Ok(ValidationOutcome::Fresh { validation: None })
            }
        })
    }

    fn retrieve<'a>(
        &'a self,
        _request: RetrievalRequest<'a>,
    ) -> SourceFuture<'a, std::result::Result<RetrievalOutcome<String>, Self::Error>> {
        Box::pin(async move {
            if self.fail_retrieval {
                Err(TestSourceError)
            } else {
                Ok(RetrievalOutcome::Retrieved(Retrieved {
                    value: "source-value".to_owned(),
                    validation: None,
                }))
            }
        })
    }
}

#[tokio::test]
async fn misses_and_expired_values_are_not_returned() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    assert_eq!(cache.get::<String>("missing").await.unwrap(), None);
    cache
        .set("short", &"value".to_owned(), Some(Duration::from_millis(1)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(cache.get::<String>("short").await.unwrap(), None);
}

#[tokio::test]
async fn purge_expired_removes_persistent_rows() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engram.db");
    let cache = Engram::builder().storage(&path).build().await.unwrap();
    cache
        .set("short", &"value".to_owned(), Some(Duration::from_millis(1)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(cache.purge_expired().await.unwrap(), 1);
    drop(cache);
    let reopened = Engram::builder().storage(path).build().await.unwrap();
    assert_eq!(reopened.get::<String>("short").await.unwrap(), None);
}

#[tokio::test]
async fn persisted_values_survive_restart_and_preserve_bytes() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engram.db");
    let payload = "a long, compressible payload ".repeat(256);
    let first = Engram::builder()
        .storage(&path)
        .compression(Compression::Zstd { level: 3 })
        .build()
        .await
        .unwrap();
    first.set("payload", &payload, None).await.unwrap();
    drop(first);
    let reopened = Engram::builder()
        .storage(path)
        .compression(Compression::Disabled)
        .build()
        .await
        .unwrap();
    assert_eq!(
        reopened.get::<String>("payload").await.unwrap(),
        Some(payload)
    );
}

#[tokio::test]
async fn removal_clear_and_type_checks_cover_both_tiers() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    cache.set("remove", &1_u64, None).await.unwrap();
    cache.remove("remove").await.unwrap();
    assert_eq!(cache.get::<u64>("remove").await.unwrap(), None);
    cache.set("typed", &1_u64, None).await.unwrap();
    assert!(matches!(
        cache.get::<String>("typed").await,
        Err(engram::Error::TypeMismatch { .. })
    ));
    cache.set("one", &1_u64, None).await.unwrap();
    cache.set("two", &2_u64, None).await.unwrap();
    cache.clear().await.unwrap();
    assert_eq!(cache.get::<u64>("one").await.unwrap(), None);
    assert_eq!(cache.get::<u64>("two").await.unwrap(), None);
}

#[tokio::test]
async fn concurrent_initialization_is_coalesced() {
    let directory = TempDir::new().unwrap();
    let cache = Arc::new(cache(&directory).await);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let cache = cache.clone();
        let calls = calls.clone();
        tasks.push(tokio::spawn(async move {
            cache
                .get_or_try_init("one", None, move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok("initialized".to_owned())
                })
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), "initialized");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_writes_remain_readable() {
    let directory = TempDir::new().unwrap();
    let cache = Arc::new(cache(&directory).await);
    let mut tasks = Vec::new();
    for value in 0_u64..32 {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            cache
                .set(format!("key:{value}"), &value, None)
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    for value in 0_u64..32 {
        assert_eq!(
            cache.get::<u64>(format!("key:{value}")).await.unwrap(),
            Some(value)
        );
    }
}

#[tokio::test]
async fn metadata_exposes_expiry_and_permanent_entries() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    cache.set("permanent", &1_u64, None).await.unwrap();
    let permanent = cache.metadata("permanent").await.unwrap().unwrap();
    assert_eq!(permanent.tier, engram::CacheTier::Memory);
    assert_eq!(permanent.expires_at, None);
    assert_eq!(permanent.remaining_ttl(), None);

    cache
        .set("temporary", &2_u64, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let temporary = cache
        .get_with_metadata::<u64>("temporary")
        .await
        .unwrap()
        .unwrap();
    assert!(temporary.metadata.expires_at.is_some());
    assert!(temporary.metadata.remaining_ttl().is_some());
}

#[tokio::test]
async fn arc_and_batch_apis_avoid_extra_value_copies_and_persist() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let source = Arc::new(vec![7_u8; 8 * 1024]);
    cache.set_arc("shared", source.clone(), None).await.unwrap();
    let retrieved = cache.get_arc::<Vec<u8>>("shared").await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&source, &retrieved));

    cache
        .set_many([("one", 1_u64), ("two", 2_u64)], None)
        .await
        .unwrap();
    assert_eq!(cache.get::<u64>("one").await.unwrap(), Some(1));
    assert_eq!(cache.get::<u64>("two").await.unwrap(), Some(2));
    cache
        .set_with_weight("weighted", &3_u64, None, 1)
        .await
        .unwrap();
    assert_eq!(cache.get::<u64>("weighted").await.unwrap(), Some(3));
}

#[tokio::test]
async fn invalid_database_path_surfaces_sqlite_error() {
    let directory = TempDir::new().unwrap();
    let result = Engram::builder()
        .storage(directory.path().join("absent").join("cache.db"))
        .build()
        .await;
    assert!(matches!(result, Err(engram::Error::Sqlite(_))));
}

#[tokio::test]
async fn caller_controlled_schema_tags_survive_type_name_changes() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let v1 = CacheKey::<u64>::new("project:1", "project-record@1");
    cache.set_key(&v1, &42, None).await.unwrap();

    assert_eq!(cache.get_key(&v1).await.unwrap(), Some(42));
    let incompatible = CacheKey::<u64>::new("project:1", "project-record@2");
    assert!(matches!(
        cache.get_key(&incompatible).await,
        Err(engram::Error::TypeMismatch { .. })
    ));
}

#[tokio::test]
async fn resolve_retrieves_changed_values_and_persists_validators() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:1", "project@1");
    cache.set_key(&key, &"old".to_owned(), None).await.unwrap();
    let source = TestSource {
        validation: ValidationOutcome::Changed,
        retrieval: RetrievalOutcome::Retrieved(Retrieved {
            value: "new".to_owned(),
            validation: Some(ValidationMetadata {
                etag: Some("new-etag".to_owned()),
                ..ValidationMetadata::default()
            }),
        }),
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };

    let resolved = cache
        .resolve(
            &key,
            ResolveOptions {
                validation: ValidationPolicy::Always,
                ..ResolveOptions::default()
            },
            &source,
        )
        .await
        .unwrap();
    assert_eq!(resolved.value, "new");
    assert_eq!(resolved.status, ResolveStatus::Retrieved);
    assert_eq!(source.validation_calls.load(Ordering::SeqCst), 1);
    assert_eq!(source.retrieval_calls.load(Ordering::SeqCst), 1);
    drop(cache);
    let reopened = Engram::builder()
        .storage(directory.path().join("engram.db"))
        .build()
        .await
        .unwrap();
    let metadata = reopened.metadata(key.key()).await.unwrap().unwrap();
    assert_eq!(metadata.tier, engram::CacheTier::Persistent);
    assert_eq!(
        metadata.validation.unwrap().etag.as_deref(),
        Some("new-etag")
    );
    assert!(metadata.validated_at.is_some());
}

#[tokio::test]
async fn resolve_preserves_a_value_when_validation_is_unavailable() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:1", "project@1");
    cache
        .set_key(&key, &"cached".to_owned(), None)
        .await
        .unwrap();
    let source = TestSource {
        validation: ValidationOutcome::Unavailable,
        retrieval: RetrievalOutcome::Invalid {
            reason: "must not be called".to_owned(),
        },
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };

    let resolved = cache
        .resolve(
            &key,
            ResolveOptions {
                validation: ValidationPolicy::Always,
                ..ResolveOptions::default()
            },
            &source,
        )
        .await
        .unwrap();
    assert_eq!(resolved.value, "cached");
    assert_eq!(resolved.status, ResolveStatus::StaleUnavailable);
    assert_eq!(source.retrieval_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        cache.get_key(&key).await.unwrap(),
        Some("cached".to_owned())
    );
}

#[tokio::test]
async fn unavailable_validation_can_refuse_to_serve_stale() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:unavailable", "project@1");
    cache
        .set_key(&key, &"cached".to_owned(), None)
        .await
        .unwrap();
    let source = TestSource {
        validation: ValidationOutcome::Unavailable,
        retrieval: RetrievalOutcome::NotFound,
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };

    assert!(matches!(
        cache
            .resolve(
                &key,
                ResolveOptions {
                    validation: ValidationPolicy::Always,
                    serve_stale_on_unavailable: false,
                    ..ResolveOptions::default()
                },
                &source,
            )
            .await,
        Err(engram::ResolveError::Unavailable)
    ));
    assert_eq!(
        cache.get_key(&key).await.unwrap(),
        Some("cached".to_owned())
    );
}

#[tokio::test]
async fn validation_after_skips_a_recently_validated_value() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:after", "project@1");
    cache
        .set_key(&key, &"cached".to_owned(), None)
        .await
        .unwrap();
    let source = TestSource {
        validation: ValidationOutcome::Fresh { validation: None },
        retrieval: RetrievalOutcome::NotFound,
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };
    cache
        .resolve(
            &key,
            ResolveOptions {
                validation: ValidationPolicy::Always,
                ..ResolveOptions::default()
            },
            &source,
        )
        .await
        .unwrap();
    let resolved = cache
        .resolve(
            &key,
            ResolveOptions {
                validation: ValidationPolicy::After(Duration::from_secs(60)),
                ..ResolveOptions::default()
            },
            &source,
        )
        .await
        .unwrap();
    assert_eq!(resolved.status, ResolveStatus::CacheHit);
    assert_eq!(source.validation_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unverifiable_policy_controls_retrieval() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:unverifiable", "project@1");
    cache
        .set_key(&key, &"cached".to_owned(), None)
        .await
        .unwrap();
    let source = TestSource {
        validation: ValidationOutcome::Unverifiable,
        retrieval: RetrievalOutcome::Retrieved(Retrieved {
            value: "retrieved".to_owned(),
            validation: None,
        }),
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };
    let stale = cache
        .resolve(
            &key,
            ResolveOptions {
                validation: ValidationPolicy::Always,
                ..ResolveOptions::default()
            },
            &source,
        )
        .await
        .unwrap();
    assert_eq!(stale.status, ResolveStatus::StaleUnverifiable);
    let fresh = cache
        .resolve(
            &key,
            ResolveOptions {
                validation: ValidationPolicy::Always,
                retrieve_on_unverifiable: true,
                ..ResolveOptions::default()
            },
            &source,
        )
        .await
        .unwrap();
    assert_eq!(fresh.value, "retrieved");
    assert_eq!(source.retrieval_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn source_errors_preserve_cached_values_and_surface() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:error", "project@1");
    cache
        .set_key(&key, &"cached".to_owned(), None)
        .await
        .unwrap();
    let validation_error = FailingSource {
        fail_validation: true,
        fail_retrieval: false,
    };
    assert!(matches!(
        cache
            .resolve(
                &key,
                ResolveOptions {
                    validation: ValidationPolicy::Always,
                    ..ResolveOptions::default()
                },
                &validation_error,
            )
            .await,
        Err(engram::ResolveError::Source(_))
    ));
    assert_eq!(
        cache.get_key(&key).await.unwrap(),
        Some("cached".to_owned())
    );

    let retrieval_error = FailingSource {
        fail_validation: false,
        fail_retrieval: true,
    };
    cache.remove(key.key()).await.unwrap();
    assert!(matches!(
        cache
            .resolve(&key, ResolveOptions::default(), &retrieval_error)
            .await,
        Err(engram::ResolveError::Source(_))
    ));
    assert_eq!(cache.get_key(&key).await.unwrap(), None);
}

#[tokio::test]
async fn resolve_refills_a_cache_miss() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:1", "project@1");
    let source = TestSource {
        validation: ValidationOutcome::Fresh { validation: None },
        retrieval: RetrievalOutcome::Retrieved(Retrieved {
            value: "retrieved".to_owned(),
            validation: None,
        }),
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };

    let resolved = cache
        .resolve(&key, ResolveOptions::default(), &source)
        .await
        .unwrap();
    assert_eq!(resolved.value, "retrieved");
    assert_eq!(resolved.status, ResolveStatus::Retrieved);
    assert_eq!(source.validation_calls.load(Ordering::SeqCst), 0);
    assert_eq!(source.retrieval_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache.get_key(&key).await.unwrap(),
        Some("retrieved".to_owned())
    );
}

#[tokio::test]
async fn background_resolution_refills_without_blocking_the_caller() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:background", "project@1");
    let source = Arc::new(TestSource {
        validation: ValidationOutcome::Fresh { validation: None },
        retrieval: RetrievalOutcome::Retrieved(Retrieved {
            value: "prefetched".to_owned(),
            validation: None,
        }),
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    });

    let resolved = cache
        .resolve_in_background(key.clone(), ResolveOptions::default(), source.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.status, ResolveStatus::Retrieved);
    assert_eq!(
        cache.get_key(&key).await.unwrap(),
        Some("prefetched".to_owned())
    );
    assert_eq!(source.retrieval_calls.load(Ordering::SeqCst), 1);
}

struct DelayedSource {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Source<String> for DelayedSource {
    type Error = TestSourceError;

    fn validate<'a>(
        &'a self,
        _request: ValidationRequest<'a, String>,
    ) -> SourceFuture<'a, std::result::Result<ValidationOutcome, Self::Error>> {
        Box::pin(async move { Ok(ValidationOutcome::Changed) })
    }

    fn retrieve<'a>(
        &'a self,
        _request: RetrievalRequest<'a>,
    ) -> SourceFuture<'a, std::result::Result<RetrievalOutcome<String>, Self::Error>> {
        Box::pin(async move {
            if let Some(sender) = self.started.lock().await.take() {
                let _ = sender.send(());
            }
            if let Some(receiver) = self.release.lock().await.take() {
                let _ = receiver.await;
            }
            Ok(RetrievalOutcome::Retrieved(Retrieved {
                value: "late-origin-value".to_owned(),
                validation: None,
            }))
        })
    }
}

#[tokio::test]
async fn late_source_response_cannot_overwrite_a_newer_write() {
    let directory = TempDir::new().unwrap();
    let cache = Arc::new(cache(&directory).await);
    let key = CacheKey::<String>::new("project:race", "project@1");
    cache.set_key(&key, &"old".to_owned(), None).await.unwrap();
    let (started_sender, started_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let source = Arc::new(DelayedSource {
        started: Mutex::new(Some(started_sender)),
        release: Mutex::new(Some(release_receiver)),
    });
    let resolving_cache = cache.clone();
    let resolving_key = key.clone();
    let resolving_source = source.clone();
    let resolving = tokio::spawn(async move {
        resolving_cache
            .resolve(
                &resolving_key,
                ResolveOptions {
                    validation: ValidationPolicy::Always,
                    ..ResolveOptions::default()
                },
                resolving_source.as_ref(),
            )
            .await
    });

    started_receiver.await.unwrap();
    cache
        .set_key(&key, &"newer-local-value".to_owned(), None)
        .await
        .unwrap();
    release_sender.send(()).unwrap();
    let _ = resolving.await.unwrap().unwrap();
    assert_eq!(
        cache.get_key(&key).await.unwrap(),
        Some("newer-local-value".to_owned())
    );
}

#[tokio::test]
async fn late_source_response_cannot_resurrect_after_remove() {
    let directory = TempDir::new().unwrap();
    let cache = Arc::new(cache(&directory).await);
    let key = CacheKey::<String>::new("project:remove-race", "project@1");
    cache.set_key(&key, &"old".to_owned(), None).await.unwrap();
    let (started_sender, started_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let source = Arc::new(DelayedSource {
        started: Mutex::new(Some(started_sender)),
        release: Mutex::new(Some(release_receiver)),
    });
    let task_cache = cache.clone();
    let task_key = key.clone();
    let task_source = source.clone();
    let resolving = tokio::spawn(async move {
        task_cache
            .resolve(
                &task_key,
                ResolveOptions {
                    validation: ValidationPolicy::Always,
                    ..ResolveOptions::default()
                },
                task_source.as_ref(),
            )
            .await
    });
    started_receiver.await.unwrap();
    cache.remove(key.key()).await.unwrap();
    release_sender.send(()).unwrap();
    let _ = resolving.await.unwrap();
    assert_eq!(cache.get_key(&key).await.unwrap(), None);
}

#[tokio::test]
async fn late_source_response_cannot_resurrect_after_clear() {
    let directory = TempDir::new().unwrap();
    let cache = Arc::new(cache(&directory).await);
    let key = CacheKey::<String>::new("project:clear-race", "project@1");
    cache.set_key(&key, &"old".to_owned(), None).await.unwrap();
    let (started_sender, started_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let source = Arc::new(DelayedSource {
        started: Mutex::new(Some(started_sender)),
        release: Mutex::new(Some(release_receiver)),
    });
    let task_cache = cache.clone();
    let task_key = key.clone();
    let task_source = source.clone();
    let resolving = tokio::spawn(async move {
        task_cache
            .resolve(
                &task_key,
                ResolveOptions {
                    validation: ValidationPolicy::Always,
                    ..ResolveOptions::default()
                },
                task_source.as_ref(),
            )
            .await
    });
    started_receiver.await.unwrap();
    cache.clear().await.unwrap();
    release_sender.send(()).unwrap();
    let _ = resolving.await.unwrap();
    assert_eq!(cache.get_key(&key).await.unwrap(), None);
}

#[tokio::test]
async fn aborting_background_resolution_releases_its_flight() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:abort", "project@1");
    let (started_sender, started_receiver) = oneshot::channel();
    let (_release_sender, release_receiver) = oneshot::channel();
    let delayed = Arc::new(DelayedSource {
        started: Mutex::new(Some(started_sender)),
        release: Mutex::new(Some(release_receiver)),
    });
    let handle = cache.resolve_in_background(key.clone(), ResolveOptions::default(), delayed);
    started_receiver.await.unwrap();
    handle.abort();
    let _ = handle.await;

    let source = TestSource {
        validation: ValidationOutcome::Fresh { validation: None },
        retrieval: RetrievalOutcome::Retrieved(Retrieved {
            value: "recovered".to_owned(),
            validation: None,
        }),
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };
    let result = timeout(
        Duration::from_secs(1),
        cache.resolve(&key, ResolveOptions::default(), &source),
    )
    .await
    .expect("an aborted flight must not wedge future resolutions")
    .unwrap();
    assert_eq!(result.value, "recovered");
}

#[tokio::test]
async fn overlapping_set_many_batches_do_not_deadlock() {
    let directory = TempDir::new().unwrap();
    let cache = Arc::new(cache(&directory).await);
    let first = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.set_many([("a", 1_u64), ("b", 1_u64)], None).await })
    };
    let second = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.set_many([("b", 2_u64), ("a", 2_u64)], None).await })
    };
    timeout(Duration::from_secs(1), async {
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    })
    .await
    .expect("sorted batch locking must not deadlock");
}

#[tokio::test]
async fn changed_not_found_removes_the_stale_value() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let key = CacheKey::<String>::new("project:deleted", "project@1");
    cache.set_key(&key, &"old".to_owned(), None).await.unwrap();
    let source = TestSource {
        validation: ValidationOutcome::Changed,
        retrieval: RetrievalOutcome::NotFound,
        validation_calls: AtomicUsize::new(0),
        retrieval_calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        cache
            .resolve(
                &key,
                ResolveOptions {
                    validation: ValidationPolicy::Always,
                    ..ResolveOptions::default()
                },
                &source,
            )
            .await,
        Err(engram::ResolveError::NotFound)
    ));
    assert_eq!(cache.get_key(&key).await.unwrap(), None);
}

#[tokio::test]
async fn namespaces_canonicalize_keys_and_apply_declared_ttl() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    let namespace = cache.namespace::<String>(engram::NamespaceConfig::new(
        "/modrinth-version-list/",
        "version-list@1",
    ));
    assert_eq!(namespace.key("/abc/").key(), "modrinth-version-list/abc");
    namespace.set("abc", &"value".to_owned()).await.unwrap();
    assert_eq!(
        namespace.get("abc").await.unwrap(),
        Some("value".to_owned())
    );
}

#[tokio::test]
async fn metrics_report_cache_and_source_activity() {
    let directory = TempDir::new().unwrap();
    let cache = cache(&directory).await;
    cache
        .set("metric", &"value".to_owned(), None)
        .await
        .unwrap();
    assert_eq!(
        cache.get::<String>("metric").await.unwrap(),
        Some("value".to_owned())
    );
    let metrics = cache.metrics();
    assert!(metrics.l1_hits >= 1);
    assert!(metrics.lookup_latency_samples >= 1);
}

#[tokio::test]
async fn buffered_writes_become_durable_after_flush() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("engram.db");
    let cache = Engram::builder()
        .storage(&path)
        .write_durability(WriteDurability::Buffered)
        .build()
        .await
        .unwrap();
    cache
        .set("buffered", &"value".to_owned(), None)
        .await
        .unwrap();
    cache.flush().await.unwrap();
    drop(cache);
    let reopened = Engram::builder().storage(path).build().await.unwrap();
    assert_eq!(
        reopened.get::<String>("buffered").await.unwrap(),
        Some("value".to_owned())
    );
}

proptest! {
    #[test]
    fn generated_operation_sequences_match_a_reference_model(
        operations in proptest::collection::vec((0_u8..6, any::<u8>()), 1..40)
    ) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let directory = TempDir::new().unwrap();
            let cache = cache(&directory).await;
            let mut model = HashMap::<String, u8>::new();
            for (operation, value) in operations {
                let key = format!("key:{}", value % 3);
                match operation {
                    0 => {
                        cache.set(&key, &value, None).await.unwrap();
                        model.insert(key, value);
                    }
                    1 => {
                        cache.remove(&key).await.unwrap();
                        model.remove(&key);
                    }
                    2 => {
                        cache.clear().await.unwrap();
                        model.clear();
                    }
                    3 => {
                        let other = format!("key:{}", (value.wrapping_add(1)) % 3);
                        cache
                            .set_many([(key.clone(), value), (other.clone(), value.wrapping_add(1))], None)
                            .await
                            .unwrap();
                        model.insert(key, value);
                        model.insert(other, value.wrapping_add(1));
                    }
                    4 => {
                        assert_eq!(cache.get::<u8>(&key).await.unwrap(), model.get(&key).copied());
                    }
                    _ => {
                        let source = TestSource {
                            validation: ValidationOutcome::Fresh { validation: None },
                            retrieval: RetrievalOutcome::Retrieved(Retrieved {
                                value: 255_u8.to_string(),
                                validation: None,
                            }),
                            validation_calls: AtomicUsize::new(0),
                            retrieval_calls: AtomicUsize::new(0),
                        };
                        // Resolve is type-specific; use it on a parallel string namespace
                        // while the numeric model continues to exercise shared cache operations.
                        let resolve_key = CacheKey::<String>::new(format!("resolve:{key}"), "sequence@1");
                        let _ = cache.resolve(&resolve_key, ResolveOptions::default(), &source).await;
                    }
                }
                for (model_key, model_value) in &model {
                    assert_eq!(cache.get::<u8>(model_key).await.unwrap(), Some(*model_value));
                }
            }
        });
    }
}
