use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use engram::{Compression, Engram};
use tempfile::TempDir;

async fn cache(directory: &TempDir) -> Engram {
    Engram::builder()
        .storage(directory.path().join("engram.db"))
        .compression(Compression::Zstd { level: 3 })
        .build()
        .await
        .unwrap()
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
async fn invalid_database_path_surfaces_sqlite_error() {
    let directory = TempDir::new().unwrap();
    let result = Engram::builder()
        .storage(directory.path().join("absent").join("cache.db"))
        .build()
        .await;
    assert!(matches!(result, Err(engram::Error::Sqlite(_))));
}
