use std::path::Path;

use sqlx::{sqlite::{SqliteConnectOptions, SqlitePoolOptions}, Row, SqlitePool};

use crate::Result;

pub(crate) struct StoredEntry {
    pub(crate) bytes: Vec<u8>,
    pub(crate) expires_at_ms: Option<i64>,
    pub(crate) encoding: i64,
}

pub(crate) struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub(crate) async fn open(path: &Path, connections: u32) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(connections)
            .connect_with(options)
            .await?;

        sqlx::query("PRAGMA journal_mode = WAL").execute(&pool).await?;
        sqlx::query("PRAGMA busy_timeout = 5000").execute(&pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS engram_entries (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                expires_at_ms INTEGER,
                encoding INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS engram_entries_expiry ON engram_entries(expires_at_ms)")
            .execute(&pool)
            .await?;
        Ok(Self { pool })
    }

    pub(crate) async fn get(&self, key: &str) -> Result<Option<StoredEntry>> {
        let row = sqlx::query("SELECT value, expires_at_ms, encoding FROM engram_entries WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| StoredEntry {
            bytes: row.get("value"),
            expires_at_ms: row.get("expires_at_ms"),
            encoding: row.get("encoding"),
        }))
    }

    pub(crate) async fn set(&self, key: &str, bytes: &[u8], expires_at_ms: Option<i64>, encoding: i64, now_ms: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO engram_entries (key, value, expires_at_ms, encoding, created_at_ms)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
               value = excluded.value,
               expires_at_ms = excluded.expires_at_ms,
               encoding = excluded.encoding,
               created_at_ms = excluded.created_at_ms",
        )
        .bind(key)
        .bind(bytes)
        .bind(expires_at_ms)
        .bind(encoding)
        .bind(now_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn remove(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM engram_entries WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn clear(&self) -> Result<()> {
        sqlx::query("DELETE FROM engram_entries").execute(&self.pool).await?;
        Ok(())
    }
}
