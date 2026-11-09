use std::path::Path;

use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};

use crate::{Result, SqliteSynchronous};

pub(crate) struct StoredEntry {
    pub(crate) bytes: Vec<u8>,
    pub(crate) expires_at_ms: Option<i64>,
    pub(crate) encoding: i64,
    pub(crate) type_name: String,
    pub(crate) memory_weight: u32,
    pub(crate) validation_metadata: Option<Vec<u8>>,
    pub(crate) validated_at_ms: Option<i64>,
}

#[derive(Clone)]
pub(crate) struct StoredWrite {
    pub(crate) key: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) expires_at_ms: Option<i64>,
    pub(crate) encoding: i64,
    pub(crate) type_name: String,
    pub(crate) memory_weight: u32,
    pub(crate) created_at_ms: i64,
    pub(crate) validation_metadata: Option<Vec<u8>>,
    pub(crate) validated_at_ms: Option<i64>,
}

pub(crate) struct SqliteStore {
    readers: SqlitePool,
    writer: SqlitePool,
}

impl SqliteStore {
    pub(crate) async fn open(
        path: &Path,
        connections: u32,
        synchronous: SqliteSynchronous,
    ) -> Result<Self> {
        // Connection options apply to every pooled connection, unlike one-off PRAGMAs.
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .pragma("journal_mode", "WAL")
            .pragma("busy_timeout", "5000")
            .pragma("synchronous", synchronous.as_pragma());
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await?;
        let readers = SqlitePoolOptions::new()
            .max_connections(connections)
            .connect_with(options)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS engram_entries (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                expires_at_ms INTEGER,
                encoding INTEGER NOT NULL,
                type_name TEXT NOT NULL,
                memory_weight INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                validation_metadata BLOB,
                validated_at_ms INTEGER
            )",
        )
        .execute(&writer)
        .await?;
        let columns = sqlx::query("PRAGMA table_info(engram_entries)")
            .fetch_all(&writer)
            .await?;
        if !columns
            .iter()
            .any(|column| column.get::<String, _>("name") == "memory_weight")
        {
            sqlx::query(
                "ALTER TABLE engram_entries
                 ADD COLUMN memory_weight INTEGER NOT NULL DEFAULT 1",
            )
            .execute(&writer)
            .await?;
        }
        if !columns
            .iter()
            .any(|column| column.get::<String, _>("name") == "validation_metadata")
        {
            sqlx::query("ALTER TABLE engram_entries ADD COLUMN validation_metadata BLOB")
                .execute(&writer)
                .await?;
        }
        if !columns
            .iter()
            .any(|column| column.get::<String, _>("name") == "validated_at_ms")
        {
            sqlx::query("ALTER TABLE engram_entries ADD COLUMN validated_at_ms INTEGER")
                .execute(&writer)
                .await?;
        }
        // A partial index avoids indexing immortal rows, which are not candidates for cleanup.
        sqlx::query("DROP INDEX IF EXISTS engram_entries_expiry")
            .execute(&writer)
            .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS engram_entries_expiry
             ON engram_entries(expires_at_ms) WHERE expires_at_ms IS NOT NULL",
        )
        .execute(&writer)
        .await?;
        Ok(Self { readers, writer })
    }

    pub(crate) async fn get(&self, key: &str) -> Result<Option<StoredEntry>> {
        let row = sqlx::query(
            "SELECT value, expires_at_ms, encoding, type_name, memory_weight,
                    validation_metadata, validated_at_ms
             FROM engram_entries WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(&self.readers)
        .await?;
        Ok(row.map(|row| StoredEntry {
            bytes: row.get("value"),
            expires_at_ms: row.get("expires_at_ms"),
            encoding: row.get("encoding"),
            type_name: row.get("type_name"),
            memory_weight: row.get::<i64, _>("memory_weight").clamp(1, u32::MAX as i64) as u32,
            validation_metadata: row.get("validation_metadata"),
            validated_at_ms: row.get("validated_at_ms"),
        }))
    }

    pub(crate) async fn set(&self, write: &StoredWrite) -> Result<()> {
        self.write(write, &self.writer).await
    }

    pub(crate) async fn set_many(&self, writes: &[StoredWrite]) -> Result<()> {
        let mut transaction = self.writer.begin().await?;
        for write in writes {
            self.write(write, &mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn write<'e, E>(&self, write: &StoredWrite, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO engram_entries
                (key, value, expires_at_ms, encoding, type_name, memory_weight, created_at_ms,
                 validation_metadata, validated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
               value = excluded.value, expires_at_ms = excluded.expires_at_ms,
               encoding = excluded.encoding, type_name = excluded.type_name,
               memory_weight = excluded.memory_weight, created_at_ms = excluded.created_at_ms,
               validation_metadata = excluded.validation_metadata,
               validated_at_ms = excluded.validated_at_ms",
        )
        .bind(&write.key)
        .bind(&write.bytes)
        .bind(write.expires_at_ms)
        .bind(write.encoding)
        .bind(&write.type_name)
        .bind(i64::from(write.memory_weight))
        .bind(write.created_at_ms)
        .bind(&write.validation_metadata)
        .bind(write.validated_at_ms)
        .execute(executor)
        .await?;
        Ok(())
    }

    pub(crate) async fn remove(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM engram_entries WHERE key = ?")
            .bind(key)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    pub(crate) async fn clear(&self) -> Result<()> {
        sqlx::query("DELETE FROM engram_entries")
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    pub(crate) async fn update_validation(
        &self,
        key: &str,
        validation_metadata: Option<&[u8]>,
        validated_at_ms: i64,
    ) -> Result<bool> {
        Ok(sqlx::query(
            "UPDATE engram_entries
             SET validation_metadata = ?, validated_at_ms = ?
             WHERE key = ?",
        )
        .bind(validation_metadata)
        .bind(validated_at_ms)
        .bind(key)
        .execute(&self.writer)
        .await?
        .rows_affected()
            == 1)
    }

    pub(crate) async fn purge_expired(&self, now_ms: i64) -> Result<u64> {
        Ok(sqlx::query(
            "DELETE FROM engram_entries WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= ?",
        )
        .bind(now_ms)
        .execute(&self.writer)
        .await?
        .rows_affected())
    }

    #[cfg(test)]
    pub(crate) async fn abort_writes_for_test(&self) -> Result<()> {
        sqlx::query(
            "CREATE TRIGGER engram_abort_write_for_test
             BEFORE INSERT ON engram_entries
             BEGIN SELECT RAISE(ABORT, 'forced batch failure'); END",
        )
        .execute(&self.writer)
        .await?;
        Ok(())
    }
}
