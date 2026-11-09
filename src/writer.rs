use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use tokio::sync::{mpsc, oneshot};

use crate::{
    storage::{SqliteStore, StoredWrite},
    Error, Result,
};

type WriteResult = std::result::Result<(), String>;

enum Command {
    Write {
        write: StoredWrite,
        active: Arc<AtomicBool>,
        completion: oneshot::Sender<WriteResult>,
    },
    Flush(oneshot::Sender<WriteResult>),
}

struct QueuedWrite {
    write: StoredWrite,
    active: Arc<AtomicBool>,
    completion: oneshot::Sender<WriteResult>,
}

/// A single SQLite writer that groups buffered cache writes into transactions.
pub(crate) struct BufferedWriter {
    sender: mpsc::UnboundedSender<Command>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl BufferedWriter {
    pub(crate) fn new(storage: Arc<SqliteStore>) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let last_error = Arc::new(Mutex::new(None));
        let task_error = last_error.clone();
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let mut writes = Vec::new();
                let mut flushes = Vec::new();
                collect(command, &mut writes, &mut flushes);
                while writes.len() < 128 {
                    match receiver.try_recv() {
                        Ok(command) => collect(command, &mut writes, &mut flushes),
                        Err(_) => break,
                    }
                }
                let mut active = Vec::with_capacity(writes.len());
                for queued in writes {
                    if queued.active.load(Ordering::Acquire) {
                        active.push(queued);
                    } else {
                        let _ = queued.completion.send(Ok(()));
                    }
                }
                let outcome = if active.is_empty() {
                    Ok(())
                } else {
                    let stored: Vec<StoredWrite> =
                        active.iter().map(|queued| queued.write.clone()).collect();
                    storage
                        .set_many(&stored)
                        .await
                        .map_err(|error| error.to_string())
                };
                if let Err(error) = &outcome {
                    *task_error.lock().expect("writer error mutex poisoned") = Some(error.clone());
                }
                for queued in active {
                    let _ = queued.completion.send(outcome.clone());
                }
                for flush in flushes {
                    let _ = flush.send(outcome.clone());
                }
            }
        });
        Arc::new(Self { sender, last_error })
    }

    pub(crate) fn enqueue(
        &self,
        write: StoredWrite,
        active: Arc<AtomicBool>,
    ) -> Result<oneshot::Receiver<WriteResult>> {
        let (completion, receiver) = oneshot::channel();
        self.sender
            .send(Command::Write {
                write,
                active,
                completion,
            })
            .map_err(|_| Error::BufferedWrite("writer task has stopped".to_owned()))?;
        Ok(receiver)
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Flush(sender))
            .map_err(|_| Error::BufferedWrite("writer task has stopped".to_owned()))?;
        let outcome = receiver
            .await
            .map_err(|_| Error::BufferedWrite("writer task has stopped".to_owned()))?;
        if let Some(error) = self
            .last_error
            .lock()
            .expect("writer error mutex poisoned")
            .take()
        {
            return Err(Error::BufferedWrite(error));
        }
        outcome.map_err(Error::BufferedWrite)
    }
}

fn collect(
    command: Command,
    writes: &mut Vec<QueuedWrite>,
    flushes: &mut Vec<oneshot::Sender<WriteResult>>,
) {
    match command {
        Command::Write {
            write,
            active,
            completion,
        } => writes.push(QueuedWrite {
            write,
            active,
            completion,
        }),
        Command::Flush(flush) => flushes.push(flush),
    }
}
