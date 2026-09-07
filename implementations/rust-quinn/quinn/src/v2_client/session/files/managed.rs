//! Managed local result copies. The journal remains the source of selected
//! identity; these files are not credentials or fresh remote authorization.
use super::*;
pub use pipestream_core::v2::client::results::{ResultPolicy, ResultUsage};
use pipestream_core::v2::{
    authority::StoreError,
    client::results::{LocalResult, ResultStore},
};
use std::time::Instant;

pub mod exports;

fn storage(error: StoreError) -> Error {
    match error {
        StoreError::Protocol(e) => e,
        StoreError::Corrupt(_) => {
            fault(ErrorCode::IntegrityError, "local result storage is corrupt")
        }
        StoreError::Io(e) => io(e),
        _ => fault(ErrorCode::InternalError, "local result storage failed"),
    }
}

/// Cloneable owner of one private, quota-bound result directory. All filesystem
/// work and last-owner cleanup run on the shared bounded file-worker pool.
#[derive(Clone)]
pub struct ManagedResults {
    store: Arc<FileValue<ResultStore>>,
    workers: Workers,
}
impl ManagedResults {
    pub async fn initialize(
        path: PathBuf,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ResultPolicy,
    ) -> Result<Self> {
        Self::open_inner(path, authority, owner, policy, true).await
    }
    pub async fn open(
        path: PathBuf,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ResultPolicy,
    ) -> Result<Self> {
        Self::open_inner(path, authority, owner, policy, false).await
    }
    async fn open_inner(
        path: PathBuf,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ResultPolicy,
        fresh: bool,
    ) -> Result<Self> {
        let ticket = reserve()?;
        let workers = workers()?;
        let destination = workers.clone();
        let store = workers
            .run(move || {
                let open = if fresh {
                    ResultStore::initialize
                } else {
                    ResultStore::open
                };
                Ok(Arc::new(Value::new(
                    open(&path, authority, owner, policy).map_err(storage)?,
                    ticket,
                    destination,
                )))
            })
            .await?;
        Ok(Self { store, workers })
    }
    pub async fn usage(&self) -> Result<ResultUsage> {
        let ticket = reserve()?;
        let store = self.store.clone();
        Ok(self
            .workers
            .run(move || {
                let _ticket = ticket;
                store.get().usage().map_err(storage)
            })
            .await?)
    }
    pub async fn recovered_stages(&self) -> Result<usize> {
        let ticket = reserve()?;
        let store = self.store.clone();
        Ok(self
            .workers
            .run(move || {
                let _ticket = ticket;
                Ok(store.get().recovered_stages())
            })
            .await?)
    }
    /// Explicit local removal only. Live readers/downloads retain their pins.
    pub async fn remove(&self, key: String) -> Result<bool> {
        let ticket = reserve()?;
        let store = self.store.clone();
        Ok(self
            .workers
            .run(move || {
                let _ticket = ticket;
                store.get().remove(&key).map_err(storage)
            })
            .await?)
    }
    /// Download from an already authenticated, correlated durable client stream.
    /// The owned transfer survives cancellation and publishes only after verified
    /// FIN and local file/directory synchronization. Every reception reserves
    /// its full selected length; repeated downloads are separately charged copies.
    pub async fn save(&self, output: Output) -> Result<SavedResult> {
        let ticket = reserve()?;
        let store = self.store.clone();
        let workers = self.workers.clone();
        let (mut output, hold, reference, selected) = output.into_result_parts();
        owned(ticket.clone(), async move {
            let saved = async {
                let destination = workers.clone();
                let mut stage = workers
                    .run(move || {
                        let limit = store.get().chunk_limit();
                        let stage = store
                            .get()
                            .stage(reference, &selected, Instant::now())
                            .map_err(storage)?;
                        Ok((Value::new(stage, ticket, destination), limit))
                    })
                    .await?;
                while let Some(bytes) = output.read_unverified().await? {
                    let destination = workers.clone();
                    stage = workers
                        .run(move || {
                            let (staged, limit) = stage;
                            let (mut pending, ticket) = staged.take();
                            for chunk in bytes.chunks(limit) {
                                pending.receive(chunk, Instant::now()).map_err(storage)?;
                            }
                            Ok((Value::new(pending, ticket, destination), limit))
                        })
                        .await?;
                }
                let verification = output.verification().cloned().ok_or_else(|| {
                    error(ErrorCode::IntegrityError, "result FIN is not verified")
                })?;
                let header = verification.header().clone();
                let key = workers
                    .run(move || {
                        let (pending, _ticket) = stage.0.take();
                        let installed = pending.finish(&header, Instant::now()).map_err(storage)?;
                        Ok(installed.key().to_owned())
                    })
                    .await?;
                Ok(SavedResult { key, verification })
            }
            .await;
            drop(hold);
            saved
        })
        .await
    }
    /// Explicit local-only lookup using a saved journal selection. A hit says
    /// nothing about current server authorization or remaining remote retention.
    pub async fn find(&self, reference: RetainedReference) -> Result<Option<LocalCopy>> {
        let ticket = reserve()?;
        let store = self.store.clone();
        let destination = self.workers.clone();
        let found = self
            .workers
            .run(move || {
                Ok(store
                    .get()
                    .find(&reference)
                    .map_err(storage)?
                    .map(|reader| {
                        let key = reader.key().to_owned();
                        (
                            Value::new(reader, ticket, destination),
                            key,
                            store.get().chunk_limit(),
                        )
                    }))
            })
            .await?;
        Ok(found.map(|(file, key, chunk)| LocalCopy {
            file: Some(file),
            key,
            chunk,
            verified: false,
            workers: self.workers.clone(),
        }))
    }
    /// Drain this root's filesystem owner. Refuse if another clone still owns it.
    /// Outstanding read/download handles can retain the underlying root longer.
    pub async fn close(self) -> Result<()> {
        let value = Arc::try_unwrap(self.store)
            .map_err(|_| error(ErrorCode::Conflict, "local result owner is still shared"))?;
        Ok(self
            .workers
            .run(move || {
                drop(value.take());
                Ok(())
            })
            .await?)
    }
}

pub struct SavedResult {
    pub key: String,
    pub verification: transport::VerifiedObject,
}
pub struct LocalCopy {
    file: Option<FileValue<LocalResult>>,
    key: String,
    chunk: usize,
    verified: bool,
    workers: Workers,
}
impl LocalCopy {
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn verified(&self) -> bool {
        self.verified
    }
    /// Bytes remain provisional until None with verified()==true. Cancellation
    /// closes this reader after its owned I/O completes; reopen via find().
    pub async fn read_unverified(&mut self) -> Result<Option<Vec<u8>>> {
        let file = self
            .file
            .take()
            .ok_or_else(|| error(ErrorCode::Cancelled, "local result reader is closed"))?;
        let destination = self.workers.clone();
        let chunk = self.chunk;
        let (file, bytes, verified) = self
            .workers
            .run(move || {
                let (mut reader, ticket) = file.take();
                let mut bytes = vec![0; chunk];
                let read = reader.read_chunk(&mut bytes).map_err(storage)?;
                bytes.truncate(read);
                let verified = reader.verified();
                Ok((Value::new(reader, ticket, destination), bytes, verified))
            })
            .await?;
        self.file = Some(file);
        self.verified = verified;
        Ok(if bytes.is_empty() { None } else { Some(bytes) })
    }
    pub async fn close(mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            self.workers
                .run(move || {
                    drop(file.take());
                    Ok(())
                })
                .await?;
        }
        Ok(())
    }
}
