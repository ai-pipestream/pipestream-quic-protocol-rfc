//! Offline raw exports. No operation here creates a network session or changes
//! retained journal evidence. Accepted copies run to completion on file workers.
use super::*;
use pipestream_core::v2::client::results::exports::ExportStore;
pub use pipestream_core::v2::client::results::exports::{ExportPolicy, ExportUsage, Exported};

#[derive(Clone)]
pub struct ManagedExports {
    store: Arc<FileValue<ExportStore>>,
    workers: Workers,
}

#[cfg(test)]
pub(crate) mod tests;
impl ManagedExports {
    pub async fn initialize(
        path: PathBuf,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ExportPolicy,
    ) -> Result<Self> {
        Self::open_inner(path, authority, owner, policy, true).await
    }
    pub async fn open(
        path: PathBuf,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ExportPolicy,
    ) -> Result<Self> {
        Self::open_inner(path, authority, owner, policy, false).await
    }
    async fn open_inner(
        path: PathBuf,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ExportPolicy,
        fresh: bool,
    ) -> Result<Self> {
        let ticket = reserve()?;
        let workers = workers()?;
        let destination = workers.clone();
        let store = workers
            .run(move || {
                let open = if fresh {
                    ExportStore::initialize
                } else {
                    ExportStore::open
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
    pub async fn usage(&self) -> Result<ExportUsage> {
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
    /// Consume the pinned reader in one owned worker job. Dropping the waiter
    /// cannot interrupt the accepted copy between intent and installation.
    pub async fn export(
        &self,
        id: OperationId,
        reference: RetainedReference,
        mut source: LocalCopy,
    ) -> Result<Exported> {
        let file = source
            .file
            .take()
            .ok_or_else(|| error(ErrorCode::Cancelled, "local result reader is closed"))?;
        let store = self.store.clone();
        Ok(self
            .workers
            .run(move || {
                let (mut reader, _ticket) = file.take();
                store
                    .get()
                    .export(id, &reference, &mut reader)
                    .map_err(storage)
            })
            .await?)
    }
    /// Remove only this named raw export and its reservation, never source
    /// copies or journal/server state. Interrupted removals are retryable.
    pub async fn remove(&self, id: OperationId) -> Result<bool> {
        let ticket = reserve()?;
        let store = self.store.clone();
        Ok(self
            .workers
            .run(move || {
                let _ticket = ticket;
                store.get().remove(id).map_err(storage)
            })
            .await?)
    }
    pub async fn close(self) -> Result<()> {
        let value = Arc::try_unwrap(self.store)
            .map_err(|_| error(ErrorCode::Conflict, "local export owner is still shared"))?;
        Ok(self
            .workers
            .run(move || {
                drop(value.take());
                Ok(())
            })
            .await?)
    }
}
