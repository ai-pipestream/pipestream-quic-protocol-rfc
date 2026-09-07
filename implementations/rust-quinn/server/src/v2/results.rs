//! Explicit managed local storage. Offline commands never construct an endpoint.
use super::*;
use session::files::managed::{
    ManagedResults, ResultPolicy,
    exports::{ExportPolicy, ManagedExports},
};

#[derive(Debug, Args)]
pub struct Owner {
    #[arg(long)]
    authority: String,
    #[arg(long)]
    owner: String,
}
impl Owner {
    fn labels(&self) -> (IdentityLabel, IdentityLabel) {
        (
            IdentityLabel(self.authority.clone()),
            IdentityLabel(self.owner.clone()),
        )
    }
}

#[derive(Debug, Args)]
pub struct Copies {
    #[arg(long)]
    result_dir: PathBuf,
    #[arg(long, default_value_t = 10000)]
    result_objects: u64,
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    result_bytes: u64,
}
impl Copies {
    async fn open(
        &self,
        labels: (IdentityLabel, IdentityLabel),
        fresh: bool,
    ) -> Result<ManagedResults> {
        let policy = ResultPolicy {
            objects: Id(self.result_objects),
            bytes: Number(self.result_bytes),
            owner_objects: Id(self.result_objects),
            owner_bytes: Number(self.result_bytes),
            chunk_bytes: Id(8192),
            handles: Id(64),
            owner_handles: Id(64),
        };
        Ok(if fresh {
            ManagedResults::initialize(self.result_dir.clone(), labels.0, labels.1, policy).await?
        } else {
            ManagedResults::open(self.result_dir.clone(), labels.0, labels.1, policy).await?
        })
    }
}
#[derive(Debug, Args)]
#[group(id = "ExportStorage")]
pub struct Exports {
    #[arg(long)]
    export_dir: PathBuf,
    #[arg(long, default_value_t = 10000)]
    export_objects: u64,
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    export_bytes: u64,
}
impl Exports {
    async fn open(
        &self,
        labels: (IdentityLabel, IdentityLabel),
        fresh: bool,
    ) -> Result<ManagedExports> {
        let policy = ExportPolicy {
            objects: self.export_objects,
            bytes: self.export_bytes,
        };
        Ok(if fresh {
            ManagedExports::initialize(self.export_dir.clone(), labels.0, labels.1, policy).await?
        } else {
            ManagedExports::open(self.export_dir.clone(), labels.0, labels.1, policy).await?
        })
    }
}
#[derive(Debug, Args)]
pub struct Selection {
    #[arg(long, value_parser=client::work)]
    work: WorkKey,
    #[arg(long)]
    attempt: u64,
    #[arg(long)]
    index: u64,
}
#[derive(Debug, Subcommand)]
pub enum Local {
    /// Read the complete local copy and verify its saved length and SHA-256.
    Verify {
        #[command(flatten)]
        selection: Selection,
    },
    /// Export exact selected bytes using a stable local identity, without network I/O.
    Export {
        #[command(flatten)]
        selection: Selection,
        #[command(flatten)]
        exports: Exports,
        #[arg(long, value_parser=client::operation_id)]
        export_id: OperationId,
    },
}
#[derive(Debug, Subcommand)]
pub enum CopyMaintenance {
    Usage,
    /// Remove only this local copy. Does not affect journal or remote output.
    Remove {
        #[arg(long)]
        copy_key: String,
    },
}
#[derive(Debug, Subcommand)]
pub enum ExportMaintenance {
    Usage,
    /// Remove only this local export and reservation. Does not remove its source.
    Remove {
        #[arg(long, value_parser=client::operation_id)]
        export_id: OperationId,
    },
}
pub async fn init_results(owner: Owner, copies: Copies) -> Result<()> {
    copies.open(owner.labels(), true).await?.close().await?;
    println!("RESULTS_INITIALIZED");
    Ok(())
}
pub async fn init_exports(owner: Owner, exports: Exports) -> Result<()> {
    exports.open(owner.labels(), true).await?.close().await?;
    println!("EXPORTS_INITIALIZED");
    Ok(())
}
pub async fn copy_maintenance(
    owner: Owner,
    copies: Copies,
    command: CopyMaintenance,
) -> Result<()> {
    let store = copies.open(owner.labels(), false).await?;
    let result: Result<()> = async {
        match command {
            CopyMaintenance::Usage => println!("LOCAL_RESULT_USAGE {:?}", store.usage().await?),
            CopyMaintenance::Remove { copy_key } => {
                println!("LOCAL_RESULT_REMOVED {}", store.remove(copy_key).await?)
            }
        }
        Ok(())
    }
    .await;
    let closed = store.close().await;
    result?;
    closed?;
    Ok(())
}
pub async fn export_maintenance(
    owner: Owner,
    exports: Exports,
    command: ExportMaintenance,
) -> Result<()> {
    let store = exports.open(owner.labels(), false).await?;
    let result: Result<()> = async {
        match command {
            ExportMaintenance::Usage => println!("LOCAL_EXPORT_USAGE {:?}", store.usage().await?),
            ExportMaintenance::Remove { export_id } => {
                println!("LOCAL_EXPORT_REMOVED {}", store.remove(export_id).await?)
            }
        }
        Ok(())
    }
    .await;
    let closed = store.close().await;
    result?;
    closed?;
    Ok(())
}
pub async fn download(
    client: &session::Client,
    selection: Selection,
    copies: Copies,
) -> Result<()> {
    let identity = client.identity();
    let store = copies
        .open((identity.authority.clone(), identity.owner.clone()), false)
        .await?;
    let result: Result<_> = async {
        let output = client
            .read_output(
                selection.work,
                Id(selection.attempt),
                OutputIndex(selection.index),
            )
            .await?;
        Ok(store.save(output).await?)
    }
    .await;
    let closed = store.close().await;
    let saved = result?;
    closed?;
    println!("DOWNLOADED {}", saved.key);
    println!("VERIFIED {:?}", saved.verification.header());
    Ok(())
}
pub async fn local(journal: ClientJournal, copies: Copies, command: Local) -> Result<()> {
    let journal = journal.open(false).await?;
    let result = local_inner(&journal, copies, command).await;
    let closed = journal.shutdown().await;
    result?;
    closed?;
    Ok(())
}
async fn local_inner(journal: &journal::Journal, copies: Copies, command: Local) -> Result<()> {
    let selection = match &command {
        Local::Verify { selection } | Local::Export { selection, .. } => selection,
    };
    // Require original, durably saved selection before opening any local copy.
    let reference = journal
        .retained_reference(
            selection.work.clone(),
            Id(selection.attempt),
            OutputIndex(selection.index),
        )
        .await?;
    let labels = (
        reference.manifest().authority.clone(),
        reference.manifest().owner.clone(),
    );
    let store = copies.open(labels.clone(), false).await?;
    let result: Result<()> = async {
        let mut source = store.find(reference.clone()).await?.ok_or_else(|| {
            anyhow::anyhow!("NOT_FOUND: selected output has no local copy; no remote fallback")
        })?;
        match command {
            Local::Verify { .. } => {
                let verified: Result<()> = async {
                    while source.read_unverified().await?.is_some() {}
                    if !source.verified() {
                        bail!("INTEGRITY_ERROR: local EOF is not verified");
                    }
                    Ok(())
                }
                .await;
                let closed = source.close().await;
                verified?;
                closed?;
                println!("LOCAL_VERIFIED");
            }
            Local::Export {
                exports, export_id, ..
            } => {
                let opened = exports.open(labels, false).await;
                let exports = match opened {
                    Ok(exports) => exports,
                    Err(e) => {
                        let _ = source.close().await;
                        return Err(e);
                    }
                };
                let exported = exports.export(export_id, reference, source).await;
                let closed = exports.close().await;
                let exported = exported?;
                closed?;
                println!(
                    "LOCAL_EXPORTED replayed={} {}",
                    exported.replayed,
                    exported.path.display()
                );
            }
        }
        Ok(())
    }
    .await;
    let closed = store.close().await;
    result?;
    closed?;
    Ok(())
}
