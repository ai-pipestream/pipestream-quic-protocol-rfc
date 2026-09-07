//! Bounded file adapters for trusted, application-owned local paths. Inputs are
//! prehashed on an open regular file; results are staged until verified FIN.
//! Cancellation of an accepted send/save waiter does not cancel its owned task.
use super::*;
use crate::v2_authority::workers::{Value, Workers};
use sha2::{Digest as _, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::OnceLock,
};

const CHUNK: usize = 8192;
static WORKERS: OnceLock<std::result::Result<Workers, Error>> = OnceLock::new();
type FileValue<T> = Value<T, Ticket>;

fn workers() -> Result<Workers> {
    Ok(WORKERS.get_or_init(|| Workers::new(4, 64)).clone()?)
}
fn reserve() -> Result<Ticket> {
    // File jobs and deferred destructors retain this process-wide owner slot.
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS
        .get_or_init(|| Arc::new(Semaphore::new(64)))
        .clone()
        .try_acquire_owned()
        .map(Arc::new)
        .map_err(|_| error(ErrorCode::LimitExceeded, "client file owner ceiling"))
}
fn fault(code: ErrorCode, detail: &'static str) -> Error {
    Error { code, detail }
}
fn io(error: std::io::Error) -> Error {
    fault(
        match error.kind() {
            std::io::ErrorKind::AlreadyExists => ErrorCode::Conflict,
            std::io::ErrorKind::NotFound => ErrorCode::NotFound,
            _ => ErrorCode::InternalError,
        },
        "client file operation failed",
    )
}
fn limit(length: u64, maximum: u64) -> std::result::Result<(), Error> {
    if maximum > MAX_NUMBER || length > maximum {
        return Err(fault(ErrorCode::LimitExceeded, "client file byte limit"));
    }
    Ok(())
}
async fn owned<T: Send + 'static>(
    ticket: Ticket,
    task: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    let (send, receive) = oneshot::channel();
    tokio::spawn(async move {
        let value = task.await;
        let _ = send.send(Packet {
            value,
            _ticket: ticket,
        });
    });
    receive
        .await
        .map_err(|_| error(ErrorCode::InternalError, "client file owner failed"))?
        .value
}

/// One prehashed, still-open input. The descriptor is not a snapshot: callers
/// must not modify it during upload. Length/digest are checked again before FIN.
/// Dropping it closes the descriptor on a file worker, not the async runtime.
pub struct FileInput {
    file: FileValue<File>,
    workers: Workers,
    ticket: Ticket,
    length: Number,
    sha256: Digest,
}
impl FileInput {
    /// Refuse non-regular files and final-component symlinks. The containing
    /// directory must be trusted and stable. At most `maximum + 1` bytes are
    /// inspected; a growing file cannot turn hashing into an unbounded scan.
    pub async fn open(path: PathBuf, maximum: u64) -> Result<Self> {
        limit(0, maximum)?;
        let ticket = reserve()?;
        let workers = workers()?;
        let destination = workers.clone();
        Ok(workers
            .run(move || {
                let mut file = OpenOptions::new()
                    .read(true)
                    .custom_flags(
                        (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
                    )
                    .open(path)
                    .map_err(io)?;
                let metadata = file.metadata().map_err(io)?;
                if !metadata.is_file() {
                    return Err(fault(ErrorCode::Conflict, "input is not a regular file"));
                }
                let length = metadata.len();
                limit(length, maximum)?;
                let mut digest = Sha256::new();
                let mut read = (&mut file).take(length + 1);
                let mut actual = 0;
                let mut buffer = [0; CHUNK];
                loop {
                    let n = read.read(&mut buffer).map_err(io)?;
                    if n == 0 {
                        break;
                    }
                    actual += n as u64;
                    digest.update(&buffer[..n]);
                }
                if actual != length {
                    return Err(fault(
                        ErrorCode::IntegrityError,
                        "input changed while hashing",
                    ));
                }
                file.rewind().map_err(io)?;
                Ok(Self {
                    file: Value::new(file, ticket.clone(), destination.clone()),
                    workers: destination,
                    ticket,
                    length: Number(length),
                    sha256: Digest(digest.finalize().into()),
                })
            })
            .await?)
    }
    pub fn length(&self) -> Number {
        self.length
    }
    pub fn sha256(&self) -> Digest {
        self.sha256
    }
    /// Wait for descriptor cleanup and release of this file-owner slot.
    pub async fn close(self) -> Result<()> {
        let Self {
            file,
            workers,
            ticket,
            ..
        } = self;
        drop(ticket);
        Ok(workers
            .run(move || {
                let (file, _ticket) = file.take();
                drop(file);
                Ok(())
            })
            .await?)
    }

    /// Send exactly this prehashed commitment using a caller-chosen original
    /// intent. A cancelled waiter still owns upload and receipt collection.
    /// A replay can return its original receipt without resending the body.
    pub async fn send(
        self,
        client: Client,
        intent: Intent,
        declaration: OperationId,
    ) -> Result<OperationReceipt> {
        let Mutation::Admit(parameters) = &intent.mutation else {
            return Err(error(
                ErrorCode::FrameError,
                "file input needs admission intent",
            ));
        };
        if parameters.input.length != self.length || parameters.input.sha256 != self.sha256 {
            return Err(error(
                ErrorCode::IntegrityError,
                "file does not match admission intent",
            ));
        }
        owned(self.ticket.clone(), async move {
            self.transmit(client, intent, declaration).await
        })
        .await
    }
    async fn transmit(
        self,
        client: Client,
        intent: Intent,
        declaration: OperationId,
    ) -> Result<OperationReceipt> {
        let mut input = client.input(intent, declaration).await?;
        let Self {
            mut file,
            workers,
            length,
            ..
        } = self;
        let transferred = async {
            let mut count = 0;
            loop {
                let destination = workers.clone();
                let amount = (length.0 + 1 - count).min(CHUNK as u64) as usize;
                let (next, bytes) = workers
                    .run(move || {
                        let (mut file, ticket) = file.take();
                        let mut bytes = vec![0; amount];
                        let n = file.read(&mut bytes).map_err(io)?;
                        bytes.truncate(n);
                        Ok((Value::new(file, ticket, destination), bytes))
                    })
                    .await?;
                file = next;
                if bytes.is_empty() {
                    break;
                }
                count += bytes.len() as u64;
                if count > length.0 {
                    return Err(error(
                        ErrorCode::IntegrityError,
                        "input grew during transmission",
                    ));
                }
                input.write(&bytes).await?;
            }
            // This verifies both length and SHA-256 before sending FIN.
            input.finish().await
        }
        .await;
        match transferred {
            Ok(()) => input.receipt().await,
            Err(local) => match input.abort().await {
                // A previously committed identical operation is authoritative,
                // even when the server stops the redundant body transmission.
                Ok(receipt) => Ok(receipt),
                Err(outcome)
                    if matches!(
                        &local,
                        Failure::Protocol(Error {
                            code: ErrorCode::Cancelled,
                            ..
                        })
                    ) =>
                {
                    // An early authority refusal stops the writer. Preserve its
                    // correlated name (or receipt-persistence failure), not the
                    // secondary local "writer stopped" symptom.
                    Err(outcome)
                }
                Err(_) => Err(local),
            },
        }
    }
}

/// A verified delivery installed without overwriting an existing destination.
/// The proof describes received bytes; it does not promise that another local
/// process will not later edit/delete the file or renew remote output retention.
pub struct SavedOutput {
    pub path: PathBuf,
    pub verification: transport::VerifiedObject,
}
struct Staging {
    file: tempfile::NamedTempFile,
    directory: File,
    destination: PathBuf,
}
impl Staging {
    fn create(destination: PathBuf) -> std::result::Result<Self, Error> {
        if destination.file_name().is_none() {
            return Err(fault(ErrorCode::Conflict, "output needs a file name"));
        }
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = File::open(parent).map_err(io)?;
        let file = tempfile::Builder::new()
            .prefix(".pipestream-result-")
            .tempfile_in(parent)
            .map_err(io)?;
        Ok(Self {
            file,
            directory,
            destination,
        })
    }
    fn commit(self) -> std::result::Result<PathBuf, Error> {
        self.file.as_file().sync_all().map_err(io)?;
        self.file
            .persist_noclobber(&self.destination)
            .map_err(|e| io(e.error))?;
        // Failure after installation is ambiguous local durability, not authority
        // failure. Never remove or overwrite the installed result to hide it.
        self.directory.sync_all().map_err(io)?;
        Ok(self.destination)
    }
}
impl Output {
    /// Save under a trusted, stable application-owned directory. No destination
    /// appears before verified FIN. Existing files/symlinks refuse CONFLICT.
    /// The accepted task survives waiter cancellation, including final fsync.
    /// Abrupt process death can leave an identifiable staging file; this API
    /// does not scan or delete other transfers' files on startup.
    pub async fn save_to(self, path: PathBuf, maximum: u64) -> Result<SavedOutput> {
        let (output, ticket) = self.into_parts();
        save(output, path, maximum, ticket).await
    }
}
impl transport::Output {
    /// Stage and install a fully verified delivery, with the same file limits
    /// and cancellation rules as the durable facade. This low-level API does
    /// not persist the supplied manifest/selection in a client journal.
    pub async fn save_to(self, path: PathBuf, maximum: u64) -> Result<SavedOutput> {
        save(self, path, maximum, ()).await
    }
}
async fn save(
    mut output: transport::Output,
    path: PathBuf,
    maximum: u64,
    hold: impl Send + 'static,
) -> Result<SavedOutput> {
    limit(output.header().length.0, maximum)?;
    let ticket = reserve()?;
    let workers = workers()?;
    owned(ticket.clone(), async move {
        let saved = async {
            let destination = workers.clone();
            let mut staged = workers
                .run(move || Ok(Value::new(Staging::create(path)?, ticket, destination)))
                .await?;
            while let Some(bytes) = output.read_unverified().await? {
                let destination = workers.clone();
                staged = workers
                    .run(move || {
                        let (mut staged, ticket) = staged.take();
                        staged.file.write_all(&bytes).map_err(io)?;
                        Ok(Value::new(staged, ticket, destination))
                    })
                    .await?;
            }
            let verification = output
                .verification()
                .cloned()
                .ok_or_else(|| error(ErrorCode::IntegrityError, "result FIN is not verified"))?;
            let path = workers
                .run(move || {
                    let (staged, _ticket) = staged.take();
                    staged.commit()
                })
                .await?;
            Ok(SavedOutput { path, verification })
        }
        .await;
        drop(hold);
        saved
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn prehash_handles_empty_and_binary_files_and_refuses_oversize() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input");
        for bytes in [vec![], vec![0x91; CHUNK * 3 + 7]] {
            std::fs::write(&path, &bytes).unwrap();
            let input = FileInput::open(path.clone(), bytes.len() as u64)
                .await
                .unwrap();
            assert_eq!(input.length(), Number(bytes.len() as u64));
            assert_eq!(input.sha256(), Digest(Sha256::digest(&bytes).into()));
            input.close().await.unwrap();
        }
        assert!(matches!(
            FileInput::open(path, 1).await,
            Err(Failure::Protocol(Error {
                code: ErrorCode::LimitExceeded,
                ..
            }))
        ));
    }
    #[tokio::test]
    async fn refuse_symlink_directory_and_fifo_without_waiting_for_a_writer() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input");
        std::fs::write(&path, b"abc").unwrap();
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        let fifo = directory.path().join("fifo");
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            0,
        )
        .unwrap();
        for path in [link, directory.path().to_owned(), fifo] {
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    FileInput::open(path, 1024)
                )
                .await
                .unwrap()
                .is_err()
            );
        }
    }
    #[test]
    fn staging_is_unpublished_until_commit_and_conflicts_preserve_existing_data() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("result");
        let mut staged = Staging::create(path.clone()).unwrap();
        staged.file.write_all(b"result").unwrap();
        assert!(!path.exists());
        std::fs::write(&path, b"existing").unwrap();
        assert_eq!(staged.commit().unwrap_err().code, ErrorCode::Conflict);
        assert_eq!(std::fs::read(&path).unwrap(), b"existing");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        let next = directory.path().join("next");
        let mut staged = Staging::create(next.clone()).unwrap();
        staged.file.write_all(b"verified").unwrap();
        assert_eq!(staged.commit().unwrap(), next);
        assert_eq!(std::fs::read(next).unwrap(), b"verified");
    }
    #[test]
    fn dropped_staging_removes_only_its_own_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("result");
        let unrelated = directory.path().join(".pipestream-result-unrelated");
        std::fs::write(&unrelated, b"other transfer").unwrap();
        let mut staged = Staging::create(path.clone()).unwrap();
        staged.file.write_all(b"unverified prefix").unwrap();
        drop(staged);
        assert!(!path.exists());
        assert_eq!(std::fs::read(&unrelated).unwrap(), b"other transfer");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}
