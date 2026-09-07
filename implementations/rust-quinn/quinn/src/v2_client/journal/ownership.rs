use super::*;
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

/// An empty, stable advisory-lock sidecar, not a source of identity or recovery
/// evidence. Never unlink it while journal users might hold its inode open.
pub(super) struct Lease {
    file: File,
}
impl Lease {
    pub(super) fn acquire(path: &Path) -> Result<Self> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(JournalError::Corrupt(
                "client journal needs a UTF-8 file name",
            ))?;
        if name.ends_with(".client-lock") {
            return Err(JournalError::Corrupt(
                "client journal name collides with ownership sidecar",
            ));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;
        let lock = parent.join(format!("{name}.client-lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(lock)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() != 0 {
            return Err(JournalError::Corrupt(
                "client journal ownership file is not empty regular storage",
            ));
        }
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
            |_| {
                error(
                    ErrorCode::Conflict,
                    "client journal already owned or locking unavailable",
                )
            },
        )?;
        Ok(Self { file })
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let _ = rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock);
    }
}
