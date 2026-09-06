//! Exclude standalone spool handles as well as loans from a retained store.

use super::*;

pub(crate) struct SpoolMaintenance {
    directory: PathBuf,
    pub(crate) files: Vec<(PathBuf, u64)>,
}

impl SpoolMaintenance {
    pub(crate) fn open(directory: PathBuf, limits: SpoolLimits) -> io::Result<Self> {
        if limits.max_files == 0 || limits.max_bytes == 0 {
            return Err(io::Error::other(limit_error(
                "invalid spool maintenance bounds",
            )));
        }
        let mut registry = STORES
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| io::Error::other("spool registry poisoned"))?;
        registry.stores.retain(|_, store| store.strong_count() != 0);
        if registry.maintenance.contains(&directory)
            || registry
                .stores
                .get(&directory)
                .is_some_and(|store| store.strong_count() != 0)
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "spool directory has a live handle or maintenance owner",
            ));
        }
        registry.maintenance.insert(directory.clone());
        drop(registry);
        let mut guard = Self {
            directory,
            files: Vec::new(),
        };
        let metadata = match std::fs::symlink_metadata(&guard.directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(guard),
            Err(error) => return Err(error),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::other("invalid spool maintenance directory"));
        }
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(&guard.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            if !name
                .to_str()
                .and_then(|s| s.strip_prefix("pipestream-"))
                .is_some_and(|suffix| {
                    !suffix.is_empty()
                        && suffix.len() <= 128
                        && suffix.bytes().all(|b| b.is_ascii_alphanumeric())
                })
            {
                return Err(io::Error::other("unrecognized abandoned spool filename"));
            }
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(io::Error::other("abandoned spool is not a regular file"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(io::Error::other("abandoned spool has an external alias"));
                }
            }
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| io::Error::other("spool file-length overflow"))?;
            if bytes > limits.max_bytes || guard.files.len() >= limits.max_files {
                return Err(io::Error::other(limit_error(
                    "abandoned spool exceeds configured bounds",
                )));
            }
            guard.files.push((entry.path(), metadata.len()));
        }
        Ok(guard)
    }
}

impl Drop for SpoolMaintenance {
    fn drop(&mut self) {
        if let Ok(mut registry) = STORES.get_or_init(Mutex::default).lock() {
            registry.maintenance.remove(&self.directory);
        }
    }
}
