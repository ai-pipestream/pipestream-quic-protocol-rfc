//! Bounded startup configuration. No credential path or principal is inferred
//! from a result URI. All reads here precede accepting network requests.
use super::*;
use anyhow::Context;
use pipestream_quic::v2::authority::{
    AuthorityStore, Authorization, Clock, ClockReading, Permission, StorePolicy,
    payload::{PayloadPolicy, PayloadStore},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::Read,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn bytes(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    if !file.metadata()?.is_file() {
        bail!("configuration must be a regular file");
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        bail!("configuration exceeds its byte limit");
    }
    Ok(bytes)
}
pub fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let bytes = bytes(path, 131072)?;
    let certificates: Vec<_> =
        CertificateDer::pem_slice_iter(&bytes).collect::<std::result::Result<_, _>>()?;
    if certificates.is_empty()
        || certificates.len() > 16
        || certificates.iter().map(|c| c.len()).sum::<usize>() > 65535
    {
        bail!("certificate chain exceeds count/byte bounds or is empty");
    }
    Ok(certificates)
}
pub fn roots(path: &Path) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certificates(path)? {
        roots.add(certificate)?;
    }
    Ok(roots)
}
pub fn key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    Ok(PrivateKeyDer::from_pem_slice(&bytes(path, 65536)?)?)
}
pub fn mappings(path: &Path) -> Result<BTreeMap<[u8; 32], IdentityLabel>> {
    let bytes = bytes(path, 1048576)?;
    let text = std::str::from_utf8(&bytes)?;
    let mut lines = text.lines();
    if lines.next() != Some("sha256\tprincipal") {
        bail!("principal map needs sha256<TAB>principal header");
    }
    let mut mappings = BTreeMap::new();
    for line in lines {
        let (fingerprint, owner) = line
            .split_once('\t')
            .context("principal row needs two columns")?;
        let fingerprint = client::hex::<32>(fingerprint)?;
        let owner = IdentityLabel(owner.into());
        owner.validate()?;
        if mappings.len() == 4096 || mappings.insert(fingerprint, owner).is_some() {
            bail!("duplicate fingerprint or too many principal mappings");
        }
    }
    if mappings.is_empty() {
        bail!("at least one mapped principal is required");
    }
    Ok(mappings)
}

struct TrustedSystemClock;
impl Clock for TrustedSystemClock {
    fn read(&self) -> ClockReading {
        match SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|t| u64::try_from(t.as_millis()).ok())
            .filter(|t| *t <= MAX_NUMBER)
        {
            Some(utc) => ClockReading {
                utc_ms: Number(utc),
                trusted: true,
            },
            None => ClockReading {
                utc_ms: Number(0),
                trusted: false,
            },
        }
    }
}
pub struct Access {
    owners: BTreeSet<String>,
    skip: bool,
}
impl Authorization for Access {
    fn permits(&self, owner: &IdentityLabel, permission: Permission) -> bool {
        self.owners.contains(&owner.0) && (permission != Permission::Skip || self.skip)
    }
}

#[derive(Debug, Args)]
pub struct Storage {
    #[arg(long)]
    pub state_db: PathBuf,
    #[arg(long)]
    pub object_dir: PathBuf,
    #[arg(long)]
    pub authority: String,
    #[arg(long)]
    pub principal_map: PathBuf,
    /// Explicitly trust system UTC across restart; forward jumps count as elapsed time.
    #[arg(long)]
    trust_system_clock: bool,
    #[arg(long)]
    allow_skip: bool,
    #[arg(long, default_value_t = 1024)]
    sessions: u64,
    #[arg(long, default_value_t = 10000)]
    payload_objects: u64,
    #[arg(long, default_value_t=8*1024*1024*1024)]
    payload_bytes: u64,
}
pub type Opened = (
    AuthorityStore,
    PayloadStore,
    BTreeMap<[u8; 32], IdentityLabel>,
);
impl Storage {
    pub fn open(&self, fresh: bool) -> Result<Opened> {
        if !self.trust_system_clock {
            bail!(
                "--trust-system-clock is required; a monotonic timer cannot establish UTC across restart"
            );
        }
        let mappings = mappings(&self.principal_map)?;
        let access = Arc::new(Access {
            owners: mappings.values().map(|v| v.0.clone()).collect(),
            skip: self.allow_skip,
        });
        let policy = StorePolicy {
            owners: Id(4096),
            sessions: Id(self.sessions),
            sessions_per_owner: Id(self.sessions.min(64)),
            active_jobs: Id(64),
            active_jobs_per_owner: Id(16),
            session_limits: Limits {
                scopes: Id(4096),
                entities: Id(1000000),
                operations: Id(1000000),
                active_jobs: Id(16),
                retained_input_bytes: Number(1 << 30),
                retained_output_bytes: Number(1 << 30),
            },
        };
        let open = if fresh {
            AuthorityStore::initialize
        } else {
            AuthorityStore::open
        };
        let store = open(
            &self.state_db,
            IdentityLabel(self.authority.clone()),
            policy,
            PhysicalLimits::default(),
            Arc::new(TrustedSystemClock),
            access,
        )?;
        let policy = PayloadPolicy {
            objects: Id(self.payload_objects),
            bytes: Number(self.payload_bytes),
            owner_objects: Id(self.payload_objects),
            owner_bytes: Number(self.payload_bytes),
            chunk_bytes: Id(8192),
            handles: Id(256),
            owner_handles: Id(64),
        };
        let open = if fresh {
            PayloadStore::initialize
        } else {
            PayloadStore::open
        };
        let payloads = open(&self.object_dir, store.payload_identity()?, policy)?;
        store.bind_payloads(&payloads)?;
        Ok((store, payloads, mappings))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_reads_reject_nonregular_symlink_and_oversize_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(bytes(&path, 3).unwrap(), b"abc");
        assert!(bytes(&path, 2).is_err());
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(bytes(&link, 10).is_err());
        assert!(bytes(directory.path(), 10).is_err());
    }
    #[test]
    fn principal_rows_reject_ambiguity_invalid_labels_and_duplicate_fingerprints() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("map");
        let fingerprint = "01".repeat(32);
        for text in [
            String::new(),
            "sha256\tprincipal\n".into(),
            format!("sha256\tprincipal\n{fingerprint}\t\n"),
            format!("sha256\tprincipal\n{fingerprint}\talice\n{fingerprint}\tbob\n"),
            format!("sha256\tprincipal\n{fingerprint}\talice\tadmin\n"),
            "sha256\tprincipal\nzz\talice\n".into(),
        ] {
            std::fs::write(&path, text).unwrap();
            assert!(mappings(&path).is_err());
        }
        std::fs::write(&path, format!("sha256\tprincipal\n{fingerprint}\talice\n")).unwrap();
        assert_eq!(mappings(&path).unwrap().len(), 1);
    }
    #[test]
    fn skip_requires_explicit_permission_and_unknown_owners_are_denied() {
        let mut access = Access {
            owners: BTreeSet::from(["alice".into()]),
            skip: false,
        };
        assert!(access.permits(&IdentityLabel("alice".into()), Permission::Admit));
        assert!(!access.permits(&IdentityLabel("alice".into()), Permission::Skip));
        assert!(!access.permits(&IdentityLabel("bob".into()), Permission::Inspect));
        access.skip = true;
        assert!(access.permits(&IdentityLabel("alice".into()), Permission::Skip));
    }
}
