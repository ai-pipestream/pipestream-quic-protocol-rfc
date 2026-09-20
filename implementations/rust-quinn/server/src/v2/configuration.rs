//! Bounded startup configuration. No credential path or principal is inferred
//! from a result URI. All reads here precede accepting network requests.
use super::*;
use anyhow::Context;
use pipestream_quic::v2::authority::{
    AuthorityStore, Authorization, Clock, ClockReading, Permission, StorePolicy,
    payload::{PayloadBackendKind, PayloadBackendSpec, PayloadPolicy, PayloadStore},
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

fn open_payloads_at(
    object_dir: &Path,
    fresh: bool,
    spec: &PayloadBackendSpec,
    identity: pipestream_quic::persistence::StoreIdentity,
    policy: PayloadPolicy,
) -> Result<PayloadStore> {
    Ok(if fresh {
        PayloadStore::initialize_with_backend(object_dir, identity, policy, spec)?
    } else {
        PayloadStore::open_with_backend(object_dir, identity, policy, spec)?
    })
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
    /// Payload storage backend: "fs" (default stage-file store) or an SPI
    /// plane: "spi-fs" | "spi-postgres" | "spi-s3" | "spi-mongo". The
    /// selection is persisted in the store binding (with a fingerprint of
    /// the connection parameters), so reopening with a different backend or
    /// endpoint is refused. Connection flags fall back to the PROBE_* env
    /// vars used by the cross-language probe.
    #[arg(long, default_value = "fs")]
    payload_backend: String,
    /// PostgreSQL URL for spi-postgres (env: PROBE_PG_URL).
    #[arg(long, env = "PROBE_PG_URL")]
    payload_pg_url: Option<String>,
    /// S3 endpoint for spi-s3 (env: PROBE_S3_ENDPOINT).
    #[arg(long, env = "PROBE_S3_ENDPOINT")]
    payload_s3_endpoint: Option<String>,
    /// S3 bucket for spi-s3 (env: PROBE_S3_BUCKET).
    #[arg(long, env = "PROBE_S3_BUCKET")]
    payload_s3_bucket: Option<String>,
    /// S3 region for spi-s3 (env: PROBE_S3_REGION).
    #[arg(long, env = "PROBE_S3_REGION", default_value = "us-east-1")]
    payload_s3_region: String,
    /// S3 key prefix for spi-s3 (env: PROBE_S3_PREFIX).
    #[arg(long, env = "PROBE_S3_PREFIX", default_value = "payload-plane/")]
    payload_s3_prefix: String,
    /// S3 access key for spi-s3 (env: PROBE_S3_ACCESS_KEY).
    #[arg(long, env = "PROBE_S3_ACCESS_KEY")]
    payload_s3_access_key: Option<String>,
    /// S3 secret key for spi-s3 (env: PROBE_S3_SECRET_KEY).
    #[arg(long, env = "PROBE_S3_SECRET_KEY")]
    payload_s3_secret_key: Option<String>,
    /// MongoDB URI for spi-mongo (env: PROBE_MONGO_URI).
    #[arg(long, env = "PROBE_MONGO_URI")]
    payload_mongo_uri: Option<String>,
    /// MongoDB database for spi-mongo (env: PROBE_MONGO_DATABASE).
    #[arg(long, env = "PROBE_MONGO_DATABASE")]
    payload_mongo_database: Option<String>,
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
    fn backend_spec(&self, kind: PayloadBackendKind) -> Result<PayloadBackendSpec> {
        let need = |value: &Option<String>, flag: &str| -> Result<String> {
            value
                .clone()
                .ok_or_else(|| anyhow::anyhow!("{flag} is required for {kind}"))
        };
        match kind {
            PayloadBackendKind::Filesystem => Ok(PayloadBackendSpec::Filesystem),
            PayloadBackendKind::SpiFilesystem => Ok(PayloadBackendSpec::SpiFilesystem),
            PayloadBackendKind::SpiPostgres => Ok(PayloadBackendSpec::SpiPostgres {
                url: need(&self.payload_pg_url, "--payload-pg-url / PROBE_PG_URL")?,
            }),
            PayloadBackendKind::SpiS3 => Ok(PayloadBackendSpec::SpiS3 {
                config: Box::new(pipestream_storage::S3Config {
                    bucket: need(&self.payload_s3_bucket, "--payload-s3-bucket / PROBE_S3_BUCKET")?,
                    prefix: self.payload_s3_prefix.clone(),
                    endpoint_url: Some(need(
                        &self.payload_s3_endpoint,
                        "--payload-s3-endpoint / PROBE_S3_ENDPOINT",
                    )?),
                    region: self.payload_s3_region.clone(),
                    access_key: need(
                        &self.payload_s3_access_key,
                        "--payload-s3-access-key / PROBE_S3_ACCESS_KEY",
                    )?,
                    secret_key: need(
                        &self.payload_s3_secret_key,
                        "--payload-s3-secret-key / PROBE_S3_SECRET_KEY",
                    )?,
                }),
            }),
            PayloadBackendKind::SpiMongo => Ok(PayloadBackendSpec::SpiMongo {
                uri: need(&self.payload_mongo_uri, "--payload-mongo-uri / PROBE_MONGO_URI")?,
                database: need(
                    &self.payload_mongo_database,
                    "--payload-mongo-database / PROBE_MONGO_DATABASE",
                )?,
            }),
        }
    }

    /// Constructs the payload store. Service SPI backends (postgres, s3,
    /// mongo) build their own tokio runtime during connect, and tokio
    /// refuses to drive a runtime from inside an async worker thread ("Cannot
    /// start a runtime from within a runtime") — so their construction runs
    /// on a dedicated plain thread. This is the only tokio-worker context
    /// the store is constructed in: request-time plane work already runs on
    /// spawn_blocking or dedicated file-worker threads, where block_on is
    /// legal. Local backends stay on the caller's thread.
    fn open_payloads(
        &self,
        fresh: bool,
        spec: &PayloadBackendSpec,
        identity: pipestream_quic::persistence::StoreIdentity,
        policy: PayloadPolicy,
    ) -> Result<PayloadStore> {
        let offloaded = matches!(
            spec,
            PayloadBackendSpec::SpiPostgres { .. }
                | PayloadBackendSpec::SpiS3 { .. }
                | PayloadBackendSpec::SpiMongo { .. }
        );
        if !offloaded {
            return open_payloads_at(&self.object_dir, fresh, spec, identity, policy);
        }
        let object_dir = self.object_dir.clone();
        let spec = spec.clone();
        let handle = std::thread::Builder::new()
            .name("payload-backend-open".to_string())
            .spawn(move || open_payloads_at(&object_dir, fresh, &spec, identity, policy))
            .map_err(|e| anyhow::anyhow!("payload backend thread failed to start: {e}"))?;
        match handle.join() {
            Ok(result) => result,
            Err(_) => anyhow::bail!("payload backend construction panicked"),
        }
    }

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
        let payload_backend: PayloadBackendKind = self
            .payload_backend
            .parse()
            .map_err(|_| anyhow::anyhow!("unknown --payload-backend"))?;
        let spec = self.backend_spec(payload_backend)?;
        let store = if fresh {
            AuthorityStore::initialize_with_payload_backend(
                &self.state_db,
                IdentityLabel(self.authority.clone()),
                policy,
                PhysicalLimits::default(),
                Arc::new(TrustedSystemClock),
                access,
                payload_backend,
            )?
        } else {
            AuthorityStore::open_with_payload_backend(
                &self.state_db,
                IdentityLabel(self.authority.clone()),
                policy,
                PhysicalLimits::default(),
                Arc::new(TrustedSystemClock),
                access,
                payload_backend,
            )?
        };
        let policy = PayloadPolicy {
            objects: Id(self.payload_objects),
            bytes: Number(self.payload_bytes),
            owner_objects: Id(self.payload_objects),
            owner_bytes: Number(self.payload_bytes),
            chunk_bytes: Id(8192),
            handles: Id(256),
            owner_handles: Id(64),
        };
        let payloads = self.open_payloads(fresh, &spec, store.payload_identity()?, policy)?;
        store.bind_payloads(&payloads)?;
        Ok((store, payloads, mappings))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        storage: Storage,
    }

    #[test]
    fn payload_backend_defaults_to_fs_and_is_selectable() {
        let base = [
            "probe",
            "--state-db",
            "state.sqlite",
            "--object-dir",
            "objects",
            "--authority",
            "issuer",
            "--principal-map",
            "principals.tsv",
        ];
        let storage = TestCli::try_parse_from(base).unwrap().storage;
        assert_eq!(storage.payload_backend, "fs");
        let storage = TestCli::try_parse_from(
            base.iter()
                .chain(["--payload-backend", "fs"].iter())
                .copied(),
        )
        .unwrap()
        .storage;
        assert_eq!(
            storage.payload_backend.parse::<PayloadBackendKind>().unwrap(),
            PayloadBackendKind::Filesystem
        );
    }

    #[test]
    fn spi_service_backends_require_connection_config() {
        let base = [
            "probe",
            "--state-db",
            "state.sqlite",
            "--object-dir",
            "objects",
            "--authority",
            "issuer",
            "--principal-map",
            "principals.tsv",
        ];
        // spi-postgres without a URL parses fine (flags are optional) but the
        // spec builder refuses it.
        let storage = TestCli::try_parse_from(
            base.iter()
                .chain(["--payload-backend", "spi-postgres"].iter())
                .copied(),
        )
        .unwrap()
        .storage;
        assert!(
            storage
                .backend_spec(PayloadBackendKind::SpiPostgres)
                .is_err()
        );
        let storage = TestCli::try_parse_from(
            base.iter()
                .chain(
                    [
                        "--payload-backend",
                        "spi-postgres",
                        "--payload-pg-url",
                        "postgres://u:p@127.0.0.1:1/db",
                    ]
                    .iter(),
                )
                .copied(),
        )
        .unwrap()
        .storage;
        assert!(
            storage
                .backend_spec(PayloadBackendKind::SpiPostgres)
                .is_ok()
        );
        // Env fallback: PROBE_PG_URL satisfies the requirement.
        unsafe {
            std::env::set_var("PROBE_PG_URL", "postgres://u:p@127.0.0.1:1/db");
        }
        let storage = TestCli::try_parse_from(
            base.iter()
                .chain(["--payload-backend", "spi-postgres"].iter())
                .copied(),
        )
        .unwrap()
        .storage;
        assert!(
            storage
                .backend_spec(PayloadBackendKind::SpiPostgres)
                .is_ok()
        );
        unsafe {
            std::env::remove_var("PROBE_PG_URL");
        }
    }

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
