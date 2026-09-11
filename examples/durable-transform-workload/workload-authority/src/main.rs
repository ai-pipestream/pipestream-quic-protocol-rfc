//! External durable-transform worker authority.
//!
//! A real PipeStream authority executable built only on public library APIs
//! (`pipestream-core` state machines via `pipestream-quinn` transport). It
//! registers one application contract, `transform/v2` (mode 0): the frozen
//! C1 byte transform over the admitted input, streamed with bounded buffers.
//! It is not a plugin inside the shipped CLI's fixed registry.
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use pipestream_quic::{
    persistence::PhysicalLimits,
    v2::*,
    v2_authority::{
        Authority,
        server::{Options, Server},
    },
    v2_tls::{ClientAuthentication, ServerSecurity},
};
use pipestream_quic::v2::authority::{
    AuthorityStore, Authorization, Clock, ClockReading, Permission, StoreError, StorePolicy,
    execution::{Application, ApplicationOutcome, WorkContext},
    ingress::{Applications, RestartSafety},
    payload::{PayloadPolicy, PayloadStore},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{Read, Write},
    net::SocketAddr,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use workload_core::transform_byte;

/// Frozen application label for the C1 transform, mode 0 leaf.
const TRANSFORM_LABEL: &str = "transform/v2";

/// Pure streaming transform over the admitted input. Chunk-relative offsets
/// restart at zero per work item; restart re-execution yields identical bytes.
/// `wrong` enables a TEST-ONLY fault (negative-control runs): bytes pass
/// through untransformed so the coordinator oracle rejects them. Never set
/// in measured cells; every use is recorded by run-negative.sh.
struct Transform {
    wrong: bool,
    delay_ms: u64,
}
impl Application for Transform {
    fn execute(&self, context: &mut WorkContext) -> std::result::Result<ApplicationOutcome, StoreError> {
        // TEST-ONLY slow worker: burn time in small steps, renewing the
        // lease each step so the delay never becomes a lease expiry.
        let mut remaining = self.delay_ms;
        while remaining > 0 {
            let step = remaining.min(100);
            std::thread::sleep(std::time::Duration::from_millis(step));
            context.renew()?;
            remaining -= step;
        }
        let input = context.input_descriptor().clone();
        context.begin_output(input.length, input.content_type)?;
        let mut bytes = [0; 8192];
        let mut staged = [0; 8192];
        let limit = bytes.len().min(context.buffer_limit());
        let mut offset: u64 = 0;
        loop {
            let count = context.read_input(&mut bytes[..limit])?;
            if count == 0 {
                break;
            }
            for (i, &b) in bytes[..count].iter().enumerate() {
                staged[i] = if self.wrong {
                    b
                } else {
                    transform_byte(b, offset + i as u64)
                };
            }
            context.write_output(&staged[..count])?;
            offset += count as u64;
            context.renew()?;
        }
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}

fn registry(wrong: bool, delay_ms: u64) -> Result<Arc<Applications>> {
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel(TRANSFORM_LABEL.into()),
        vec![Mode(0)],
        RestartSafety::Pure,
        Arc::new(Transform { wrong, delay_ms }),
    )?;
    Ok(Arc::new(apps))
}

fn bytes(path: &Path, maximum: usize) -> Result<Vec<u8>> {
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

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
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

fn roots(path: &Path) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certificates(path)? {
        roots.add(certificate)?;
    }
    Ok(roots)
}

fn key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    Ok(PrivateKeyDer::from_pem_slice(&bytes(path, 65536)?)?)
}

fn hex32(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        bail!("fingerprint must be 64 hex digits");
    }
    let mut out = [0u8; 32];
    for (i, chunk) in text.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).context("hex")?, 16)
            .context("hex digits")?;
    }
    Ok(out)
}

fn mappings(path: &Path) -> Result<BTreeMap<[u8; 32], IdentityLabel>> {
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
        let fingerprint = hex32(fingerprint)?;
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

struct Access {
    owners: BTreeSet<String>,
}
impl Authorization for Access {
    fn permits(&self, owner: &IdentityLabel, permission: Permission) -> bool {
        self.owners.contains(&owner.0) && permission != Permission::Skip
    }
}

#[derive(Debug, Args)]
struct Storage {
    #[arg(long)]
    state_db: PathBuf,
    #[arg(long)]
    object_dir: PathBuf,
    #[arg(long)]
    authority: String,
    #[arg(long)]
    principal_map: PathBuf,
    #[arg(long)]
    trust_system_clock: bool,
    #[arg(long, default_value_t = 1024)]
    sessions: u64,
    #[arg(long, default_value_t = 10000)]
    payload_objects: u64,
    #[arg(long, default_value_t=8*1024*1024*1024)]
    payload_bytes: u64,
    /// Physical SQLite database file cap in MiB. Funds record growth for
    /// large corpora; defaults preserve historical behavior.
    #[arg(long, default_value_t = 256)]
    db_mib: u64,
    /// Physical SQLite WAL file cap in MiB.
    #[arg(long, default_value_t = 64)]
    wal_mib: u64,
}

/// Physical file-length funding from CLI MiB caps. (256, 64) reproduces
/// `PhysicalLimits::default()` exactly.
fn physical_limits(db_mib: u64, wal_mib: u64) -> Result<PhysicalLimits> {
    let limits = PhysicalLimits {
        database_bytes: db_mib
            .checked_mul(1 << 20)
            .filter(|n| *n >= 65536 && *n <= (16 << 30) && *n % 65536 == 0)
            .context("db-mib out of funded range")?,
        wal_bytes: wal_mib
            .checked_mul(1 << 20)
            .filter(|n| *n >= 65536 && *n <= (16 << 30) && *n % 65536 == 0)
            .context("wal-mib out of funded range")?,
        ..PhysicalLimits::default()
    };
    Ok(limits)
}

impl Storage {
    fn open(&self, fresh: bool) -> Result<(AuthorityStore, PayloadStore, BTreeMap<[u8; 32], IdentityLabel>)> {
        if !self.trust_system_clock {
            bail!(
                "--trust-system-clock is required; a monotonic timer cannot establish UTC across restart"
            );
        }
        let mappings = mappings(&self.principal_map)?;
        let access = Arc::new(Access {
            owners: mappings.values().map(|v| v.0.clone()).collect(),
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
            physical_limits(self.db_mib, self.wal_mib)?,
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

#[derive(Debug, Args)]
struct Network {
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    cert: PathBuf,
    #[arg(long)]
    key: PathBuf,
    #[arg(long)]
    client_ca: PathBuf,
    #[arg(long)]
    result_authority: String,
    #[arg(long)]
    ready_file: Option<PathBuf>,
    #[arg(long, default_value_t = 16*1024*1024)]
    object_limit: u64,
    /// TEST-ONLY: pass admitted bytes through untransformed so the
    /// coordinator oracle rejects them (negative-control runs).
    #[arg(long, default_value_t = false)]
    test_wrong_transform: bool,
    /// TEST-ONLY: sleep this many ms per executed chunk while renewing the
    /// lease (slow-worker arm). Must stay far below execution deadlines;
    /// every use is recorded by run-slow.sh.
    #[arg(long, default_value_t = 0)]
    test_work_delay_ms: u64,
}

async fn serve(storage: Storage, network: Network) -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let (store, payloads, principals) = storage.open(false)?;
    let authentication = ClientAuthentication::new(
        IdentityLabel(storage.authority),
        roots(&network.client_ca)?,
        principals,
        Arc::new(rustls::time_provider::DefaultTimeProvider),
    )?;
    let security = ServerSecurity::new(
        certificates(&network.cert)?,
        key(&network.key)?,
        Some(Arc::new(authentication)),
    )?;
    let authority = Authority::new(store, payloads, 4)?;
    let mut options = Options::default();
    options.offer.object_limit = Number(network.object_limit);
    if network.test_wrong_transform {
        eprintln!("TEST-ONLY test-wrong-transform enabled: outputs will fail verification");
    }
    if network.test_work_delay_ms > 0 {
        eprintln!(
            "TEST-ONLY test-work-delay-ms enabled: {} ms per chunk",
            network.test_work_delay_ms
        );
    }
    let server = Server::bind(
        network.bind,
        security,
        authority,
        registry(network.test_wrong_transform, network.test_work_delay_ms)?,
        authority::execution::ResultEndpoint::new(network.result_authority)?,
        options,
    )?;
    let address = server.local_addr()?;
    if let Some(path) = network.ready_file {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        writeln!(file, "{address}")?;
        file.sync_all()?;
    }
    println!("LISTENING {address}");
    let report = server
        .run(async {
            tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        })
        .await?;
    if !report.drained() || report.fault.is_some() {
        bail!("server shutdown did not fully drain: {report:?}");
    }
    println!("DRAINED");
    Ok(())
}

#[derive(Debug, Parser)]
#[command(about = "Durable-transform worker authority (transform/v2)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    InitAuthority(Storage),
    Serve {
        #[command(flatten)]
        storage: Storage,
        #[command(flatten)]
        network: Network,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::InitAuthority(storage) => {
            storage.open(true)?;
            println!("INITIALIZED");
            Ok(())
        }
        Command::Serve { storage, network } => serve(storage, network).await,
    }
}

#[cfg(test)]
mod funding_tests {
    use super::*;

    #[test]
    fn defaults_reproduce_historical_funding() {
        assert_eq!(
            physical_limits(256, 64).unwrap(),
            PhysicalLimits::default()
        );
    }

    #[test]
    fn larger_funding_maps_mib_to_bytes() {
        let limits = physical_limits(1024, 256).unwrap();
        assert_eq!(limits.database_bytes, 1024 << 20);
        assert_eq!(limits.wal_bytes, 256 << 20);
        // Untouched lanes stay at default.
        assert_eq!(
            limits.journal_bytes,
            PhysicalLimits::default().journal_bytes
        );
    }

    #[test]
    fn zero_and_overflow_funding_rejected() {
        assert!(physical_limits(0, 64).is_err());
        assert!(physical_limits(256, 0).is_err());
        assert!(physical_limits(u64::MAX, 64).is_err());
    }
}
