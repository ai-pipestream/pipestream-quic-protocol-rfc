//! V2 process ownership. The fixture spawns every subject process, kills only
//! what it spawned, and reaps on every path. Readiness is a non-empty
//! ready file plus a successful authenticated client op — a live process
//! alone is not readiness.

use crate::durable::mtls::{AUTHORITY, Identity, Material};
use crate::{ensure_success, path, run_output_owned, unique_suffix, wait_output};
use anyhow::{Context, Result, bail, ensure};
use std::{
    fs,
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

// 30s so a cold JVM server can still become ready inside the window.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Frozen JVM memory limits for every Java subject process the fixture
/// spawns. Group R requires the limits and the environment allowance to be
/// FIXED before a decisive measurement run, so they live here as one constant
/// rather than per row: the same flags apply to client-side and server-side
/// Java processes in every direction, and every row's evidence can name them.
///
/// `-Xmx2g` bounds the heap (and, because the JVM's default direct-memory
/// ceiling tracks max heap, the direct scope with it). `-Xms256m` is
/// deliberately far below the ceiling: an -Xms equal to -Xmx would commit the
/// plateau up front and make the RSS plateau assertion pass trivially, which
/// is a false pass, not evidence. The value is recorded into run.tsv, the
/// r-capability-manifest row and every R row's observed.tsv.
pub const JAVA_MEMORY_FLAGS: &[&str] = &["-Xms256m", "-Xmx2g"];

/// Client control deadline (milliseconds) the fixture passes to the Java
/// client on kill rows as `--control-timeout-ms`. A scheduled server kill
/// leaves the client's pending request drain-waiting: the connection's idle
/// timeout is the stream lifetime, so without a shortened control deadline
/// the client returns exactly at its default 30 s — the driver's own op
/// bound (observation O-2, Java handoff; ClientCommands.java at 3e1547dd).
/// 10 s is comfortably inside the 30 s bound and above every legitimate
/// loopback op latency. The Rust client CLI exposes no such option (its
/// transport `response_timeout` has no CLI surface), so the flag is emitted
/// for Java clients only; `client_command` enforces that.
pub const JAVA_CLIENT_KILL_CONTROL_TIMEOUT_MS: u64 = 10_000;

/// Human-readable form of [`JAVA_MEMORY_FLAGS`] for evidence files.
pub fn java_memory_flags_text() -> String {
    JAVA_MEMORY_FLAGS.join(" ")
}
const OP_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Kill-row client op budget. A scheduled server kill is noticed at the
/// NEGOTIATED TRANSPORT BOUND, not the application control deadline: the
/// Java client's idle timeout is max(handshake 10 s, control deadline,
/// stream lifetime 300 s) (DurableClient.java:203-207) and the Rust
/// authority sets max_idle_timeout(60 s) (quinn/src/v2_authority/server.rs:
/// 225), so QUIC negotiates 60 s and the Java client surfaces CONTROL_RESET
/// "connection ended before drain" at ~61 s (DurableClient.java:271-278) —
/// while the Rust client's per-request response_timeout is 60 s
/// (quinn/src/v2_client/transport.rs:127, swept at :538), refusing LIMIT_
/// EXCEEDED "client response deadline" at ~60 s. Both exceed the driver's
/// default 30 s op wait; 90 s observes the named refusal without a guessed
/// sleep and leaves the row assertions untouched.
pub const KILL_ROW_OP_TIMEOUT: Duration = Duration::from_secs(90);

/// The per-run scratch directory every spawned Java subject is pointed at.
/// HotSpot ignores the TMPDIR environment variable (java.io.tmpdir defaults
/// to /tmp), so without this each JVM extracts sqlite-jdbc's libsqlitejdbc
/// plus a .lck under /tmp (a FRESH uuid whenever the previous JVM was
/// killed and left the lock - the kill rows do this constantly), writes
/// hsperfdata_<user>, and ClientCommands drops its per-invocation
/// pipestream-v2-sequence-*.sqlite scratch there: the /tmp flood the owner
/// hit. Keyed by the run id so a run's subjects share one directory and
/// `run()` can remove exactly it afterwards.
pub fn java_tmp_dir(run_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!("pipestream-java-{run_id}"))
}

/// Test-only fixture-hook arming for one server process (milestone-5 subject
/// flags). The driver writes the schedule TSV before starting the server;
/// arming is per-process, so a restart re-arms with the rows it has not yet
/// consumed. Rust server only: the Java server has no published fixture hook
/// (Claude's FixtureMain), so arming a Java server is a fixture error.
#[derive(Clone)]
pub struct FixtureArming {
    pub events: PathBuf,
    pub run_id: String,
    pub scenario_id: String,
    pub schedule: Option<PathBuf>,
}

/// Which subject implementation a side of the pair runs. Everything is
/// still driven by spawning published binaries; the driver never links
/// against a subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subject {
    Rust,
    Java,
}

/// The storage plane a run's authority storage runs on. Both subject types
/// gained an optional plane selection on their storage branches (not merged
/// here), at the same seam as their authority storage arguments: Java spells
/// it `--object-plane v2|spi` on `init-authority` and `serve`
/// (V2Main/FixtureMain), Rust spells it `--payload-backend fs|spi-fs` on the
/// shared Storage args of `init-authority` and `serve` (server/src/v2.rs,
/// configuration::Storage). The driver emits the subject's own flag only
/// when a run selects SPI, so a default run's spawned command lines stay
/// byte-identical to a run against the merged subjects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum StoragePlane {
    /// The merged v2 envelope-keyed store; no plane flag is passed.
    #[default]
    #[value(name = "v2")]
    V2,
    /// The storage-SPI object plane; requires the subjects' storage-branch
    /// builds, which carry the flag.
    #[value(name = "spi")]
    Spi,
}

impl StoragePlane {
    /// Run-manifest (run.tsv) spelling of the selection.
    pub fn name(self) -> &'static str {
        match self {
            Self::V2 => "v2",
            Self::Spi => "spi",
        }
    }
}

/// The SPI object backend a run's subjects store object payloads on. Both
/// subject types gained the selection on their storage branches (not merged
/// here): Java spells it `--spi-backend fs|postgres|s3|mongo` on
/// `init-authority` and `serve` (V2Main/FixtureMain, and a non-fs selection
/// requires `--object-plane spi`); Rust spells the plane and backend as one
/// `--payload-backend fs|spi-fs|spi-postgres|spi-s3|spi-mongo` value on the
/// shared Storage args (server/src/v2.rs, configuration::Storage). `fs` (the
/// default) is today's stage-file store; a non-fs backend implies the SPI
/// plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum StorageBackend {
    /// The local stage-file store (the merged default).
    #[default]
    #[value(name = "fs")]
    Fs,
    #[value(name = "postgres")]
    Postgres,
    #[value(name = "s3")]
    S3,
    #[value(name = "mongo")]
    Mongo,
}

impl StorageBackend {
    /// Run-manifest (run.tsv) spelling of the selection; also the Java
    /// `--spi-backend` value.
    pub fn name(self) -> &'static str {
        match self {
            Self::Fs => "fs",
            Self::Postgres => "postgres",
            Self::S3 => "s3",
            Self::Mongo => "mongo",
        }
    }
}

/// One PROBE_* coordinate convention and where it surfaces on each subject.
/// `java_flag`/`rust_flag` are empty when the coordinate is secret (or has
/// no subject flag): secrets travel through the inherited driver environment
/// only, never through a spawned command line (spawn failures log the joined
/// command, so a flag there would leak into run evidence).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinateSpec {
    pub probe: &'static str,
    pub java_flag: &'static str,
    pub rust_flag: &'static str,
    pub required: bool,
}

/// The postgres coordinate set. The Rust subject takes credentials inside
/// the URL; the Java subject resolves PROBE_PG_USER/PROBE_PG_PASSWORD from
/// the environment at open time.
const POSTGRES_COORDINATES: &[CoordinateSpec] = &[
    CoordinateSpec {
        probe: "PROBE_PG_URL",
        java_flag: "--spi-pg-url",
        rust_flag: "--payload-pg-url",
        required: true,
    },
    CoordinateSpec {
        probe: "PROBE_PG_USER",
        java_flag: "",
        rust_flag: "",
        required: false,
    },
    CoordinateSpec {
        probe: "PROBE_PG_PASSWORD",
        java_flag: "",
        rust_flag: "",
        required: false,
    },
];

/// The S3 coordinate set. Region and prefix are optional on both subjects
/// (each carries its own default); access/secret keys are required but
/// env-only.
const S3_COORDINATES: &[CoordinateSpec] = &[
    CoordinateSpec {
        probe: "PROBE_S3_ENDPOINT",
        java_flag: "--spi-s3-endpoint",
        rust_flag: "--payload-s3-endpoint",
        required: true,
    },
    CoordinateSpec {
        probe: "PROBE_S3_BUCKET",
        java_flag: "--spi-s3-bucket",
        rust_flag: "--payload-s3-bucket",
        required: true,
    },
    CoordinateSpec {
        probe: "PROBE_S3_REGION",
        java_flag: "--spi-s3-region",
        rust_flag: "--payload-s3-region",
        required: false,
    },
    CoordinateSpec {
        probe: "PROBE_S3_PREFIX",
        java_flag: "--spi-s3-prefix",
        rust_flag: "--payload-s3-prefix",
        required: false,
    },
    CoordinateSpec {
        probe: "PROBE_S3_ACCESS_KEY",
        java_flag: "",
        rust_flag: "",
        required: true,
    },
    CoordinateSpec {
        probe: "PROBE_S3_SECRET_KEY",
        java_flag: "",
        rust_flag: "",
        required: true,
    },
];

/// The MongoDB coordinate set.
const MONGO_COORDINATES: &[CoordinateSpec] = &[
    CoordinateSpec {
        probe: "PROBE_MONGO_URI",
        java_flag: "--spi-mongo-uri",
        rust_flag: "--payload-mongo-uri",
        required: true,
    },
    CoordinateSpec {
        probe: "PROBE_MONGO_DATABASE",
        java_flag: "--spi-mongo-database",
        rust_flag: "--payload-mongo-database",
        required: true,
    },
];

/// The coordinate conventions for one backend (empty for fs).
pub fn backend_coordinates(backend: StorageBackend) -> &'static [CoordinateSpec] {
    match backend {
        StorageBackend::Fs => &[],
        StorageBackend::Postgres => POSTGRES_COORDINATES,
        StorageBackend::S3 => S3_COORDINATES,
        StorageBackend::Mongo => MONGO_COORDINATES,
    }
}

/// Resolve the coordinate set for a backend from the driver's environment
/// (PROBE_* conventions; run scripts own the service lifecycle and export
/// these). Every required coordinate must be present — a non-fs selection
/// with missing coordinates is a usage error, surfaced before anything is
/// spawned. `lookup` abstracts the environment for testing. The result holds
/// the non-secret coordinates only, as `(PROBE name, value)` pairs in
/// convention order; secrets stay in the environment the subjects inherit.
pub fn resolve_backend_coordinates(
    backend: StorageBackend,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Vec<(String, String)>> {
    let mut coordinates = Vec::new();
    let mut missing = Vec::new();
    for spec in backend_coordinates(backend) {
        match lookup(spec.probe) {
            Some(value) if !spec.java_flag.is_empty() => {
                coordinates.push((spec.probe.to_owned(), value));
            }
            Some(_) => {}
            None if spec.required => missing.push(spec.probe),
            None => {}
        }
    }
    ensure!(
        missing.is_empty(),
        "storage backend `{}` is missing required coordinate environment: {}; run scripts own \
         the service lifecycle and export the PROBE_* conventions before the driver starts (the \
         driver never starts services)",
        backend.name(),
        missing.join(", ")
    );
    Ok(coordinates)
}

/// The effective storage plane for a run: an explicit selection wins, `fs`
/// defaults to the merged v2 plane, and a non-fs backend implies SPI. An
/// explicit `--storage-plane v2` with a non-fs backend is a usage error —
/// both subjects refuse that combination (Java: "an SPI backend selection
/// requires --object-plane spi"; Rust persists the plane in the store
/// binding), so the driver fails before spawning anything.
pub fn effective_plane(
    plane: Option<StoragePlane>,
    backend: StorageBackend,
) -> Result<StoragePlane> {
    match (plane, backend) {
        (Some(StoragePlane::V2), backend) if backend != StorageBackend::Fs => bail!(
            "--storage-plane v2 conflicts with --storage-backend {}: a non-fs backend requires \
             the spi plane",
            backend.name()
        ),
        (Some(plane), _) => Ok(plane),
        (None, StorageBackend::Fs) => Ok(StoragePlane::V2),
        (None, _) => Ok(StoragePlane::Spi),
    }
}

/// The driver-level storage selection one run executes with: the effective
/// plane, the backend, and the resolved non-secret coordinates (PROBE name
/// to value). Carried by the `ScenarioContext` so no row can start a
/// subject on a different selection than the run recorded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageSelection {
    pub plane: StoragePlane,
    pub backend: StorageBackend,
    pub coordinates: Vec<(String, String)>,
}

impl Subject {
    pub fn name(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Java => "java",
        }
    }

    /// Marker printed on stdout by a successful authority initialization.
    fn initialized_marker(self) -> &'static str {
        // Rust prints "AUTHORITY_INITIALIZED <label>"; Java V2Main prints
        // "INITIALIZED <root>" (api-plan.md section 1.5).
        match self {
            Self::Rust => "AUTHORITY_INITIALIZED",
            Self::Java => "INITIALIZED",
        }
    }

    /// Marker printed on stdout by a successful client journal initialization.
    pub fn client_initialized_marker(self) -> &'static str {
        // Rust prints "CLIENT_INITIALIZED"; Java ClientCommands.initClient
        // prints "INITIALIZED <intent>" (ClientCommands.java:146).
        match self {
            Self::Rust => "CLIENT_INITIALIZED",
            Self::Java => "INITIALIZED",
        }
    }
}

/// Everything one scenario needs to own a V2 authority: storage roots,
/// credentials, and the subject entry points. `init-authority` has been run
/// against these roots before `start_server` is used.
#[derive(Clone)]
pub struct AuthorityFixture {
    pub rust_bin: PathBuf,
    pub java_jar: Option<PathBuf>,
    pub server: Subject,
    pub client: Subject,
    pub root: PathBuf,
    pub state_db: PathBuf,
    pub object_dir: PathBuf,
    pub certs: Material,
    /// Extra flags appended to every `serve` invocation after the fixed
    /// arguments. G4 skip rows pass `--allow-skip` (a rust Storage open flag
    /// and a java serve flag); empty for every other fixture.
    extra_serve_args: Vec<String>,
    /// Kill-row client control deadline (see
    /// [`JAVA_CLIENT_KILL_CONTROL_TIMEOUT_MS`]); `None` for every fixture
    /// but the hooked kill rows.
    client_control_timeout_ms: Option<u64>,
    /// Per-op wait for client commands this fixture spawns. The driver's
    /// default 30 s for every row but the kill rows, which widen it to
    /// [`KILL_ROW_OP_TIMEOUT`] so a killed authority's named refusal at the
    /// ~60 s transport bound is observed instead of raced.
    client_op_timeout: Duration,
    /// Authority label the storage roots are initialised with and every
    /// journal binds to. [`AUTHORITY`] for every fixture but the second
    /// authority of g5-cross-authority-reference.
    authority: String,
    /// The durable run this fixture belongs to; keys the Java subjects'
    /// tmp directory (see [`java_tmp_dir`]).
    run_id: String,
    /// Storage plane/backend selection with the resolved non-secret
    /// coordinates. The default (v2 plane, fs backend, no coordinates)
    /// emits nothing, so every spawned command line stays byte-identical
    /// to a run against the merged subjects; SPI selections append the
    /// subject's own plane/backend flags plus coordinate flags to every
    /// storage-opening invocation (see [`AuthorityFixture::storage_args`]).
    storage: StorageSelection,
}

impl AuthorityFixture {
    pub fn new(
        rust_bin: &Path,
        java_jar: Option<&Path>,
        root: &Path,
        certs: Material,
        server: Subject,
        client: Subject,
        run_id: String,
    ) -> Result<Self> {
        fs::create_dir_all(root)?;
        Ok(Self {
            rust_bin: rust_bin.to_path_buf(),
            java_jar: java_jar.map(Path::to_path_buf),
            root: root.to_path_buf(),
            state_db: root.join("authority.sqlite"),
            object_dir: root.join("objects"),
            certs,
            server,
            client,
            extra_serve_args: Vec::new(),
            client_control_timeout_ms: None,
            client_op_timeout: OP_TIMEOUT,
            authority: AUTHORITY.to_owned(),
            run_id,
            storage: StorageSelection::default(),
        })
    }

    /// Run the server subject's authority storage with the given plane,
    /// backend and resolved coordinates: the subject's own plane/backend
    /// flags plus coordinate flags are appended to `init-authority`, every
    /// `serve` start, and the other storage-opening invocations built from
    /// [`AuthorityFixture::storage_args`]. Requires the subjects'
    /// storage-branch builds; the default [`StorageSelection`] (v2 plane,
    /// fs backend) emits nothing.
    pub fn with_storage(mut self, storage: StorageSelection) -> Self {
        self.storage = storage;
        self
    }

    /// Shorten the client control deadline on a kill row so the client
    /// notices a killed authority well inside the driver's 30 s op bound.
    /// Emitted as `--control-timeout-ms` for the Java client only.
    pub fn with_client_control_timeout_ms(mut self, timeout_ms: Option<u64>) -> Self {
        self.client_control_timeout_ms = timeout_ms;
        self
    }

    /// Widen the per-op wait for this fixture's client commands; the kill
    /// rows pass [`KILL_ROW_OP_TIMEOUT`] (see its documentation).
    pub fn with_client_op_timeout(mut self, timeout: Duration) -> Self {
        self.client_op_timeout = timeout;
        self
    }

    /// Append flags to every `serve` invocation this fixture starts (used by
    /// the G4 skip rows to arm `--allow-skip` on both subjects).
    pub fn with_extra_serve_args(mut self, args: &[&str]) -> Self {
        self.extra_serve_args = args.iter().map(|arg| (*arg).to_owned()).collect();
        self
    }

    /// Initialise and serve these roots under another authority label
    /// (g5-cross-authority-reference: authority Y is `issuer-b`).
    pub fn with_authority(mut self, authority: &str) -> Self {
        self.authority = authority.to_owned();
        self
    }

    /// The authority label these roots carry.
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// Create the Java subjects' run-keyed tmp directory if this fixture
    /// drives a Java subject. Idempotent; called from both entry points so
    /// server and client spawns alike find it (base() always runs at
    /// init-authority time, before any client, but client_base() creates it
    /// too so no ordering assumption is load-bearing).
    fn ensure_java_tmp(&self) -> Result<()> {
        if self.server == Subject::Java || self.client == Subject::Java {
            fs::create_dir_all(java_tmp_dir(&self.run_id))
                .context("create the Java subjects' tmp directory")?;
        }
        Ok(())
    }

    fn subject_base(&self, subject: Subject) -> Result<Vec<String>> {
        Ok(match subject {
            Subject::Rust => vec![path(&self.rust_bin), "v2".to_owned()],
            Subject::Java => {
                let jar = self
                    .java_jar
                    .as_ref()
                    .context("Java subject requested but no --java-jar was provided")?;
                let mut base = vec!["java".to_owned()];
                // Frozen before any measurement run; see JAVA_MEMORY_FLAGS.
                base.extend(JAVA_MEMORY_FLAGS.iter().map(|flag| (*flag).to_owned()));
                // HotSpot ignores TMPDIR; point the JVM and sqlite-jdbc at
                // the run-keyed scratch directory explicitly (before -cp).
                let tmp = java_tmp_dir(&self.run_id);
                base.push(format!("-Djava.io.tmpdir={}", path(&tmp)));
                base.push(format!("-Dorg.sqlite.tmpdir={}", path(&tmp)));
                base.extend([
                    "--enable-native-access=ALL-UNNAMED".to_owned(),
                    "-cp".to_owned(),
                    path(jar),
                    "ai.pipestream.quic.v2.V2Main".to_owned(),
                ]);
                base
            }
        })
    }

    /// Entry-point prefix for the fixture's server subject.
    pub fn base(&self) -> Result<Vec<String>> {
        self.ensure_java_tmp()?;
        self.subject_base(self.server)
    }

    /// Entry-point prefix for the fixture's client subject.
    pub fn client_base(&self) -> Result<Vec<String>> {
        self.ensure_java_tmp()?;
        self.subject_base(self.client)
    }

    /// Authority storage arguments. Rust takes the paired DB/object
    /// directories explicitly; Java takes the root that contains both
    /// (api-plan.md section 1.5: fixed layout root/authority.sqlite +
    /// root/objects, byte-for-byte paired like Rust).
    ///
    /// This is the single choke point for the storage-plane/backend
    /// selection too: every storage-opening invocation builds from here —
    /// `init-authority`, every `serve` start (including the g3
    /// duplicate-server probe and the hooked FixtureMain path), and the
    /// offline `v2 revoke` — so on an SPI run they all name the same
    /// backend and coordinates, and a v2 run emits no storage flag at all.
    /// Coordinate flags carry non-secret values only; secrets stay in the
    /// PROBE_* environment the spawned subjects inherit.
    pub fn storage_args(&self) -> Vec<String> {
        let mut args = match self.server {
            Subject::Rust => vec![
                "--state-db".into(),
                path(&self.state_db),
                "--object-dir".into(),
                path(&self.object_dir),
            ],
            Subject::Java => vec!["--root".into(), path(&self.root)],
        };
        args.extend([
            "--authority".into(),
            self.authority.clone(),
            "--principal-map".into(),
            path(&self.certs.principal_map),
            "--trust-system-clock".into(),
        ]);
        if self.storage.plane == StoragePlane::Spi {
            // The subjects spell the plane and backend differently (Java
            // `--object-plane spi --spi-backend <kind>`, Rust
            // `--payload-backend spi[-<kind>]`), verified against the
            // unmerged storage branches; both accept the flags on
            // init-authority and serve.
            match self.server {
                Subject::Java => {
                    args.extend(["--object-plane".into(), "spi".into()]);
                    args.extend(["--spi-backend".into(), self.storage.backend.name().into()]);
                }
                Subject::Rust => {
                    let backend = match self.storage.backend {
                        StorageBackend::Fs => "spi-fs",
                        StorageBackend::Postgres => "spi-postgres",
                        StorageBackend::S3 => "spi-s3",
                        StorageBackend::Mongo => "spi-mongo",
                    };
                    args.extend(["--payload-backend".into(), backend.into()]);
                }
            }
            for spec in backend_coordinates(self.storage.backend) {
                let flag = match self.server {
                    Subject::Java => spec.java_flag,
                    Subject::Rust => spec.rust_flag,
                };
                if flag.is_empty() {
                    continue;
                }
                if let Some((_, value)) = self
                    .storage
                    .coordinates
                    .iter()
                    .find(|(name, _)| name == spec.probe)
                {
                    args.extend([flag.into(), value.clone()]);
                }
            }
        }
        args
    }

    /// The full `init-authority` command line this fixture spawns. Kept
    /// separate from [`AuthorityFixture::run_init_authority`] so the exact
    /// invocation (including the storage-plane flag) is unit-testable
    /// without a subject binary.
    fn init_authority_command(&self) -> Result<Vec<String>> {
        let mut command = with_owned(&self.base()?, &["init-authority".into()]);
        command.extend(self.storage_args());
        if self.server == Subject::Java {
            // Java's init-authority validates the result-locator authority up
            // front (V2Main.configuration requires --result-authority); the
            // Rust clap definition has no such flag on init-authority.
            command.extend(["--result-authority".into(), "localhost:7443".into()]);
        }
        Ok(command)
    }

    pub fn run_init_authority(&self) -> Result<()> {
        let command = self.init_authority_command()?;
        let output = run_output_owned(&self.root, &command, OP_TIMEOUT)?;
        ensure_success(&output, "v2 init-authority")?;
        ensure!(
            String::from_utf8_lossy(&output.stdout).contains(self.server.initialized_marker()),
            "init-authority did not confirm initialization\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    pub fn connection_args(&self, server: &OwnedServer, principal: &str) -> Result<Vec<String>> {
        let identity = self.certs.principal(principal)?.clone();
        self.connection_args_for(server, &identity)
    }

    /// Connection arguments presenting an explicit identity, mapped or not.
    /// Used by G5 probes that must present an unmapped or foreign certificate.
    pub fn connection_args_for(
        &self,
        server: &OwnedServer,
        identity: &crate::durable::mtls::Identity,
    ) -> Result<Vec<String>> {
        Ok(vec![
            "--connect".into(),
            server.address.clone(),
            "--server-name".into(),
            "localhost".into(),
            "--ca".into(),
            path(&self.certs.ca_cert),
            "--cert".into(),
            path(&identity.cert),
            "--key".into(),
            path(&identity.key),
        ])
    }

    /// Journal args shared by `init-client` and every `client` op.
    pub fn journal_args(&self, journal: &Path, owner: &str, creation_sequence: u64) -> Vec<String> {
        vec![
            "--journal".into(),
            path(journal),
            "--authority".into(),
            self.authority.clone(),
            "--owner".into(),
            owner.to_owned(),
            "--creation-sequence".into(),
            creation_sequence.to_string(),
        ]
    }

    pub fn run_client_op(
        &self,
        journal: &Path,
        owner: &str,
        creation_sequence: u64,
        connection: &[String],
        operation: &[&str],
    ) -> Result<Output> {
        self.run_client_op_with(
            journal,
            owner,
            creation_sequence,
            &[],
            connection,
            operation,
        )
    }

    /// Build one `client` invocation: entry point, journal args, the
    /// kill-row control deadline (Java client only), per-invocation extras
    /// (the short-session-policy triple), connection args and the operation.
    /// Both `run_client_op_with` and `spawn_client_op` build through here so
    /// the flag placement can never drift between them.
    fn client_command(
        &self,
        journal: &Path,
        owner: &str,
        creation_sequence: u64,
        extra: &[String],
        connection: &[String],
        operation: &[&str],
    ) -> Result<Vec<String>> {
        let mut command = with_owned(&self.client_base()?, &["client".into()]);
        command.extend(self.journal_args(journal, owner, creation_sequence));
        if let Some(timeout_ms) = self.client_control_timeout_ms
            && self.client == Subject::Java
        {
            command.push("--control-timeout-ms".into());
            command.push(timeout_ms.to_string());
        }
        command.extend(extra.iter().cloned());
        command.extend(connection.iter().cloned());
        command.extend(operation.iter().map(|value| (*value).to_owned()));
        Ok(command)
    }

    /// One client op carrying additional per-invocation arguments between the
    /// journal args and the connection. Short-session-policy rows use this to
    /// redeclare the policy triple on every op: the Java client rebuilds its
    /// intent from the CLI flags and refuses CONFLICT when they differ from
    /// the initialized journal, and the Rust client flattens the same policy
    /// flags into every `client` invocation.
    pub fn run_client_op_with(
        &self,
        journal: &Path,
        owner: &str,
        creation_sequence: u64,
        extra: &[String],
        connection: &[String],
        operation: &[&str],
    ) -> Result<Output> {
        let command = self.client_command(
            journal,
            owner,
            creation_sequence,
            extra,
            connection,
            operation,
        )?;
        run_output_owned(&self.root, &command, self.client_op_timeout)
    }

    /// Spawn a one-shot client op without waiting for it. The returned child
    /// is fixture-owned: callers must reap it (bounded) on every path.
    pub fn spawn_client_op(
        &self,
        journal: &Path,
        owner: &str,
        creation_sequence: u64,
        connection: &[String],
        operation: &[&str],
    ) -> Result<Child> {
        let command = self.client_command(
            journal,
            owner,
            creation_sequence,
            &[],
            connection,
            operation,
        )?;
        ensure!(!command.is_empty(), "empty client command");
        Command::new(&command[0])
            .args(&command[1..])
            .current_dir(&self.root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("start {}", command.join(" ")))
    }

    /// Reap a spawned client op with bounded output draining.
    pub fn wait_client_op(child: Child, timeout: Duration) -> Result<Output> {
        wait_output(child, timeout)
    }

    /// Authenticated readiness probe: `next-sequence` over mTLS.
    pub fn next_sequence(&self, server: &OwnedServer, principal: &str) -> Result<u64> {
        let connection = self.connection_args(server, principal)?;
        let mut command = self.client_base()?;
        command.push("next-sequence".into());
        command.extend(connection);
        let output = run_output_owned(&self.root, &command, self.client_op_timeout)?;
        ensure_success(&output, "v2 next-sequence readiness probe")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let sequence = stdout
            .split_whitespace()
            .nth(1)
            .context("next-sequence did not print NEXT_SEQUENCE N")?
            .parse::<u64>()
            .context("next-sequence printed a non-decimal sequence")?;
        Ok(sequence)
    }

    /// Raw `next-sequence` probe presenting an explicit identity. Callers
    /// assert the expected failure themselves and record the transcript; G5
    /// rows never treat a probe failure as a fixture error.
    pub fn probe_next_sequence(&self, server: &OwnedServer, identity: &Identity) -> Result<Output> {
        let connection = self.connection_args_for(server, identity)?;
        let mut command = self.client_base()?;
        command.push("next-sequence".into());
        command.extend(connection);
        run_output_owned(&self.root, &command, self.client_op_timeout)
    }

    /// Raw `next-sequence` attempt WITHOUT a client certificate. Both subject
    /// CLIs require --cert/--key, so this documents the client-side refusal
    /// (g5-missing-client-cert); the wire-level arms of that row are not
    /// reachable through the published binaries.
    pub fn probe_next_sequence_without_cert(&self, server: &OwnedServer) -> Result<Output> {
        let mut command = self.client_base()?;
        command.push("next-sequence".into());
        command.extend([
            "--connect".into(),
            server.address.clone(),
            "--server-name".into(),
            "localhost".into(),
            "--ca".into(),
            path(&self.certs.ca_cert),
        ]);
        run_output_owned(&self.root, &command, self.client_op_timeout)
    }

    /// Spawn `v2 serve`, wait for the ready file, then require one successful
    /// authenticated op before declaring the server ready.
    pub fn start_server(&self) -> Result<OwnedServer> {
        self.start_server_armed(None, true)
    }

    /// Entry-point prefix for one server process. The Java fixture hooks live
    /// in the separate `FixtureMain` launcher (never `V2Main`); the Rust hooks
    /// are `--fixture-*` flags on the normal `v2 serve`.
    fn serve_base(&self, hooked: bool) -> Result<Vec<String>> {
        match (self.server, hooked) {
            (Subject::Java, true) => {
                let mut base = self.subject_base(Subject::Java)?;
                let main = base
                    .last_mut()
                    .expect("the Java base ends in the main class name");
                *main = "ai.pipestream.quic.v2.FixtureMain".into();
                base.push("serve".into());
                Ok(base)
            }
            _ => Ok(with_owned(&self.base()?, &["serve".into()])),
        }
    }

    /// The full `serve` command line for one server process, ready file and
    /// fixture arming included. Kept separate from
    /// [`AuthorityFixture::start_server_armed`] so the exact spawned
    /// invocation (including the storage-plane flag) is unit-testable
    /// without a subject binary.
    fn serve_command(&self, ready: &Path, arming: Option<&FixtureArming>) -> Result<Vec<String>> {
        let mut command = self.serve_base(arming.is_some())?;
        command.extend(self.storage_args());
        command.extend([
            "--bind".into(),
            "127.0.0.1:0".into(),
            "--cert".into(),
            path(&self.certs.server.cert),
            "--key".into(),
            path(&self.certs.server.key),
            "--client-ca".into(),
            path(&self.certs.ca_cert),
            "--result-authority".into(),
            "localhost:7443".into(),
            "--ready-file".into(),
            path(ready),
        ]);
        command.extend(self.extra_serve_args.iter().cloned());
        if let Some(arming) = arming {
            command.extend([
                "--fixture-events".into(),
                path(&arming.events),
                "--fixture-run".into(),
                arming.run_id.clone(),
                "--fixture-scenario".into(),
                arming.scenario_id.clone(),
            ]);
            if let Some(schedule) = &arming.schedule {
                command.extend(["--fixture-schedule".into(), path(schedule)]);
            }
        }
        Ok(command)
    }

    /// Spawn `v2 serve` with the milestone-5 `--fixture-*` arming. `probe`
    /// controls the authenticated readiness op: a schedule that kills the
    /// server at CONNECTION_AUTHENTICATED would kill the probe connection
    /// itself, so that row starts ready-file-only (process liveness plus
    /// marker, with the client op as the real readiness check). Both hooked
    /// subjects are supported: the Rust server takes the flags directly, the
    /// Java server runs Claude's FixtureMain with the same flags.
    pub fn start_server_armed(
        &self,
        arming: Option<&FixtureArming>,
        probe: bool,
    ) -> Result<OwnedServer> {
        let serial = unique_suffix();
        let ready = self.root.join(format!("ready-{serial:x}"));
        let log = self.root.join(format!("server-{serial:x}.log"));
        let command = self.serve_command(&ready, arming)?;
        let log_file = File::create(&log)?;
        let mut spawned = Command::new(&command[0]);
        spawned
            .args(&command[1..])
            .current_dir(&self.root)
            .stdout(Stdio::from(log_file.try_clone()?))
            .stderr(Stdio::from(log_file));
        let mut child = spawned
            .spawn()
            .with_context(|| format!("start {}", command.join(" ")))?;
        let deadline = Instant::now() + READY_TIMEOUT;
        let address = loop {
            if let Ok(text) = fs::read_to_string(&ready)
                && let Some(address) = parse_ready(&text)
            {
                break address;
            }
            if let Some(status) = child.try_wait()? {
                bail!(
                    "v2 serve exited before readiness ({status})\nlog:\n{}",
                    fs::read_to_string(&log).unwrap_or_default()
                );
            }
            ensure!(Instant::now() < deadline, "v2 serve readiness timed out");
            thread::sleep(Duration::from_millis(25));
        };
        let server = OwnedServer {
            name: "v2-serve",
            child: Some(child),
            address,
            log,
        };
        // A live process plus a marker file is not readiness: an authenticated
        // op must succeed before the fixture counts the server as ready.
        if probe {
            self.next_sequence(&server, "alice")?;
        }
        Ok(server)
    }

    /// Restart the same target against the same state DB, object dir and
    /// principal map with a fresh ready file and process start. Valid only
    /// after the fixture itself stopped or killed the previous process.
    #[allow(dead_code)] // exercised by the milestone-2 crash/restart rows
    pub fn restart_server(&self, previous: OwnedServer) -> Result<OwnedServer> {
        previous.kill()?;
        self.start_server()
    }
}

fn parse_ready(text: &str) -> Option<String> {
    let trimmed = text.trim();
    trimmed
        .parse::<std::net::SocketAddr>()
        .ok()
        .map(|_| trimmed.to_owned())
}

fn with_owned(base: &[String], arguments: &[String]) -> Vec<String> {
    let mut result = base.to_vec();
    result.extend(arguments.iter().cloned());
    result
}

pub struct OwnedServer {
    pub name: &'static str,
    child: Option<Child>,
    pub address: String,
    pub log: PathBuf,
}

impl OwnedServer {
    /// Fixture-owned pid for resource-collector anchoring.
    pub fn pid(&self) -> Result<u32> {
        let child = self
            .child
            .as_ref()
            .context("server process already consumed")?;
        Ok(child.id())
    }

    /// Graceful stop: SIGTERM, then require a drained, zero exit.
    pub fn stop(mut self) -> Result<Output> {
        let mut child = self
            .child
            .take()
            .context("server process already consumed")?;
        ensure!(
            child.try_wait()?.is_none(),
            "{} was already reaped before stop",
            self.name
        );
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .context("send SIGTERM to fixture-owned server")?;
        ensure!(status.success(), "failed to signal {}", self.name);
        let output = wait_output(child, SHUTDOWN_TIMEOUT)?;
        let log = fs::read_to_string(&self.log).unwrap_or_default();
        ensure!(
            output.status.success() && log.contains("DRAINED"),
            "{} did not drain cleanly ({})\nlog:\n{log}",
            self.name,
            output.status
        );
        Ok(output)
    }

    /// Immediate hard kill (SIGKILL). Process death only; not power-loss
    /// coverage. The child is reaped before returning.
    pub fn kill(mut self) -> Result<()> {
        let mut child = self
            .child
            .take()
            .context("server process already consumed")?;
        child.kill().context("SIGKILL fixture-owned server")?;
        child.wait().context("reap killed server")?;
        Ok(())
    }

    /// Wait for a subject self-kill (a scheduled `kill` row exits 86 on its
    /// own at the armed boundary). Returns the process output; the exit code
    /// is the row's evidence.
    pub fn wait_exit(mut self, timeout: Duration) -> Result<Output> {
        let child = self
            .child
            .take()
            .context("server process already consumed")?;
        wait_output(child, timeout)
    }
}

impl Drop for OwnedServer {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_java_subject_base_carries_the_frozen_memory_flags_before_the_classpath() {
        let directory = tempfile::tempdir().unwrap();
        let jar = directory.path().join("subject-all.jar");
        fs::write(&jar, b"not really a jar").unwrap();
        let certs =
            crate::durable::mtls::generate(&directory.path().join("certs"), &[("alice", "alice")])
                .unwrap();
        let fixture = AuthorityFixture::new(
            Path::new("/nonexistent/pipestream-quinn"),
            Some(&jar),
            directory.path(),
            certs,
            Subject::Java,
            Subject::Java,
            "java-tmp-test".to_owned(),
        )
        .unwrap();
        let base = fixture.base().unwrap();
        assert_eq!(base[0], "java");
        assert_eq!(&base[1..3], JAVA_MEMORY_FLAGS);
        // HotSpot ignores the TMPDIR environment variable: every spawned
        // JVM is pointed at the run-keyed tmp directory explicitly (the
        // driver's TMPDIR governs the root), before the classpath, and the
        // directory must exist - base() creates it.
        let tmp = java_tmp_dir("java-tmp-test");
        assert_eq!(
            base[3],
            format!("-Djava.io.tmpdir={}", path(&tmp)),
            "{base:?}"
        );
        assert_eq!(
            base[4],
            format!("-Dorg.sqlite.tmpdir={}", path(&tmp)),
            "{base:?}"
        );
        assert!(tmp.is_dir(), "base() must create the java tmp directory");
        // The limits and flags are argument-order sensitive: they must
        // precede the classpath and main class, which stay last so
        // serve_base can swap V2Main for FixtureMain.
        assert_eq!(base[5], "--enable-native-access=ALL-UNNAMED");
        assert_eq!(base[6], "-cp");
        assert_eq!(base.last().unwrap(), "ai.pipestream.quic.v2.V2Main");
        assert_eq!(java_memory_flags_text(), "-Xms256m -Xmx2g");
        // The rust subject is never given JVM flags.
        let rust = fixture.subject_base(Subject::Rust).unwrap();
        assert!(!rust.iter().any(|argument| argument.starts_with("-Xm")));
        assert!(
            !rust
                .iter()
                .any(|argument| argument.starts_with("-Djava.io.tmpdir")),
            "the rust subject is never given JVM flags: {rust:?}"
        );
        fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn ready_file_text_must_parse_as_a_socket_address() {
        assert_eq!(
            parse_ready("127.0.0.1:7443\n"),
            Some("127.0.0.1:7443".to_owned())
        );
        assert_eq!(parse_ready(""), None);
        assert_eq!(parse_ready("not-an-address"), None);
        assert_eq!(parse_ready("127.0.0.1:99999"), None);
    }

    /// Kill rows arm a server self-kill; the Java launcher must notice the
    /// dead authority well inside the driver's 30 s op bound (observation
    /// O-2), so the fixture passes `--control-timeout-ms` on those rows. The
    /// Rust client CLI exposes no such option (quinn/src/v2_client
    /// `response_timeout` has no CLI surface), so the flag must reach the
    /// Java client only — clap would reject an unknown flag outright.
    #[test]
    fn kill_row_control_timeout_flag_reaches_the_java_client_only() {
        let directory = tempfile::tempdir().unwrap();
        let jar = directory.path().join("subject-all.jar");
        fs::write(&jar, b"not really a jar").unwrap();
        let certs =
            crate::durable::mtls::generate(&directory.path().join("certs"), &[("alice", "alice")])
                .unwrap();
        let journal = directory.path().join("session.sqlite");
        let connection = vec!["--connect".to_owned(), "127.0.0.1:7443".to_owned()];
        let java = AuthorityFixture::new(
            Path::new("/nonexistent/pipestream-quinn"),
            Some(&jar),
            directory.path(),
            certs.clone(),
            Subject::Java,
            Subject::Java,
            "java-tmp-test".to_owned(),
        )
        .unwrap()
        .with_client_control_timeout_ms(Some(JAVA_CLIENT_KILL_CONTROL_TIMEOUT_MS));
        let command = java
            .client_command(
                &journal,
                "alice",
                1,
                &[],
                &connection,
                &["watch", "--work", "0:0:1"],
            )
            .unwrap();
        let position = command
            .windows(2)
            .position(|pair| pair == ["--control-timeout-ms", "10000"])
            .expect("the Java kill-row client must carry --control-timeout-ms 10000");
        // The flag sits with the other named options, before the connection
        // block and the operation subcommand.
        let connect = command.iter().position(|arg| arg == "--connect").unwrap();
        assert!(
            position + 1 < connect,
            "flag must precede the connection block: {command:?}"
        );
        assert_eq!(
            java.client_control_timeout_ms,
            Some(JAVA_CLIENT_KILL_CONTROL_TIMEOUT_MS)
        );

        // Without the kill-row setting no flag is emitted at all.
        let plain_java = AuthorityFixture::new(
            Path::new("/nonexistent/pipestream-quinn"),
            Some(&jar),
            directory.path(),
            certs.clone(),
            Subject::Java,
            Subject::Java,
            "java-tmp-test".to_owned(),
        )
        .unwrap();
        let plain = plain_java
            .client_command(&journal, "alice", 1, &[], &connection, &["binding"])
            .unwrap();
        assert!(
            !plain.iter().any(|arg| arg == "--control-timeout-ms"),
            "a fixture without the kill-row setting must not pass the flag: {plain:?}"
        );

        // The Rust client CLI has no such option: even with the setting the
        // fixture must not emit it (clap would reject the unknown flag).
        let rust = AuthorityFixture::new(
            Path::new("/nonexistent/pipestream-quinn"),
            Some(&jar),
            directory.path(),
            certs,
            Subject::Rust,
            Subject::Rust,
            "java-tmp-test".to_owned(),
        )
        .unwrap()
        .with_client_control_timeout_ms(Some(JAVA_CLIENT_KILL_CONTROL_TIMEOUT_MS));
        let rust_command = rust
            .client_command(&journal, "alice", 1, &[], &connection, &["binding"])
            .unwrap();
        assert!(
            !rust_command.iter().any(|arg| arg == "--control-timeout-ms"),
            "the Rust client CLI exposes no control-timeout option: {rust_command:?}"
        );
    }

    /// A killed authority is noticed only at the negotiated transport bound
    /// (~60 s: the rust server's max_idle_timeout, quinn/src/v2_authority/
    /// server.rs:225, or the rust client's response_timeout, quinn/
    /// src/v2_client/transport.rs:127), so kill rows must be able to widen
    /// the per-op wait past the default 30 s. The default stays 30 s for
    /// every other row, and the widened budget travels with the fixture
    /// clone that a hooked row's restart builds its recovered session from.
    #[test]
    fn kill_rows_widen_the_client_op_wait_and_nothing_else_does() {
        let directory = tempfile::tempdir().unwrap();
        let jar = directory.path().join("subject-all.jar");
        fs::write(&jar, b"not really a jar").unwrap();
        let certs =
            crate::durable::mtls::generate(&directory.path().join("certs"), &[("alice", "alice")])
                .unwrap();
        let fixture = AuthorityFixture::new(
            Path::new("/nonexistent/pipestream-quinn"),
            Some(&jar),
            directory.path(),
            certs,
            Subject::Java,
            Subject::Java,
            "java-tmp-test".to_owned(),
        )
        .unwrap();
        assert_eq!(fixture.client_op_timeout, OP_TIMEOUT);
        let kill_row = fixture.clone().with_client_op_timeout(KILL_ROW_OP_TIMEOUT);
        assert_eq!(kill_row.client_op_timeout, Duration::from_secs(90));
        let restarted = kill_row.clone();
        assert_eq!(restarted.client_op_timeout, Duration::from_secs(90));
        assert_eq!(fixture.client_op_timeout, Duration::from_secs(30));
    }

    /// The storage selection is driver-level: an SPI run must pass the
    /// subject's own plane/backend/coordinate flags at BOTH storage entry
    /// points (init-authority and every serve start, hooked or not), and the
    /// default v2/fs selection must leave every spawned command line
    /// byte-identical — the merged subjects do not carry the flags, and
    /// clap would refuse an unknown option.
    #[test]
    #[allow(clippy::type_complexity)] // the case table is a test literal
    fn storage_selection_flags_are_emitted_only_for_spi_and_at_every_entry_point() {
        fn fixture_for(
            directory: &tempfile::TempDir,
            server: Subject,
            storage: StorageSelection,
        ) -> AuthorityFixture {
            let jar = directory.path().join("subject-all.jar");
            fs::write(&jar, b"not really a jar").unwrap();
            let certs = crate::durable::mtls::generate(
                &directory.path().join("certs"),
                &[("alice", "alice")],
            )
            .unwrap();
            AuthorityFixture::new(
                Path::new("/nonexistent/pipestream-quinn"),
                Some(&jar),
                directory.path(),
                certs,
                server,
                server,
                "java-tmp-test".to_owned(),
            )
            .unwrap()
            .with_storage(storage)
        }

        fn assert_gains_exactly(default: &[String], selected: &[String], tokens: &[&str]) {
            assert_eq!(
                selected.len(),
                default.len() + tokens.len(),
                "a selected-storage command gains exactly the expected tokens: {selected:?}"
            );
            let position = selected
                .windows(tokens.len())
                .position(|window| window == tokens)
                .unwrap_or_else(|| panic!("the command must carry {tokens:?}: {selected:?}"));
            let mut expected = default.to_vec();
            expected.splice(
                position..position,
                tokens.iter().map(|arg| (*arg).to_owned()),
            );
            assert_eq!(
                *selected, expected,
                "the storage tokens must sit at the storage boundary"
            );
        }

        fn selection(backend: StorageBackend, coordinates: &[(&str, &str)]) -> StorageSelection {
            StorageSelection {
                plane: StoragePlane::Spi,
                backend,
                coordinates: coordinates
                    .iter()
                    .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                    .collect(),
            }
        }

        // (backend, coordinates, Java token run, Rust token run)
        let cases: [(StorageBackend, &[(&str, &str)], &[&str], &[&str]); 4] = [
            (
                StorageBackend::Fs,
                &[],
                &["--object-plane", "spi", "--spi-backend", "fs"],
                &["--payload-backend", "spi-fs"],
            ),
            (
                StorageBackend::Postgres,
                &[("PROBE_PG_URL", "postgres://localhost:5432/probe")],
                &[
                    "--object-plane",
                    "spi",
                    "--spi-backend",
                    "postgres",
                    "--spi-pg-url",
                    "postgres://localhost:5432/probe",
                ],
                &[
                    "--payload-backend",
                    "spi-postgres",
                    "--payload-pg-url",
                    "postgres://localhost:5432/probe",
                ],
            ),
            (
                StorageBackend::S3,
                &[
                    ("PROBE_S3_ENDPOINT", "http://localhost:9000"),
                    ("PROBE_S3_BUCKET", "probe-bucket"),
                    ("PROBE_S3_REGION", "probe-region"),
                    ("PROBE_S3_PREFIX", "probe/prefix/"),
                ],
                &[
                    "--object-plane",
                    "spi",
                    "--spi-backend",
                    "s3",
                    "--spi-s3-endpoint",
                    "http://localhost:9000",
                    "--spi-s3-bucket",
                    "probe-bucket",
                    "--spi-s3-region",
                    "probe-region",
                    "--spi-s3-prefix",
                    "probe/prefix/",
                ],
                &[
                    "--payload-backend",
                    "spi-s3",
                    "--payload-s3-endpoint",
                    "http://localhost:9000",
                    "--payload-s3-bucket",
                    "probe-bucket",
                    "--payload-s3-region",
                    "probe-region",
                    "--payload-s3-prefix",
                    "probe/prefix/",
                ],
            ),
            (
                StorageBackend::Mongo,
                &[
                    ("PROBE_MONGO_URI", "mongodb://localhost:27017"),
                    ("PROBE_MONGO_DATABASE", "probe"),
                ],
                &[
                    "--object-plane",
                    "spi",
                    "--spi-backend",
                    "mongo",
                    "--spi-mongo-uri",
                    "mongodb://localhost:27017",
                    "--spi-mongo-database",
                    "probe",
                ],
                &[
                    "--payload-backend",
                    "spi-mongo",
                    "--payload-mongo-uri",
                    "mongodb://localhost:27017",
                    "--payload-mongo-database",
                    "probe",
                ],
            ),
        ];

        let directory = tempfile::tempdir().unwrap();
        let ready = directory.path().join("ready");
        let arming = FixtureArming {
            events: directory.path().join("events.tsv"),
            run_id: "run-a".to_owned(),
            scenario_id: "g2-kill".to_owned(),
            schedule: Some(directory.path().join("schedule.tsv")),
        };
        for server in [Subject::Java, Subject::Rust] {
            let default = fixture_for(&directory, server, StorageSelection::default());

            // The default selection emits no storage flag at any entry point.
            for command in [
                default.init_authority_command().unwrap(),
                default.serve_command(&ready, None).unwrap(),
                default.serve_command(&ready, Some(&arming)).unwrap(),
            ] {
                for token in ["--object-plane", "--spi-backend", "--payload-backend"] {
                    assert!(
                        !command.iter().any(|arg| arg == token),
                        "a default run must stay byte-identical: {command:?}"
                    );
                }
            }

            for (backend, coordinates, java_tokens, rust_tokens) in cases {
                let tokens = match server {
                    Subject::Java => java_tokens,
                    Subject::Rust => rust_tokens,
                };
                let selected = fixture_for(&directory, server, selection(backend, coordinates));
                // The subject's own token run is appended at init-authority
                // and at every serve start, exactly, in convention order.
                assert_gains_exactly(
                    &default.init_authority_command().unwrap(),
                    &selected.init_authority_command().unwrap(),
                    tokens,
                );
                assert_gains_exactly(
                    &default.serve_command(&ready, None).unwrap(),
                    &selected.serve_command(&ready, None).unwrap(),
                    tokens,
                );
                let hooked = selected.serve_command(&ready, Some(&arming)).unwrap();
                assert!(
                    hooked.windows(tokens.len()).any(|window| window == tokens),
                    "the hooked (FixtureMain) serve must carry {tokens:?}: {hooked:?}"
                );
                // The selection travels with the fixture clone a kill row's
                // restart builds its recovered session from.
                assert_eq!(selected.clone().storage, selected.storage);
            }
        }
        assert_eq!(StoragePlane::V2.name(), "v2");
        assert_eq!(StoragePlane::Spi.name(), "spi");
        assert_eq!(StorageBackend::Fs.name(), "fs");
        assert_eq!(StorageBackend::Postgres.name(), "postgres");
        assert_eq!(StorageBackend::S3.name(), "s3");
        assert_eq!(StorageBackend::Mongo.name(), "mongo");
        fs::remove_dir_all(java_tmp_dir("java-tmp-test")).unwrap();
    }

    /// Coordinates resolve from the PROBE_* environment conventions; every
    /// required coordinate of the selected backend must be present (a
    /// clear usage error naming the missing variables), secrets are never
    /// forwarded onto a command line, and optional coordinates (S3 region,
    /// prefix) are forwarded only when set.
    #[test]
    fn backend_coordinates_resolve_from_probe_env_with_usage_errors() {
        fn resolve_with(
            values: &[(&str, &str)],
            backend: StorageBackend,
        ) -> Result<Vec<(String, String)>> {
            let owned: Vec<(String, String)> = values
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect();
            resolve_backend_coordinates(backend, |name| {
                owned
                    .iter()
                    .find(|(key, _)| key == name)
                    .map(|(_, value)| value.clone())
            })
        }

        // fs needs nothing and forwards nothing.
        assert_eq!(resolve_with(&[], StorageBackend::Fs).unwrap(), Vec::new());

        // postgres: the URL is required; user/password are env-only secrets
        // (never forwarded as flags, never required by the driver).
        let error = resolve_with(&[], StorageBackend::Postgres)
            .unwrap_err()
            .to_string();
        assert!(error.contains("postgres"), "{error}");
        assert!(error.contains("PROBE_PG_URL"), "{error}");
        let resolved = resolve_with(
            &[
                ("PROBE_PG_URL", "postgres://localhost:5432/probe"),
                ("PROBE_PG_USER", "probe-user"),
                ("PROBE_PG_PASSWORD", "probe-password"),
            ],
            StorageBackend::Postgres,
        )
        .unwrap();
        assert_eq!(
            resolved,
            vec![(
                "PROBE_PG_URL".to_owned(),
                "postgres://localhost:5432/probe".to_owned()
            )],
            "secrets must not be forwarded: {resolved:?}"
        );

        // s3: endpoint/bucket/access/secret are required; region and prefix
        // are optional and forwarded only when present.
        let error = resolve_with(&[("PROBE_S3_BUCKET", "probe-bucket")], StorageBackend::S3)
            .unwrap_err()
            .to_string();
        assert!(error.contains("PROBE_S3_ENDPOINT"), "{error}");
        assert!(error.contains("PROBE_S3_ACCESS_KEY"), "{error}");
        assert!(error.contains("PROBE_S3_SECRET_KEY"), "{error}");
        let resolved = resolve_with(
            &[
                ("PROBE_S3_ENDPOINT", "http://localhost:9000"),
                ("PROBE_S3_BUCKET", "probe-bucket"),
                ("PROBE_S3_ACCESS_KEY", "probe-access"),
                ("PROBE_S3_SECRET_KEY", "probe-secret"),
            ],
            StorageBackend::S3,
        )
        .unwrap();
        assert_eq!(
            resolved,
            vec![
                (
                    "PROBE_S3_ENDPOINT".to_owned(),
                    "http://localhost:9000".to_owned()
                ),
                ("PROBE_S3_BUCKET".to_owned(), "probe-bucket".to_owned()),
            ],
            "unset optional coordinates are omitted, secrets never forwarded: {resolved:?}"
        );
        let resolved = resolve_with(
            &[
                ("PROBE_S3_ENDPOINT", "http://localhost:9000"),
                ("PROBE_S3_BUCKET", "probe-bucket"),
                ("PROBE_S3_ACCESS_KEY", "probe-access"),
                ("PROBE_S3_SECRET_KEY", "probe-secret"),
                ("PROBE_S3_REGION", "probe-region"),
                ("PROBE_S3_PREFIX", "probe/prefix/"),
            ],
            StorageBackend::S3,
        )
        .unwrap();
        assert_eq!(
            resolved
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "PROBE_S3_ENDPOINT",
                "PROBE_S3_BUCKET",
                "PROBE_S3_REGION",
                "PROBE_S3_PREFIX"
            ]
        );

        // mongo: URI and database are both required.
        let error = resolve_with(
            &[("PROBE_MONGO_URI", "mongodb://localhost:27017")],
            StorageBackend::Mongo,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("PROBE_MONGO_DATABASE"), "{error}");
        let resolved = resolve_with(
            &[
                ("PROBE_MONGO_URI", "mongodb://localhost:27017"),
                ("PROBE_MONGO_DATABASE", "probe"),
            ],
            StorageBackend::Mongo,
        )
        .unwrap();
        assert_eq!(
            resolved,
            vec![
                (
                    "PROBE_MONGO_URI".to_owned(),
                    "mongodb://localhost:27017".to_owned()
                ),
                ("PROBE_MONGO_DATABASE".to_owned(), "probe".to_owned()),
            ]
        );
    }

    /// Plane selection: fs defaults to v2, a non-fs backend implies SPI,
    /// an explicit SPI selection wins, and an explicit v2 with a non-fs
    /// backend is a usage error (both subjects refuse that combination).
    #[test]
    fn effective_plane_selection_and_conflicts() {
        assert_eq!(
            effective_plane(None, StorageBackend::Fs).unwrap(),
            StoragePlane::V2
        );
        assert_eq!(
            effective_plane(None, StorageBackend::Postgres).unwrap(),
            StoragePlane::Spi
        );
        assert_eq!(
            effective_plane(None, StorageBackend::S3).unwrap(),
            StoragePlane::Spi
        );
        assert_eq!(
            effective_plane(None, StorageBackend::Mongo).unwrap(),
            StoragePlane::Spi
        );
        assert_eq!(
            effective_plane(Some(StoragePlane::Spi), StorageBackend::Fs).unwrap(),
            StoragePlane::Spi
        );
        assert_eq!(
            effective_plane(Some(StoragePlane::Spi), StorageBackend::S3).unwrap(),
            StoragePlane::Spi
        );
        let error = effective_plane(Some(StoragePlane::V2), StorageBackend::Mongo)
            .unwrap_err()
            .to_string();
        assert!(error.contains("--storage-plane v2"), "{error}");
        assert!(error.contains("mongo"), "{error}");
    }
}
