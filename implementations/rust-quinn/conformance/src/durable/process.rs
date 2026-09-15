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
}

impl AuthorityFixture {
    pub fn new(
        rust_bin: &Path,
        java_jar: Option<&Path>,
        root: &Path,
        certs: Material,
        server: Subject,
        client: Subject,
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
        })
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
        self.subject_base(self.server)
    }

    /// Entry-point prefix for the fixture's client subject.
    pub fn client_base(&self) -> Result<Vec<String>> {
        self.subject_base(self.client)
    }

    /// Authority storage arguments. Rust takes the paired DB/object
    /// directories explicitly; Java takes the root that contains both
    /// (api-plan.md section 1.5: fixed layout root/authority.sqlite +
    /// root/objects, byte-for-byte paired like Rust).
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
        args
    }

    pub fn run_init_authority(&self) -> Result<()> {
        let mut command = with_owned(&self.base()?, &["init-authority".into()]);
        command.extend(self.storage_args());
        if self.server == Subject::Java {
            // Java's init-authority validates the result-locator authority up
            // front (V2Main.configuration requires --result-authority); the
            // Rust clap definition has no such flag on init-authority.
            command.extend(["--result-authority".into(), "localhost:7443".into()]);
        }
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
            path(&ready),
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
        )
        .unwrap();
        let base = fixture.base().unwrap();
        assert_eq!(base[0], "java");
        assert_eq!(&base[1..3], JAVA_MEMORY_FLAGS);
        // The limits are argument-order sensitive: they must precede the
        // classpath and main class, which stay last so serve_base can swap
        // V2Main for FixtureMain.
        assert_eq!(base[3], "--enable-native-access=ALL-UNNAMED");
        assert_eq!(base[4], "-cp");
        assert_eq!(base.last().unwrap(), "ai.pipestream.quic.v2.V2Main");
        assert_eq!(java_memory_flags_text(), "-Xms256m -Xmx2g");
        // The rust subject is never given JVM flags.
        let rust = fixture.subject_base(Subject::Rust).unwrap();
        assert!(!rust.iter().any(|argument| argument.starts_with("-Xm")));
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
        )
        .unwrap();
        assert_eq!(fixture.client_op_timeout, OP_TIMEOUT);
        let kill_row = fixture.clone().with_client_op_timeout(KILL_ROW_OP_TIMEOUT);
        assert_eq!(kill_row.client_op_timeout, Duration::from_secs(90));
        let restarted = kill_row.clone();
        assert_eq!(restarted.client_op_timeout, Duration::from_secs(90));
        assert_eq!(fixture.client_op_timeout, Duration::from_secs(30));
    }
}
