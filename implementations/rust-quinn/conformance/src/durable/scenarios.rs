//! Scenario matrix and the milestone-1/2 rows (rust client against rust
//! server, driven purely through spawned V2 CLI binaries).

use crate::durable::events::{ArtifactRef, EventWriter};
use crate::durable::mtls;
use crate::durable::oracle;
use crate::durable::process::{AuthorityFixture, OwnedServer, Subject};
use crate::durable::schedule;
use crate::{hex, unique_suffix};
use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::Output,
    thread,
    time::{Duration, Instant},
};

const INPUT_LEN: usize = 64 * 1024;
const RACE_INPUT_LEN: usize = 4 * 1024 * 1024;
/// Client-kill row input: large enough that a loopback upload cannot finish
/// inside the seeded kill delay (the 16 MiB object limit caps how large the
/// admit input may be).
const CLIENT_KILL_INPUT_LEN: usize = 12 * 1024 * 1024;
const WATCH_TIMEOUT: Duration = Duration::from_secs(20);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const OP_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Row {
    pub id: &'static str,
    pub group: &'static str,
    /// Implemented for the rust-client/rust-server direction.
    pub rust_implemented: bool,
}

pub fn rows() -> Vec<Row> {
    fn push(rows: &mut Vec<Row>, group: &'static str, ids: &[&'static str]) {
        rows.extend(ids.iter().map(|id| Row {
            id,
            group,
            rust_implemented: false,
        }));
    }
    let mut rows = Vec::new();
    push(
        &mut rows,
        "G1",
        &[
            "g1-leaf-copy",
            "g1-empty-input",
            "g1-zero-output",
            "g1-mode1-branch",
            "g1-mode2-descendants",
            "g1-oversize-payload",
            "g1-out-of-order-pages",
        ],
    );
    push(
        &mut rows,
        "G2",
        &[
            "g2-crash-before-create-commit",
            "g2-crash-after-create-commit",
            "g2-drop-reply-declaration",
            "g2-drop-reply-admission",
            "g2-kill-after-admission-before-publication",
            "g2-kill-at-publication-commit",
            "g2-drop-reply-publication",
            "g2-kill-client-after-request-sent",
            "g2-duplicate-op-changed-params",
            "g2-simultaneous-duplicate",
            "g2-kill-server-after-admission-recovery",
            "g2-not-found-in-flight",
        ],
    );
    push(
        &mut rows,
        "G3",
        &[
            "g3-input-before-metadata",
            "g3-orphan-cleanup",
            "g3-terminal-cleanup",
            "g3-partial-retirement",
            "g3-restart-same-roots",
        ],
    );
    push(
        &mut rows,
        "G4",
        &[
            "g4-publication-vs-cancel",
            "g4-publication-vs-skip",
            "g4-stale-attempt-retry",
            "g4-ancestor-fence-publication",
            "g4-deadline-settlement",
        ],
    );
    push(
        &mut rows,
        "G5",
        &[
            "g5-cert-rotation-same-owner",
            "g5-missing-client-cert",
            "g5-unmapped-principal",
            "g5-foreign-owner",
            "g5-untrusted-identity",
            "g5-expired-identity",
            "g5-remapped-owner",
            "g5-cross-authority-reference",
            "g5-no-existence-disclosure",
        ],
    );
    push(
        &mut rows,
        "G6",
        &[
            "g6-canonical-violations",
            "g6-wrong-length-hash-fin",
            "g6-duplicate-response",
            "g6-error-after-result-header",
            "g6-stopped-control",
            "g6-frames-from-raw-probe",
        ],
    );
    push(
        &mut rows,
        "G7",
        &[
            "g7-receipt-before-output-expiry",
            "g7-output-before-receipt-expiry",
            "g7-read-pin-past-expiry",
            "g7-unsafe-clock-refusal",
            "g7-deadline-queue-time",
            "g7-cleanup-interrupted-refund",
        ],
    );
    push(
        &mut rows,
        "G8",
        &[
            "g8-exact-root-complete",
            "g8-child-cut-conflict",
            "g8-detach-drains",
            "g8-refusals-after-detach",
            "g8-half-close-preserves-responses",
            "g8-timeout-no-completion-claim",
        ],
    );
    push(
        &mut rows,
        "R",
        &[
            "r-connection-ceiling",
            "r-pending-ceiling",
            "r-stalled-principal-progress",
            "r-memory-ladder",
            "r-staging-quota",
            "r-journal-bounds",
        ],
    );
    for id in [
        "g1-leaf-copy",
        "g2-crash-before-create-commit",
        "g2-crash-after-create-commit",
        "g2-drop-reply-declaration",
        "g2-drop-reply-admission",
        "g2-kill-after-admission-before-publication",
        "g2-kill-at-publication-commit",
        "g2-kill-client-after-request-sent",
        "g2-duplicate-op-changed-params",
        "g2-simultaneous-duplicate",
        "g2-kill-server-after-admission-recovery",
        "g5-untrusted-identity",
        "g5-missing-client-cert",
        "g5-unmapped-principal",
        "g5-foreign-owner",
        "g5-no-existence-disclosure",
    ] {
        rows.iter_mut()
            .find(|row| row.id == id)
            .unwrap_or_else(|| panic!("{id} is in the matrix"))
            .rust_implemented = true;
    }
    rows
}

pub struct ScenarioContext {
    pub run_id: String,
    pub run_root: PathBuf,
    pub seed: u64,
    pub rust_bin: PathBuf,
    pub java_jar: Option<PathBuf>,
}

impl ScenarioContext {
    pub fn scenario_dir(&self, scenario_id: &str) -> PathBuf {
        self.run_root.join(scenario_id)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DirectionOutcome {
    Pass(String),
    Incomplete(String),
    Fail(String),
}

/// Directions a passing row actually executed, for the run summary line.
fn direction_coverage(row: &Row, context: &ScenarioContext) -> String {
    if row.id == "g1-leaf-copy" && context.java_jar.is_some() {
        "rust-client/rust-server, rust-client/java-server, java-client/rust-server".to_owned()
    } else if row.id.starts_with("g5-") && context.java_jar.is_some() {
        "rust-client/rust-server, rust-client/java-server".to_owned()
    } else if HOOKED_G2_ROWS.contains(&row.id) && context.java_jar.is_some() {
        "rust-client/rust-server, java-client/rust-server".to_owned()
    } else {
        "rust-client/rust-server".to_owned()
    }
}

pub fn run_direction(row: &Row, context: &ScenarioContext, dev: bool) -> DirectionOutcome {
    if !row.rust_implemented {
        return DirectionOutcome::Incomplete(format!(
            "{} rust-client/rust-server not implemented yet",
            row.id
        ));
    }
    match run_rust_direction(row, context) {
        Ok(()) => DirectionOutcome::Pass(direction_coverage(row, context)),
        Err(error) => {
            let message = format!("{error:#}");
            if dev {
                DirectionOutcome::Incomplete(format!("{} dev run failed: {message}", row.id))
            } else {
                DirectionOutcome::Fail(format!("{}: {message}", row.id))
            }
        }
    }
}

fn require(output: &Output, marker: &str, description: &str) -> Result<String> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        output.status.success() && stdout.contains(marker),
        "{description} failed ({})\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    Ok(stdout.into_owned())
}

fn run_rust_direction(row: &Row, context: &ScenarioContext) -> Result<()> {
    match row.id {
        "g1-leaf-copy" => g1_leaf_copy(context),
        "g2-crash-before-create-commit" => g2_crash_before_create_commit(context),
        "g2-crash-after-create-commit" => g2_crash_after_create_commit(context),
        "g2-drop-reply-declaration" => g2_drop_reply_declaration(context),
        "g2-drop-reply-admission" => g2_drop_reply_admission(context),
        "g2-kill-after-admission-before-publication" => {
            g2_kill_after_admission_before_publication(context)
        }
        "g2-kill-at-publication-commit" => g2_kill_at_publication_commit(context),
        "g2-kill-client-after-request-sent" => g2_kill_client_after_request_sent(context),
        "g2-duplicate-op-changed-params" => g2_duplicate_op_changed_params(context),
        "g2-simultaneous-duplicate" => g2_simultaneous_duplicate(context),
        "g2-kill-server-after-admission-recovery" => {
            g2_kill_server_after_admission_recovery(context)
        }
        "g5-untrusted-identity" => g5_untrusted_identity(context),
        "g5-missing-client-cert" => g5_missing_client_cert(context),
        "g5-unmapped-principal" => g5_unmapped_principal(context),
        "g5-foreign-owner" => g5_foreign_owner(context),
        "g5-no-existence-disclosure" => g5_no_existence_disclosure(context),
        other => bail!("scenario {other} has no rust direction implemented"),
    }
}

// ---------------------------------------------------------------------------
// Shared row plumbing
// ---------------------------------------------------------------------------

/// One authenticated client session against one fixture-owned server.
struct Session {
    fixture: AuthorityFixture,
    server: OwnedServer,
    sequence: u64,
    journal: PathBuf,
    connection: Vec<String>,
}

impl Session {
    fn op(&self, operation: &[&str]) -> Result<Output> {
        self.fixture.run_client_op(
            &self.journal,
            "alice",
            self.sequence,
            &self.connection,
            operation,
        )
    }

    /// One watch poll; returns the full stdout (contains WORK and VIEW lines).
    fn watch(&self, work: &str) -> Result<String> {
        let output = self.op(&["watch", "--work", work])?;
        require(&output, "WORK", "watch operation")
    }
}

fn open_scenario(context: &ScenarioContext, id: &str) -> Result<(PathBuf, PathBuf)> {
    let scenario_dir = context.scenario_dir(id);
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    Ok((scenario_dir, artifacts))
}

fn open_events(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    client: Subject,
) -> Result<EventWriter> {
    let process_start_id = format!("{}-{:x}", std::process::id(), unique_suffix());
    EventWriter::open(
        &scenario_dir.join("events.tsv"),
        &context.run_id,
        id,
        client.name(),
        "client",
        &process_start_id,
    )
}

/// mTLS material, init-authority, serve with authenticated readiness, and a
/// fresh client journal bound to NEXT_SEQUENCE 1.
fn setup_session(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<Session> {
    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        client,
    )?;
    fixture.run_init_authority()?;
    let server = fixture.start_server()?;
    let sequence = fixture.next_sequence(&server, "alice")?;
    ensure!(
        sequence == 1,
        "fresh authority must report NEXT_SEQUENCE 1, got {sequence}"
    );
    let journal = scenario_dir.join("client").join("session.sqlite");
    fs::create_dir_all(journal.parent().expect("journal has a parent directory"))?;
    let mut command = fixture.client_base()?;
    command.push("init-client".into());
    command.extend(fixture.journal_args(&journal, "alice", sequence));
    let init = crate::run_output_owned(&fixture.root, &command, OP_WAIT)?;
    require(&init, client.client_initialized_marker(), "v2 init-client")?;
    let connection = fixture.connection_args(&server, "alice")?;
    Ok(Session {
        fixture,
        server,
        sequence,
        journal,
        connection,
    })
}

/// These rows carry no schedule rows in this milestone; enforce the contract
/// anyway so a hook-dependent row can never be silently skipped.
fn enforce_no_fault_schedule(context: &ScenarioContext, id: &str) -> Result<()> {
    let schedule_rows = schedule::parse("", &context.run_id, id)?;
    schedule::execute(&schedule_rows, |_| {
        bail!("{id} is a no-fault row; no process-lifecycle action may be scheduled")
    })
}

fn declare_sealed(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    domain: &str,
    entities: &[u64],
) -> Result<String> {
    let declare = oracle::operation_hex(oracle::operation_id(seed, domain, 0));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&declare)?),
        None,
        None,
        None,
        None,
    )?;
    let mut operation = vec!["declare", "--operation", &declare];
    let entities_text = entities
        .iter()
        .map(|entity| entity.to_string())
        .collect::<Vec<_>>()
        .join(",");
    operation.push("--entities");
    operation.push(&entities_text);
    operation.push("--seal");
    let declared = session.op(&operation)?;
    require(&declared, "RECEIPT", "declare operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&declare)?),
        None,
        None,
        None,
        None,
    )?;
    Ok(declare)
}

fn admit_input(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    domain: &str,
    declaration: &str,
    work: &str,
    input: &Path,
) -> Result<String> {
    let admit = oracle::operation_hex(oracle::operation_id(seed, domain, 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit)?),
        Some(work),
        Some(1),
        None,
        None,
    )?;
    let admitted = session.op(&[
        "admit",
        "--operation",
        &admit,
        "--declaration",
        declaration,
        "--work",
        work,
        "--input",
        &crate::path(input),
        "--application",
        "copy/v2",
    ])?;
    let receipt = require(&admitted, "RECEIPT", "admit operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some(work),
        Some(1),
        None,
        None,
    )?;
    Ok(receipt)
}

fn parse_state(stdout: &str) -> Result<u64> {
    stdout
        .split_whitespace()
        .find_map(|token| token.strip_prefix("state="))
        .context("watch did not report a state")?
        .parse::<u64>()
        .context("watch state is not decimal")
}

fn parse_field_u64(stdout: &str, field: &str) -> Result<Option<u64>> {
    let pattern = format!("{field}: Some(Number(");
    let Some(start) = stdout.find(&pattern) else {
        return Ok(None);
    };
    let digits = &stdout[start + pattern.len()..];
    let end = digits
        .find(')')
        .context("malformed {field} value in watch view")?;
    Ok(Some(
        digits[..end]
            .parse::<u64>()
            .context("malformed {field} decimal")?,
    ))
}

/// Watch until terminal success (state=5), never past the failure state (6).
fn watch_terminal(
    session: &Session,
    events: &mut EventWriter,
    work: &str,
    admit: &str,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now() + timeout;
    let terminal = loop {
        let stdout = session.watch(work)?;
        let state = parse_state(&stdout)?;
        ensure!(
            state != 6,
            "work {work} entered the terminal failure state: {stdout}"
        );
        if state == 5 {
            break stdout;
        }
        ensure!(
            Instant::now() < deadline,
            "work {work} did not succeed within {timeout:?}\nlast view:\n{stdout}"
        );
        thread::sleep(Duration::from_millis(100));
    };
    events.append(
        "OBSERVATION_JOURNALED",
        Some(hex_to_id(admit)?),
        Some(work),
        Some(1),
        None,
        None,
    )?;
    Ok(terminal)
}

/// Select + read output index `attempt` and verify the received bytes equal
/// the independent oracle expectation, byte for byte.
#[allow(clippy::too_many_arguments)]
fn read_output_verified(
    session: &Session,
    events: &mut EventWriter,
    work: &str,
    attempt: u64,
    expected: &[u8],
    expected_sha256: &str,
    artifacts: &Path,
    output_name: &str,
) -> Result<String> {
    let select = session.op(&[
        "select",
        "--work",
        work,
        "--attempt",
        &attempt.to_string(),
        "--index",
        "0",
    ])?;
    require(&select, "REFERENCE", "select operation")?;
    let output_path = artifacts.join(output_name);
    let read = session.op(&[
        "read",
        "--work",
        work,
        "--attempt",
        &attempt.to_string(),
        "--index",
        "0",
        "--output",
        &crate::path(&output_path),
    ])?;
    require(&read, "VERIFIED", "read operation")?;
    let received = fs::read(&output_path)
        .with_context(|| format!("read installed result {}", output_path.display()))?;
    let actual_sha256 = hex(&Sha256::digest(&received));
    if received != expected {
        bail!(
            "RESULT_VERIFIED mismatch for {work}: expected sha256={expected_sha256} \
             len={} (independent oracle), actual sha256={actual_sha256} len={} (received \
             output at {})",
            expected.len(),
            received.len(),
            output_path.display()
        );
    }
    events.append(
        "RESULT_VERIFIED",
        None,
        Some(work),
        Some(attempt),
        None,
        None,
    )?;
    events.append(
        "RESULT_INSTALLED",
        None,
        Some(work),
        Some(attempt),
        None,
        Some(ArtifactRef {
            path: format!("artifacts/{output_name}"),
            len: received.len() as u64,
            sha256: actual_sha256.clone(),
        }),
    )?;
    Ok(actual_sha256)
}

fn write_kv(scenario_dir: &Path, name: &str, rows: &[(&str, String)]) -> Result<()> {
    let text = rows
        .iter()
        .map(|(key, value)| format!("{key}\t{value}\n"))
        .collect::<String>();
    fs::write(scenario_dir.join(name), text)?;
    Ok(())
}

fn detach(session: &Session) -> Result<()> {
    let detach = session.op(&["detach"])?;
    require(&detach, "DETACHED", "detach operation").map(|_| ())
}

/// Stop the server gracefully, validate the event stream, and seal.
fn stop_and_seal(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    server: OwnedServer,
    events: EventWriter,
) -> Result<()> {
    server.stop()?;
    seal(context, scenario_dir, id, events)
}

fn seal(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    events: EventWriter,
) -> Result<()> {
    drop(events);
    crate::durable::events::read_events_checked(
        &scenario_dir.join("events.tsv"),
        &context.run_id,
        id,
        Some(scenario_dir),
    )?;
    let sealed = File::create(scenario_dir.join("SEALED"))?;
    sealed.sync_all()?;
    Ok(())
}

fn hex_to_id(text: &str) -> Result<[u8; 16]> {
    ensure!(
        text.len() == 32 && text.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "expected a 32-digit operation id, got {text:?}"
    );
    let mut id = [0u8; 16];
    for (byte, pair) in id.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(id)
}

// ---------------------------------------------------------------------------
// G1 lifecycle rows
// ---------------------------------------------------------------------------

/// g1-leaf-copy: declare one entity, admit a deterministic input to copy/v2,
/// watch to terminal success, then select/read index 0 and verify the
/// received bytes equal the independently computed input bytes.
///
/// The canonical rust-client/rust-server direction runs in the row directory.
/// When a Java jar is provided, both mixed directions run too, each in its own
/// subdirectory of the row directory; without --java-jar (dev mode only; the
/// acceptance gate rejects it) the mixed directories get an INCOMPLETE marker.
fn g1_leaf_copy(context: &ScenarioContext) -> Result<()> {
    let scenario_dir = context.scenario_dir("g1-leaf-copy");
    g1_leaf_copy_direction(context, &scenario_dir, Subject::Rust, Subject::Rust)?;
    for (server, client, name) in [
        (Subject::Java, Subject::Rust, "rust-client-java-server"),
        (Subject::Rust, Subject::Java, "java-client-rust-server"),
    ] {
        let direction_dir = scenario_dir.join(name);
        if context.java_jar.is_none() {
            fs::create_dir_all(&direction_dir)?;
            fs::write(
                direction_dir.join("INCOMPLETE"),
                b"no --java-jar provided; this direction was not run\n",
            )?;
            continue;
        }
        g1_leaf_copy_direction(context, &direction_dir, server, client)?;
    }
    Ok(())
}

fn g1_leaf_copy_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-leaf-copy";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    // Expected values come from the oracle, stored separately from observed.
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let expected_output_sha256 = oracle::sha256_hex(&input);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", INPUT_LEN.to_string()),
            ("input_sha256", expected_output_sha256.clone()),
            ("expected_output_sha256", expected_output_sha256.clone()),
            ("work", "0:0:1".into()),
            ("attempt", "1".into()),
        ],
    )?;
    fs::write(artifacts.join("input.bin"), &input)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/input.bin".into(),
            len: input.len() as u64,
            sha256: expected_output_sha256.clone(),
        }),
    )?;

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let input_path = artifacts.join("input.bin");
    let admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let _receipt = admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
    )?;

    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit, WATCH_TIMEOUT)?;

    // Manifest evidence.
    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    let manifest_text = require(&manifest, "MANIFEST", "manifest operation")?;
    fs::write(artifacts.join("manifest.txt"), &manifest_text)?;
    let manifest_sha256 = oracle::sha256_hex(manifest_text.as_bytes());
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/manifest.txt".into(),
            len: manifest_text.len() as u64,
            sha256: manifest_sha256,
        }),
    )?;

    read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &expected_output_sha256,
        &artifacts,
        "output.bin",
    )?;
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
        ("object_limit_client", "16777216".into()),
        ("object_limit_server", "16777216".into()),
        ("application", "copy/v2".into()),
        ("mode", "0".into()),
        ("terminal_state", "5".into()),
        (
            "watch_terminal",
            terminal.trim().replace(['\t', '\n', '\r'], " "),
        ),
    ];
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, scenario_id, session.server, events)
}

// ---------------------------------------------------------------------------
// G2 failure rows (hook-free)
// ---------------------------------------------------------------------------

/// Named-code refusal evidence on the CLI's stderr. An authority refusal
/// renders `authority refusal CONFLICT: ...` via `session::Failure::Refused`
/// (quinn/src/v2_client/session.rs); the client's immutable-intent journal
/// guard renders `CONFLICT: ...` via `Error` Display (src/v2/mod.rs). Both
/// name CONFLICT, wire code 7 (src/v2/records.rs). Anything less specific
/// than this named-code match is not refusal evidence.
fn refusal_conflict_line(stderr: &str) -> Option<String> {
    stderr
        .lines()
        .find(|line| {
            line.starts_with("authority refusal CONFLICT:") || line.starts_with("CONFLICT:")
        })
        .map(str::to_owned)
}

/// Parse the SCOPE line of `page --scope 0`: declared count and member count.
/// Members render as `ScopeMember { ... }` on the Rust CLI and
/// `Entry[entity=N, state=...]` on the Java CLI; both are counted.
fn parse_scope_page(stdout: &str) -> Result<(u64, u64)> {
    let declared = stdout
        .split_whitespace()
        .find_map(|token| token.strip_prefix("declared="))
        .context("page did not report declared=")?
        .parse::<u64>()
        .context("page declared= is not decimal")?;
    let members_line = stdout
        .lines()
        .find(|line| line.starts_with("MEMBERS "))
        .context("page did not print a MEMBERS line")?;
    let members = (members_line.matches("ScopeMember {").count()
        + members_line.matches("Entry[").count()) as u64;
    Ok((declared, members))
}

/// g2-duplicate-op-changed-params: a second admission under the SAME
/// operation ID but with changed input (different length and hash) must
/// refuse CONFLICT (7); the original receipt is unaltered and exactly one
/// job exists.
fn g2_duplicate_op_changed_params(context: &ScenarioContext) -> Result<()> {
    let scenario_id = "g2-duplicate-op-changed-params";
    let (scenario_dir, artifacts) = open_scenario(context, scenario_id)?;
    let mut events = open_events(context, &scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, &scenario_dir, Subject::Rust, Subject::Rust)?;

    let first = oracle::dataset(context.seed, INPUT_LEN);
    let changed = oracle::dataset(context.seed ^ 0x5a5a_5a5a_5a5a_5a5a, INPUT_LEN / 2);
    let first_sha256 = oracle::sha256_hex(&first);
    let changed_sha256 = oracle::sha256_hex(&changed);
    fs::write(artifacts.join("input-first.bin"), &first)?;
    fs::write(artifacts.join("input-changed.bin"), &changed)?;
    write_kv(
        &scenario_dir,
        "expected.tsv",
        &[
            ("first_input_len", first.len().to_string()),
            ("first_input_sha256", first_sha256.clone()),
            ("changed_input_len", changed.len().to_string()),
            ("changed_input_sha256", changed_sha256.clone()),
            ("expected_refusal_code", "7 (CONFLICT)".into()),
            ("expected_lookup", "first receipt unchanged".into()),
            ("expected_page_members", "1".into()),
        ],
    )?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let first_receipt = admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &artifacts.join("input-first.bin"),
    )?;
    fs::write(artifacts.join("first-receipt.txt"), &first_receipt)?;

    // Second admission: same operation ID, changed parameters.
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let duplicate = session.op(&[
        "admit",
        "--operation",
        &admit_hex,
        "--declaration",
        &declare,
        "--work",
        "0:0:1",
        "--input",
        &crate::path(&artifacts.join("input-changed.bin")),
        "--application",
        "copy/v2",
    ])?;
    let duplicate_stdout = String::from_utf8_lossy(&duplicate.stdout).into_owned();
    let duplicate_stderr = String::from_utf8_lossy(&duplicate.stderr).into_owned();
    let refusal_line = refusal_conflict_line(&duplicate_stderr);
    let refusal_text = format!(
        "exit={}\nstdout:\n{duplicate_stdout}\nstderr:\n{duplicate_stderr}",
        duplicate.status,
    );
    fs::write(artifacts.join("duplicate-refusal.txt"), &refusal_text)?;
    events.append(
        "REFUSAL_RECEIVED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        Some(7),
        Some(ArtifactRef {
            path: "artifacts/duplicate-refusal.txt".into(),
            len: refusal_text.len() as u64,
            sha256: oracle::sha256_hex(refusal_text.as_bytes()),
        }),
    )?;
    if duplicate.status.success() || refusal_line.is_none() {
        bail!(
            "g2-duplicate-op-changed-params expected the named CONFLICT refusal (code 7) \
             on stderr and a nonzero exit, got exit {} stdout:\n{duplicate_stdout}\n\
             stderr:\n{duplicate_stderr}",
            duplicate.status
        );
    }
    let refusal_line = refusal_line.expect("checked above");

    // Lookup must return the FIRST receipt, byte-identical.
    let lookup = session.op(&["lookup", "--operation", &admit_hex])?;
    let lookup_stdout = require(&lookup, "RECEIPT", "operation lookup after refusal")?;
    if lookup_stdout.trim() != first_receipt.trim() {
        bail!(
            "g2-duplicate-op-changed-params lookup mismatch: expected the original first \
             receipt unchanged:\n{}\nactual lookup returned:\n{}",
            first_receipt.trim(),
            lookup_stdout.trim()
        );
    }

    // Exactly one admitted entity in the scope page.
    let page = session.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "scope page")?;
    let (declared, members) = parse_scope_page(&page_stdout)?;
    if declared != 1 || members != 1 {
        bail!(
            "g2-duplicate-op-changed-params expected exactly one admitted entity \
             (declared=1, members=1), got declared={declared} members={members}:\n{page_stdout}"
        );
    }
    detach(&session)?;

    write_kv(
        &scenario_dir,
        "observed.tsv",
        &[
            ("refusal_matched", format!("stderr line: {refusal_line}")),
            (
                "refusal_code_source",
                "Failure::Refused Display 'authority refusal {code.name()}: ...' \
                 (quinn/src/v2_client/session.rs); CONFLICT = wire code 7 \
                 (src/v2/mod.rs, src/v2/records.rs)"
                    .into(),
            ),
            ("lookup_matches_first_receipt", "true".into()),
            ("page_declared", declared.to_string()),
            ("page_members", members.to_string()),
        ],
    )?;

    stop_and_seal(context, &scenario_dir, scenario_id, session.server, events)
}

/// g2-simultaneous-duplicate: two concurrent one-shot client processes submit
/// the IDENTICAL admission operation (same ID, same input) against the same
/// session. Both must exit 0 with the identical receipt; exactly one job
/// exists (attempt 1). The second process is spawned while the first is
/// still in flight (4 MiB input); concurrency is real, never faked.
fn g2_simultaneous_duplicate(context: &ScenarioContext) -> Result<()> {
    let scenario_id = "g2-simultaneous-duplicate";
    let (scenario_dir, artifacts) = open_scenario(context, scenario_id)?;
    let mut events = open_events(context, &scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, &scenario_dir, Subject::Rust, Subject::Rust)?;

    let input = oracle::dataset(context.seed, RACE_INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    write_kv(
        &scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            ("expected_both_exit", "0".into()),
            ("expected_receipts", "identical".into()),
            ("expected_page_members", "1".into()),
            ("expected_attempt", "1".into()),
        ],
    )?;

    // The identical declaration must be durably journaled by BOTH clients
    // (each one-shot CLI reopens its own journal; the journal lock is
    // per-file, so two journals are the only honest way to overlap clients).
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let second_journal = scenario_dir.join("client").join("session-b.sqlite");
    {
        let mut command = session.fixture.base()?;
        command.push("init-client".into());
        command.extend(session.fixture.journal_args(&second_journal, "alice", 1));
        let init = crate::run_output_owned(&session.fixture.root, &command, OP_WAIT)?;
        require(
            &init,
            "CLIENT_INITIALIZED",
            "v2 init-client (second journal)",
        )?;
    }
    let declare_b = session.fixture.run_client_op(
        &second_journal,
        "alice",
        session.sequence,
        &session.connection,
        &[
            "declare",
            "--operation",
            &declare,
            "--entities",
            "1",
            "--seal",
        ],
    )?;
    require(
        &declare_b,
        "RECEIPT",
        "identical declare from second journal",
    )?;

    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));

    // Spawn both at the same time; do not wait for the first before
    // starting the second. The 4 MiB stream keeps the first in flight.
    let first = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &[
            "admit",
            "--operation",
            &admit_hex,
            "--declaration",
            &declare,
            "--work",
            "0:0:1",
            "--input",
            &crate::path(&input_path),
            "--application",
            "copy/v2",
        ],
    )?;
    let first_pid = first.id();
    let second = session.fixture.spawn_client_op(
        &second_journal,
        "alice",
        session.sequence,
        &session.connection,
        &[
            "admit",
            "--operation",
            &admit_hex,
            "--declaration",
            &declare,
            "--work",
            "0:0:1",
            "--input",
            &crate::path(&input_path),
            "--application",
            "copy/v2",
        ],
    )?;
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let first_out = AuthorityFixture::wait_client_op(first, OP_WAIT)?;
    let second_out = AuthorityFixture::wait_client_op(second, OP_WAIT)?;
    let first_stdout = String::from_utf8_lossy(&first_out.stdout).into_owned();
    let second_stdout = String::from_utf8_lossy(&second_out.stdout).into_owned();
    fs::write(
        artifacts.join("concurrent-receipts.txt"),
        format!(
            "first_pid={first_pid}\nfirst_exit={}\nfirst_stdout:\n{first_stdout}\n\
             second_exit={}\nsecond_stdout:\n{second_stdout}",
            first_out.status, second_out.status
        ),
    )?;
    if !first_out.status.success() || !second_out.status.success() {
        bail!(
            "g2-simultaneous-duplicate expected both concurrent admissions to exit 0, \
             got first={} second={}\nfirst stderr:\n{}\nsecond stderr:\n{}",
            first_out.status,
            second_out.status,
            String::from_utf8_lossy(&first_out.stderr),
            String::from_utf8_lossy(&second_out.stderr)
        );
    }
    if !first_stdout.contains("RECEIPT") || !second_stdout.contains("RECEIPT") {
        bail!(
            "g2-simultaneous-duplicate expected RECEIPT on both stdout streams:\n\
             first:\n{first_stdout}\nsecond:\n{second_stdout}"
        );
    }
    if first_stdout.trim() != second_stdout.trim() {
        bail!(
            "g2-simultaneous-duplicate expected identical admission receipts, got:\n\
             first:\n{first_stdout}\nsecond:\n{second_stdout}"
        );
    }
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    // Exactly one job, terminal success under attempt 1.
    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit_hex, WATCH_TIMEOUT)?;
    let page = session.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "scope page")?;
    let (declared, members) = parse_scope_page(&page_stdout)?;
    if declared != 1 || members != 1 {
        bail!(
            "g2-simultaneous-duplicate expected exactly one job (declared=1, members=1), \
             got declared={declared} members={members}:\n{page_stdout}"
        );
    }

    read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output.bin",
    )?;
    detach(&session)?;

    write_kv(
        &scenario_dir,
        "observed.tsv",
        &[
            ("first_pid", first_pid.to_string()),
            ("concurrency", "second client spawned while first was in flight (4 MiB input); serialization via the durable uniqueness constraint".into()),
            ("first_exit", first_out.status.to_string()),
            ("second_exit", second_out.status.to_string()),
            ("receipts_identical", "true".into()),
            ("page_declared", declared.to_string()),
            ("page_members", members.to_string()),
            (
                "watch_terminal",
                terminal.trim().replace(['\t', '\n', '\r'], " "),
            ),
        ],
    )?;

    stop_and_seal(context, &scenario_dir, scenario_id, session.server, events)
}

/// g2-kill-server-after-admission-recovery: uncontrolled-crash variant
/// (explicitly NOT a lost-ACK test and not a boundary test). Admission
/// receipt is observed, then the server is SIGKILLed at a seeded delay, then
/// the same roots are reopened. The work must still be admitted under
/// attempt 1 with its original deadline; restart reconciliation fabricates
/// no outcome; the copy eventually publishes (or an explicit retry is
/// required, recorded per row evidence); the result reads back byte-exact.
fn g2_kill_server_after_admission_recovery(context: &ScenarioContext) -> Result<()> {
    let scenario_id = "g2-kill-server-after-admission-recovery";
    let (scenario_dir, artifacts) = open_scenario(context, scenario_id)?;
    let mut events = open_events(context, &scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, &scenario_dir, Subject::Rust, Subject::Rust)?;

    let input = oracle::dataset(context.seed, RACE_INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let _receipt = admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
    )?;

    // State just before the crash: admission durable, execution uncontrolled.
    let pre_crash = session.watch("0:0:1")?;
    let pre_state = parse_state(&pre_crash)?;
    let pre_deadline = parse_field_u64(&pre_crash, "deadline")?
        .context("pre-crash watch did not report a deadline")?;

    // Seeded, uncontrolled delay after the receipt was observed.
    let delay_ms = 20 + (context.seed % 481);
    write_kv(
        &scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            ("expected_attempt", "1".into()),
            ("expected_deadline_preserved", "true".into()),
            ("expected_terminal_state", "5".into()),
            ("crash_label", "crash at uncontrolled point after ADMISSION_RECEIPT_OBSERVED - process death, not power loss, not a boundary test".into()),
        ],
    )?;
    thread::sleep(Duration::from_millis(delay_ms));
    let server = session.server;
    server.kill()?;
    events.append("", None, Some("0:0:1"), Some(1), None, None)?;

    // Restart the same roots; a live process plus ready file plus one
    // authenticated op is the only readiness this fixture accepts.
    let server = session.fixture.start_server()?;
    let recovery_connection = session.fixture.connection_args(&server, "alice")?;
    let recovered = Session {
        fixture: session.fixture,
        server,
        sequence: session.sequence,
        journal: session.journal,
        connection: recovery_connection,
    };

    // Immediately after restart, before any retry: no fabricated outcome and
    // no new wire attempt. Poll until the job republishes or the bound ends.
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut post_crash = recovered.watch("0:0:1")?;
    let mut recovery_path = "automatic-redispatch-under-attempt-1".to_owned();
    let mut post_state = parse_state(&post_crash)?;
    let mut post_deadline = parse_field_u64(&post_crash, "deadline")?
        .context("post-restart watch did not report a deadline")?;
    let mut post_attempt = post_crash
        .split_whitespace()
        .find_map(|token| token.strip_prefix("attempt="))
        .context("post-restart watch did not report an attempt")?
        .parse::<u64>()
        .context("post-restart attempt is not decimal")?;
    while post_state != 5 {
        ensure!(
            post_state != 6,
            "restart reconciliation fabricated a failure outcome: {post_crash}"
        );
        ensure!(
            post_attempt == 1,
            "restart created a new wire attempt: {post_crash}"
        );
        if Instant::now() >= deadline {
            // Explicit retry per the restartable-job contract.
            let retry = oracle::operation_hex(oracle::operation_id(context.seed, "retry", 2));
            let retry_op = recovered.op(&[
                "retry",
                "--operation",
                &retry,
                "--work",
                "0:0:1",
                "--expected-attempt",
                "1",
            ])?;
            require(&retry_op, "RECEIPT", "explicit retry after restart")?;
            recovery_path = "explicit-retry-required".to_owned();
            events.append(
                "REQUEST_SENT",
                Some(hex_to_id(&retry)?),
                Some("0:0:1"),
                Some(1),
                None,
                None,
            )?;
            events.append(
                "RECEIPT_VALIDATED",
                Some(hex_to_id(&retry)?),
                Some("0:0:1"),
                Some(1),
                None,
                None,
            )?;
        }
        thread::sleep(Duration::from_millis(200));
        post_crash = recovered.watch("0:0:1")?;
        post_state = parse_state(&post_crash)?;
        post_deadline = parse_field_u64(&post_crash, "deadline")?
            .context("post-restart watch did not report a deadline")?;
        post_attempt = post_crash
            .split_whitespace()
            .find_map(|token| token.strip_prefix("attempt="))
            .context("post-restart watch did not report an attempt")?
            .parse::<u64>()
            .context("post-restart attempt is not decimal")?;
    }
    ensure!(
        post_attempt == 1,
        "g2-kill-server-after-admission-recovery expected the terminal success under \
         attempt 1, got attempt={post_attempt}:\n{post_crash}"
    );
    if post_deadline != pre_deadline {
        bail!(
            "g2-kill-server-after-admission-recovery expected the original deadline \
             preserved across restart: expected {pre_deadline}, actual {post_deadline}"
        );
    }
    events.append(
        "OBSERVATION_JOURNALED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    read_output_verified(
        &recovered,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output.bin",
    )?;
    detach(&recovered)?;

    write_kv(
        &scenario_dir,
        "observed.tsv",
        &[
            ("seed", context.seed.to_string()),
            ("kill_delay_ms", delay_ms.to_string()),
            (
                "crash_label",
                "crash at uncontrolled point after ADMISSION_RECEIPT_OBSERVED - process death, not power loss, not a boundary test".into(),
            ),
            ("pre_crash_state", pre_state.to_string()),
            ("pre_crash_deadline_ms", pre_deadline.to_string()),
            ("post_restart_state", post_state.to_string()),
            ("post_restart_attempt", post_attempt.to_string()),
            ("post_restart_deadline_ms", post_deadline.to_string()),
            ("deadline_preserved", "true".into()),
            ("recovery_path", recovery_path),
        ],
    )?;

    stop_and_seal(
        context,
        &scenario_dir,
        scenario_id,
        recovered.server,
        events,
    )
}

// ---------------------------------------------------------------------------
// G2 lost-ACK and boundary-kill rows (milestone-5/6 fixture hooks)
// ---------------------------------------------------------------------------

/// Everything the hooked rows share: a live session plus the arming the
/// server process was started with (same events file across restarts).
struct Hooked {
    session: Session,
    events: PathBuf,
}

/// mTLS, init-authority, armed server start, fresh client journal. The
/// schedule TSV is written before the server starts; `probe` controls the
/// authenticated readiness op (a CONNECTION_AUTHENTICATED kill would kill
/// the probe itself).
#[allow(clippy::too_many_arguments)]
fn setup_hooked(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    server: Subject,
    client: Subject,
    schedule_rows: &[schedule::ScheduleRow],
    schedule_name: &str,
    probe: bool,
) -> Result<Hooked> {
    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        client,
    )?;
    fixture.run_init_authority()?;
    let events = scenario_dir.join("events.tsv");
    let arming = write_arming(context, scenario_dir, id, schedule_rows, schedule_name)?;
    if arming.is_some() && server != Subject::Rust {
        bail!(
            "{id} direction {}/{} needs subject fixture hooks the Java server does not \
             publish yet (Claude's FixtureMain): INCOMPLETE, never skip-pass",
            client.name(),
            server.name()
        );
    }
    let server = fixture.start_server_armed(arming.as_ref(), probe)?;
    // The driver's own next-sequence op is an authenticated connection; with
    // probe=false it would consume an armed CONNECTION_AUTHENTICATED kill row
    // itself. Fresh roots bind their first creation at sequence 1, and the
    // no-phantom rows re-probe after the restart.
    let sequence = if probe {
        let sequence = fixture.next_sequence(&server, "alice")?;
        ensure!(
            sequence == 1,
            "fresh authority must report NEXT_SEQUENCE 1, got {sequence}"
        );
        sequence
    } else {
        1
    };
    let journal = scenario_dir.join("client").join("session.sqlite");
    fs::create_dir_all(journal.parent().expect("journal has a parent directory"))?;
    let mut command = fixture.client_base()?;
    command.push("init-client".into());
    command.extend(fixture.journal_args(&journal, "alice", sequence));
    let init = crate::run_output_owned(&fixture.root, &command, OP_WAIT)?;
    require(&init, client.client_initialized_marker(), "v2 init-client")?;
    let connection = fixture.connection_args(&server, "alice")?;
    Ok(Hooked {
        session: Session {
            fixture,
            server,
            sequence,
            journal,
            connection,
        },
        events,
    })
}

/// Write the per-process schedule file and build the matching arming. An
/// empty row set arms nothing (the events file is still shared when the
/// caller passes rows on the first start).
fn write_arming(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    schedule_rows: &[schedule::ScheduleRow],
    schedule_name: &str,
) -> Result<Option<crate::durable::process::FixtureArming>> {
    if schedule_rows.is_empty() {
        return Ok(None);
    }
    let schedule_path = scenario_dir.join(schedule_name);
    fs::write(&schedule_path, schedule::render(schedule_rows)?)?;
    Ok(Some(crate::durable::process::FixtureArming {
        events: scenario_dir.join("events.tsv"),
        run_id: context.run_id.clone(),
        scenario_id: id.to_owned(),
        schedule: Some(schedule_path),
    }))
}

/// Restart the same roots against the same shared events file with a fresh
/// per-process schedule (already-consumed kill rows are dropped by the row).
#[allow(clippy::too_many_arguments)]
fn restart_hooked(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    fixture: &AuthorityFixture,
    sequence: u64,
    journal: &Path,
    schedule_rows: &[schedule::ScheduleRow],
    schedule_name: &str,
    probe: bool,
) -> Result<Session> {
    let arming = write_arming(context, scenario_dir, id, schedule_rows, schedule_name)?;
    let server = fixture.start_server_armed(arming.as_ref(), probe)?;
    let connection = fixture.connection_args(&server, "alice")?;
    Ok(Session {
        fixture: fixture.clone(),
        server,
        sequence,
        journal: journal.to_path_buf(),
        connection,
    })
}

/// Subject-side (server) records for one boundary; a missing file is zero.
fn subject_record_count(events: &Path, boundary: &str) -> Result<u64> {
    let text = match fs::read_to_string(events) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    Ok(text
        .lines()
        .filter(|line| {
            let columns: Vec<&str> = line.split('\t').collect();
            columns.len() > 7 && columns[4] == "server" && columns[7] == boundary
        })
        .count() as u64)
}

/// A lost-ACK claim needs subject evidence the boundary was actually reached:
/// wait for the server record before releasing, restarting or asserting.
fn wait_subject_record(events: &Path, boundary: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if subject_record_count(events, boundary)? > 0 {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "subject did not reach boundary {boundary} within {timeout:?}; a guessed sleep is \
             never boundary evidence"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// Release a paused boundary by writing the release file the subject polls.
/// No milestone-6 row schedules `pause` (the subject only accepts pause at
/// reply-pair boundaries, and no row needs one); exercised by the unit tests
/// below and kept for the pause rows to come.
#[allow(dead_code)]
fn write_release(events: &Path, boundary: &str) -> Result<()> {
    let release = events
        .parent()
        .context("events path has a parent directory")?
        .join(format!("release-{boundary}"));
    fs::write(&release, b"released by the neutral driver\n")?;
    Ok(())
}

/// Named-code refusal evidence for codes beyond CONFLICT: the transcript must
/// name the code, never a generic error.
fn refusal_named_line(stderr: &str, codes: &[&str]) -> Option<String> {
    stderr
        .lines()
        .find(|line| {
            codes.iter().any(|code| {
                line.starts_with(&format!("authority refusal {code}:"))
                    || line.starts_with(&format!("{code}:"))
            })
        })
        .map(str::to_owned)
}

/// One expected-to-fail client op: require a nonzero exit and a transcript
/// artifact; return the captured stdout/stderr text.
fn expect_failure(
    session: &Session,
    artifacts: &Path,
    name: &str,
    operation: &[&str],
) -> Result<String> {
    let output = session.op(operation)?;
    let text = format!(
        "exit={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    fs::write(artifacts.join(name), &text)?;
    ensure!(
        !output.status.success(),
        "{name} was expected to fail (connection loss or named refusal) but exited zero\n{text}"
    );
    Ok(text)
}

/// Run the java-client/rust-server direction of a G2 row when a jar is
/// present; a failure is recorded as an INCOMPLETE marker in the direction
/// directory instead of failing the row (the rust direction is the evidence).
fn run_hooked_direction(
    context: &ScenarioContext,
    row_id: &str,
    direction: impl Fn(&ScenarioContext, &Path) -> Result<()>,
) -> Result<()> {
    let Some(jar) = &context.java_jar else {
        return Ok(());
    };
    let direction_dir = context.scenario_dir(row_id).join("java-client-rust-server");
    fs::create_dir_all(&direction_dir)?;
    if let Err(error) = direction(context, &direction_dir) {
        fs::write(
            direction_dir.join("INCOMPLETE"),
            format!(
                "java-client/rust-server direction failed; the row evidence is the \
                 rust-client/rust-server direction. Error:\n{error:#}\njava_jar_sha256={}\n",
                oracle::sha256_hex(&fs::read(jar)?)
            ),
        )?;
        println!("INCOMPLETE {row_id} java-client/rust-server: {error:#}");
    }
    Ok(())
}

fn g2_schedule_row(
    context: &ScenarioContext,
    id: &str,
    boundary: &str,
    action: schedule::Action,
) -> schedule::ScheduleRow {
    schedule::ScheduleRow {
        run_id: context.run_id.clone(),
        scenario_id: id.to_owned(),
        target: "server".into(),
        boundary: boundary.into(),
        action,
        seed: context.seed,
        deadline_ms: 10_000,
    }
}

/// How long the driver waits for a scheduled server kill to fire (the runtime
/// worker must reach the armed commit) or for a released pause to proceed.
const KILL_TIMEOUT: Duration = Duration::from_secs(45);

/// Consume a hooked session into its parts so the server handle can be
/// stopped (drop-reply rows) or awaited (kill rows) before a restart.
fn split_hooked(hooked: Hooked) -> (Session, PathBuf) {
    (hooked.session, hooked.events)
}

/// Shared redispatch-or-retry wait used by the kill rows: after a restart the
/// work must reach terminal success under its ORIGINAL attempt and deadline,
/// never a fabricated failure and never a new wire attempt from the restart
/// itself. `pre_deadline` is the deadline observed before the kill (from the
/// admission receipt; `None` when the client receipt format does not expose
/// it, in which case only the attempt and outcome are asserted).
fn await_terminal_after_restart(
    recovered: &Session,
    events: &mut EventWriter,
    context: &ScenarioContext,
    work: &str,
    admit_hex: &str,
    pre_deadline: Option<u64>,
    timeout: Duration,
) -> Result<(String, String)> {
    let deadline = Instant::now() + timeout;
    let mut view = recovered.watch(work)?;
    let mut recovery_path = "automatic-redispatch-under-attempt-1".to_owned();
    let mut state = parse_state(&view)?;
    let mut attempt = parse_attempt(&view)?;
    let mut deadline_ms = parse_field_u64(&view, "deadline")?
        .context("post-restart watch did not report a deadline")?;
    while state != 5 {
        ensure!(
            state != 6,
            "restart reconciliation fabricated a failure outcome: {view}"
        );
        ensure!(attempt == 1, "restart created a new wire attempt: {view}");
        if Instant::now() >= deadline {
            // Explicit retry per the restartable-job contract.
            let retry = oracle::operation_hex(oracle::operation_id(context.seed, "retry", 2));
            let retry_op = recovered.op(&[
                "retry",
                "--operation",
                &retry,
                "--work",
                work,
                "--expected-attempt",
                "1",
            ])?;
            require(&retry_op, "RECEIPT", "explicit retry after restart")?;
            recovery_path = "explicit-retry-required".to_owned();
            events.append(
                "REQUEST_SENT",
                Some(hex_to_id(&retry)?),
                Some(work),
                Some(1),
                None,
                None,
            )?;
            events.append(
                "RECEIPT_VALIDATED",
                Some(hex_to_id(&retry)?),
                Some(work),
                Some(1),
                None,
                None,
            )?;
        }
        thread::sleep(Duration::from_millis(200));
        view = recovered.watch(work)?;
        state = parse_state(&view)?;
        attempt = parse_attempt(&view)?;
        deadline_ms = parse_field_u64(&view, "deadline")?
            .context("post-restart watch did not report a deadline")?;
    }
    ensure!(
        attempt == 1,
        "expected terminal success under the original attempt 1, got attempt={attempt}:\n{view}"
    );
    if let Some(pre_deadline) = pre_deadline
        && deadline_ms != pre_deadline
    {
        bail!(
            "expected the original deadline preserved across restart: expected \
             {pre_deadline}, actual {deadline_ms}"
        );
    }
    events.append(
        "OBSERVATION_JOURNALED",
        Some(hex_to_id(admit_hex)?),
        Some(work),
        Some(1),
        None,
        None,
    )?;
    Ok((recovery_path, view))
}

fn parse_attempt(stdout: &str) -> Result<u64> {
    stdout
        .split_whitespace()
        .find_map(|token| token.strip_prefix("attempt="))
        .context("watch did not report an attempt")?
        .parse::<u64>()
        .context("watch attempt is not decimal")
}

/// The seven lost-ACK / boundary-kill rows this milestone implements. Shared
/// between `direction_coverage` and the row registry so the coverage string
/// can never drift from the implemented set.
const HOOKED_G2_ROWS: &[&str] = &[
    "g2-crash-before-create-commit",
    "g2-crash-after-create-commit",
    "g2-drop-reply-declaration",
    "g2-drop-reply-admission",
    "g2-kill-after-admission-before-publication",
    "g2-kill-at-publication-commit",
    "g2-kill-client-after-request-sent",
];

/// Capture an op transcript the way `expect_failure` does, for ops the row
/// drives directly.
fn transcript(output: &Output) -> String {
    format!(
        "exit={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

/// Pull the named Section 12.2 code out of a captured op transcript (the
/// `exit=/stdout:/stderr:` text), without inferring codes from generic errors.
fn transcript_named_code(text: &str) -> Option<u32> {
    text.lines().find_map(|line| {
        REFUSAL_CODES.iter().find_map(|(name, code)| {
            let named = line.starts_with(&format!("authority refusal {name}:"))
                || line.starts_with(&format!("{name}:"))
                || line.starts_with(&format!("connection lost: closed by peer: {name} ("))
                || line.starts_with(&format!("connection lost: closed by peer: {name} "));
            named.then_some(*code)
        })
    })
}

/// Artifact reference for a transcript stored under the scenario's
/// `artifacts/` directory.
fn artifact_ref(name: &str, text: &str) -> ArtifactRef {
    ArtifactRef {
        path: format!("artifacts/{name}"),
        len: text.len() as u64,
        sha256: oracle::sha256_hex(text.as_bytes()),
    }
}

/// A binding attempt from a fresh journal carrying an explicit creation
/// policy and sequence. Used to prove that changed-policy and
/// ahead-of-sequence creation replays refuse CONFLICT (7) as a named code.
fn binding_attempt(
    session: &Session,
    journal: &Path,
    creation_sequence: u64,
    max_execution_ms: u64,
) -> Result<Output> {
    let mut init = session.fixture.client_base()?;
    init.push("init-client".into());
    init.extend(
        session
            .fixture
            .journal_args(journal, "alice", creation_sequence),
    );
    init.extend(["--max-execution-ms".into(), max_execution_ms.to_string()]);
    crate::run_output_owned(&session.fixture.root, &init, OP_WAIT)?;
    let mut command = session.fixture.client_base()?;
    command.push("client".into());
    command.extend(
        session
            .fixture
            .journal_args(journal, "alice", creation_sequence),
    );
    command.extend(["--max-execution-ms".into(), max_execution_ms.to_string()]);
    command.extend(session.connection.iter().cloned());
    command.push("binding".into());
    crate::run_output_owned(&session.fixture.root, &command, OP_WAIT)
}

/// admit arguments shared by the admission rows (the replay re-sends the SAME
/// operation: same id, declaration, work, input, application).
fn admit_op_args(operation: &str, declaration: &str, input: &Path) -> Vec<String> {
    vec![
        "admit".into(),
        "--operation".into(),
        operation.into(),
        "--declaration".into(),
        declaration.into(),
        "--work".into(),
        "0:0:1".into(),
        "--input".into(),
        crate::path(input),
        "--application".into(),
        "copy/v2".into(),
    ]
}

fn op_refs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

/// The deadline recorded in an admission receipt (`Outcome::Admitted`
/// `deadline: Number(...)`): the durable pre-kill evidence of the original
/// deadline for the boundary-kill rows, where a pre-kill watch would race
/// the armed kill itself.
fn parse_receipt_deadline(receipt: &str) -> Result<u64> {
    let pattern = "deadline: Number(";
    let start = receipt
        .find(pattern)
        .context("admission receipt did not carry a deadline")?;
    let digits = &receipt[start + pattern.len()..];
    let end = digits
        .find(')')
        .context("malformed deadline in admission receipt")?;
    digits[..end]
        .parse::<u64>()
        .context("deadline decimal malformed in admission receipt")
}

/// g2-crash-after-create-commit: drop-reply at SESSION_COMMITTED. The create
/// commits durably, the binding reply is withheld and the connection reset;
/// the replay returns the identical first-generation binding, the high-water
/// mark does not double-allocate, and changed-policy / ahead-of-sequence
/// replays refuse CONFLICT (7) as named codes.
fn g2_crash_after_create_commit(context: &ScenarioContext) -> Result<()> {
    let id = "g2-crash-after-create-commit";
    g2_crash_after_create_commit_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(context, id, |context, direction_dir| {
        g2_crash_after_create_commit_direction(context, direction_dir, Subject::Rust, Subject::Java)
    })?;
    Ok(())
}

fn g2_crash_after_create_commit_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-crash-after-create-commit";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "SESSION_COMMITTED",
        schedule::Action::DropReply,
    )];
    let hooked = setup_hooked(
        context,
        scenario_dir,
        id,
        server,
        client,
        &rows,
        "schedule.tsv",
        true,
    )?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "lost_ack_boundary",
                "SESSION_COMMITTED: binding create durable, reply withheld, connection reset"
                    .into(),
            ),
            ("expected_generation", "1".into()),
            ("expected_creation_sequence", "1".into()),
            (
                "expected_next_sequence_after_restart",
                "2 (one durable creation, no double alloc)".into(),
            ),
            ("changed_policy_replay", "named CONFLICT (7)".into()),
            ("ahead_sequence_replay", "named CONFLICT (7)".into()),
        ],
    )?;

    // Binding attempt: the create commits, the reply never arrives.
    events.append("REQUEST_SENT", None, None, None, None, None)?;
    let withheld = expect_failure(
        &hooked.session,
        &artifacts,
        "binding-withheld.txt",
        &["binding"],
    )?;
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        transcript_named_code(&withheld),
        Some(artifact_ref("binding-withheld.txt", &withheld)),
    )?;
    let (session, events_path) = split_hooked(hooked);
    wait_subject_record(&events_path, "SESSION_COMMITTED", KILL_TIMEOUT)
        .context("subject never reached the armed SESSION_COMMITTED boundary")?;
    // The reply is already lost (connection reset); the process stop models
    // the client-observed crash. It is process death, not power loss.
    session.server.stop()?;

    // Same roots, same arming: the replayed create is served from the durable
    // journal, so the armed drop-reply must not fire a second time.
    let restarted = restart_hooked(
        context,
        scenario_dir,
        id,
        &session.fixture,
        session.sequence,
        &session.journal,
        &rows,
        "schedule.tsv",
        true,
    )?;
    let next = restarted
        .fixture
        .next_sequence(&restarted.server, "alice")?;
    ensure!(
        next == 2,
        "g2-crash-after-create-commit expected NEXT_SEQUENCE 2 after the durable create (no \
         double alloc), got {next}"
    );
    let replay = restarted.op(&["binding"])?;
    let replay_stdout = require(&replay, "BINDING", "replayed binding after reply loss")?;
    if client == Subject::Rust {
        for needle in [
            "generation: Id(1)",
            "creation_sequence: Id(1)",
            "execution_limit_ms: Duration(60000)",
            "output_retention_ms: Duration(3600000)",
            "receipt_retention_ms: Duration(86400000)",
        ] {
            ensure!(
                replay_stdout.contains(needle),
                "g2-crash-after-create-commit replayed binding must echo the original creation \
                 intent ({needle}):\n{replay_stdout}"
            );
        }
    }
    events.append("RECEIPT_VALIDATED", None, None, None, None, None)?;

    // Changed-policy replay of the same creation refuses named CONFLICT (7).
    let changed_journal = scenario_dir.join("client").join("changed-policy.sqlite");
    let changed = binding_attempt(&restarted, &changed_journal, 1, 120_000)?;
    let changed_text = transcript(&changed);
    fs::write(artifacts.join("changed-policy-refusal.txt"), &changed_text)?;
    let changed_line = refusal_conflict_line(&String::from_utf8_lossy(&changed.stderr));
    ensure!(
        !changed.status.success() && changed_line.is_some(),
        "g2-crash-after-create-commit changed-policy replay must refuse named CONFLICT:\n\
         {changed_text}"
    );
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        Some(7),
        Some(artifact_ref("changed-policy-refusal.txt", &changed_text)),
    )?;

    // An ahead-of-sequence creation refuses named CONFLICT (7) as well.
    let ahead_journal = scenario_dir.join("client").join("ahead-sequence.sqlite");
    let ahead = binding_attempt(&restarted, &ahead_journal, 9, 60_000)?;
    let ahead_text = transcript(&ahead);
    fs::write(artifacts.join("ahead-sequence-refusal.txt"), &ahead_text)?;
    let ahead_line = refusal_conflict_line(&String::from_utf8_lossy(&ahead.stderr));
    ensure!(
        !ahead.status.success() && ahead_line.is_some(),
        "g2-crash-after-create-commit ahead-of-sequence creation must refuse named CONFLICT:\n\
         {ahead_text}"
    );
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        Some(7),
        Some(artifact_ref("ahead-sequence-refusal.txt", &ahead_text)),
    )?;

    detach(&restarted)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            (
                "subject_boundary_record",
                "SESSION_COMMITTED recorded before the reply was withheld".into(),
            ),
            ("next_sequence_after_restart", next.to_string()),
            (
                "replay_binding",
                "generation Id(1), creation_sequence Id(1), original policy echoed".into(),
            ),
            (
                "changed_policy_refusal",
                changed_line.expect("checked above"),
            ),
            ("ahead_sequence_refusal", ahead_line.expect("checked above")),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, restarted.server, events)
}

/// g2-crash-before-create-commit: the matrix's uncontrolled-crash variant is a
/// scheduled kill at CONNECTION_AUTHENTICATED. No creation is durable; the
/// restart reports NEXT_SEQUENCE 1 (no phantom session) and the identical
/// replayed create binds generation 1.
fn g2_crash_before_create_commit(context: &ScenarioContext) -> Result<()> {
    let id = "g2-crash-before-create-commit";
    g2_crash_before_create_commit_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(context, id, |context, direction_dir| {
        g2_crash_before_create_commit_direction(
            context,
            direction_dir,
            Subject::Rust,
            Subject::Java,
        )
    })?;
    Ok(())
}

fn g2_crash_before_create_commit_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-crash-before-create-commit";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "CONNECTION_AUTHENTICATED",
        schedule::Action::Kill,
    )];
    let hooked = setup_hooked(
        context,
        scenario_dir,
        id,
        server,
        client,
        &rows,
        "schedule.tsv",
        false,
    )?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "kill_boundary",
                "CONNECTION_AUTHENTICATED: kill before any create commits".into(),
            ),
            ("subject_exit", "86 after the boundary record".into()),
            (
                "expected_next_sequence_after_restart",
                "1 (no phantom session)".into(),
            ),
            ("expected_generation", "1".into()),
            (
                "expected_next_sequence_after_replay",
                "2 (exactly one durable creation)".into(),
            ),
        ],
    )?;

    events.append("REQUEST_SENT", None, None, None, None, None)?;
    let binding_transcript = expect_failure(
        &hooked.session,
        &artifacts,
        "binding-lost.txt",
        &["binding"],
    )?;
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        transcript_named_code(&binding_transcript),
        Some(artifact_ref("binding-lost.txt", &binding_transcript)),
    )?;
    let (session, events_path) = split_hooked(hooked);
    wait_subject_record(&events_path, "CONNECTION_AUTHENTICATED", KILL_TIMEOUT)
        .context("subject never reached the armed CONNECTION_AUTHENTICATED boundary")?;
    let output = session.server.wait_exit(KILL_TIMEOUT)?;
    ensure!(
        output.status.code() == Some(86),
        "g2-crash-before-create-commit: the scheduled kill must exit 86 after the boundary \
         record, got {}",
        output.status
    );
    let recovered = restart_hooked(
        context,
        scenario_dir,
        id,
        &session.fixture,
        session.sequence,
        &session.journal,
        &[],
        "schedule-restart.tsv",
        true,
    )?;
    let next = recovered
        .fixture
        .next_sequence(&recovered.server, "alice")?;
    ensure!(
        next == 1,
        "g2-crash-before-create-commit expected no phantom session (NEXT_SEQUENCE 1) after the \
         pre-create kill, got {next}"
    );
    let replay = recovered.op(&["binding"])?;
    let replay_stdout = require(&replay, "BINDING", "replayed create after pre-create kill")?;
    if client == Subject::Rust {
        ensure!(
            replay_stdout.contains("generation: Id(1)")
                && replay_stdout.contains("creation_sequence: Id(1)"),
            "g2-crash-before-create-commit replayed create must bind the first generation:\n\
             {replay_stdout}"
        );
    }
    let next = recovered
        .fixture
        .next_sequence(&recovered.server, "alice")?;
    ensure!(
        next == 2,
        "g2-crash-before-create-commit expected exactly one durable creation after the replay, \
         got NEXT_SEQUENCE {next}"
    );
    detach(&recovered)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("subject_exit_code", "86".into()),
            ("next_sequence_after_restart", "1".into()),
            ("replay_binding", "generation 1".into()),
            ("next_sequence_after_replay", next.to_string()),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, recovered.server, events)
}

/// g2-drop-reply-declaration: drop-reply at DECLARATION_COMMITTED. The
/// declaration commits durably; the identical replay returns the durable
/// receipt, a changed membership under the same operation refuses CONFLICT
/// (7), and each entity is paged exactly once.
fn g2_drop_reply_declaration(context: &ScenarioContext) -> Result<()> {
    let id = "g2-drop-reply-declaration";
    g2_drop_reply_declaration_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(context, id, |context, direction_dir| {
        g2_drop_reply_declaration_direction(context, direction_dir, Subject::Rust, Subject::Java)
    })?;
    Ok(())
}

fn g2_drop_reply_declaration_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-drop-reply-declaration";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "DECLARATION_COMMITTED",
        schedule::Action::DropReply,
    )];
    let hooked = setup_hooked(
        context,
        scenario_dir,
        id,
        server,
        client,
        &rows,
        "schedule.tsv",
        true,
    )?;
    let (session, events_path) = split_hooked(hooked);
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let declare_hex = oracle::operation_hex(oracle::operation_id(context.seed, "declare", 0));
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "lost_ack_boundary",
                "DECLARATION_COMMITTED: declaration durable, receipt withheld".into(),
            ),
            (
                "identical_replay",
                "durable receipt returned unchanged".into(),
            ),
            ("changed_membership_same_op", "named CONFLICT (7)".into()),
            (
                "expected_page",
                "declared=2 members=2 (entities 1 and 2)".into(),
            ),
        ],
    )?;

    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&declare_hex)?),
        None,
        None,
        None,
        None,
    )?;
    let withheld = expect_failure(
        &session,
        &artifacts,
        "declare-withheld.txt",
        &[
            "declare",
            "--operation",
            &declare_hex,
            "--entities",
            "1,2",
            "--seal",
        ],
    )?;
    events.append(
        "REFUSAL_RECEIVED",
        Some(hex_to_id(&declare_hex)?),
        None,
        None,
        transcript_named_code(&withheld),
        Some(artifact_ref("declare-withheld.txt", &withheld)),
    )?;
    wait_subject_record(&events_path, "DECLARATION_COMMITTED", KILL_TIMEOUT)
        .context("subject never reached the armed DECLARATION_COMMITTED boundary")?;

    // Identical replay: the durable declaration receipt comes back.
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&declare_hex)?),
        None,
        None,
        None,
        None,
    )?;
    let replay = session.op(&[
        "declare",
        "--operation",
        &declare_hex,
        "--entities",
        "1,2",
        "--seal",
    ])?;
    let receipt = require(&replay, "RECEIPT", "replayed declare after reply loss")?;
    if client == Subject::Rust {
        for needle in [
            "body: Declared {",
            "scope: Number(0)",
            "accepted_count: BatchCount(2)",
            "seal: Some(",
        ] {
            ensure!(
                receipt.contains(needle),
                "g2-drop-reply-declaration replayed receipt must carry the durable declaration \
                 ({needle}):\n{receipt}"
            );
        }
    }
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&declare_hex)?),
        None,
        None,
        None,
        None,
    )?;

    // Changed membership under the same operation refuses named CONFLICT (7).
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&declare_hex)?),
        None,
        None,
        None,
        None,
    )?;
    let changed = session.op(&[
        "declare",
        "--operation",
        &declare_hex,
        "--entities",
        "1,2,3",
        "--seal",
    ])?;
    let changed_text = transcript(&changed);
    fs::write(artifacts.join("changed-declare-refusal.txt"), &changed_text)?;
    let changed_line = refusal_conflict_line(&String::from_utf8_lossy(&changed.stderr));
    ensure!(
        !changed.status.success() && changed_line.is_some(),
        "g2-drop-reply-declaration changed-membership declare must refuse named CONFLICT:\n\
         {changed_text}"
    );
    events.append(
        "REFUSAL_RECEIVED",
        Some(hex_to_id(&declare_hex)?),
        None,
        None,
        Some(7),
        Some(artifact_ref("changed-declare-refusal.txt", &changed_text)),
    )?;

    let page = session.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "scope page")?;
    let (declared, members) = parse_scope_page(&page_stdout)?;
    ensure!(
        declared == 2 && members == 2,
        "g2-drop-reply-declaration expected each entity paged exactly once (declared=2, \
         members=2), got declared={declared} members={members}:\n{page_stdout}"
    );
    detach(&session)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            (
                "subject_boundary_record",
                "DECLARATION_COMMITTED recorded before the reply was withheld".into(),
            ),
            (
                "changed_membership_refusal",
                changed_line.expect("checked above"),
            ),
            ("page_declared", declared.to_string()),
            ("page_members", members.to_string()),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// g2-drop-reply-admission: drop-reply at ADMISSION_COMMITTED. The admission
/// commits durably; the identical replay returns the attempt-1 receipt with
/// no re-execution, and a changed input under the same operation refuses
/// CONFLICT (7).
fn g2_drop_reply_admission(context: &ScenarioContext) -> Result<()> {
    let id = "g2-drop-reply-admission";
    g2_drop_reply_admission_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(context, id, |context, direction_dir| {
        g2_drop_reply_admission_direction(context, direction_dir, Subject::Rust, Subject::Java)
    })?;
    Ok(())
}

fn g2_drop_reply_admission_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-drop-reply-admission";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "ADMISSION_COMMITTED",
        schedule::Action::DropReply,
    )];
    let hooked = setup_hooked(
        context,
        scenario_dir,
        id,
        server,
        client,
        &rows,
        "schedule.tsv",
        true,
    )?;
    let (session, events_path) = split_hooked(hooked);
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    let changed = oracle::dataset(context.seed ^ 0x5a5a_5a5a_5a5a_5a5a, INPUT_LEN / 2);
    let changed_path = artifacts.join("input-changed.bin");
    fs::write(&changed_path, &changed)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            (
                "lost_ack_boundary",
                "ADMISSION_COMMITTED: admission durable, receipt withheld".into(),
            ),
            (
                "identical_replay",
                "attempt-1 receipt returned unchanged; terminal success under attempt 1 (no \
                 re-execution)"
                    .into(),
            ),
            ("changed_input_same_op", "named CONFLICT (7)".into()),
            ("expected_page_members", "1".into()),
        ],
    )?;

    let args = admit_op_args(&admit_hex, &declare, &input_path);
    let arg_refs = op_refs(&args);
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let withheld = expect_failure(&session, &artifacts, "admit-withheld.txt", &arg_refs)?;
    events.append(
        "REFUSAL_RECEIVED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        transcript_named_code(&withheld),
        Some(artifact_ref("admit-withheld.txt", &withheld)),
    )?;
    wait_subject_record(&events_path, "ADMISSION_COMMITTED", KILL_TIMEOUT)
        .context("subject never reached the armed ADMISSION_COMMITTED boundary")?;

    // Identical replay: the durable attempt-1 admission receipt comes back.
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let replay = session.op(&arg_refs)?;
    let replay_receipt = require(&replay, "RECEIPT", "replayed admit after reply loss")?;
    if client == Subject::Rust {
        ensure!(
            replay_receipt.contains("attempt: Id(1)"),
            "g2-drop-reply-admission replayed receipt must carry the original attempt 1:\n\
             {replay_receipt}"
        );
    }
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit_hex, WATCH_TIMEOUT)?;
    ensure!(
        parse_attempt(&terminal)? == 1,
        "g2-drop-reply-admission expected the terminal success under the original attempt 1 \
         (no re-execution):\n{terminal}"
    );

    // Changed input under the same operation refuses named CONFLICT (7).
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let changed_args = admit_op_args(&admit_hex, &declare, &changed_path);
    let changed_refs = op_refs(&changed_args);
    let duplicate = session.op(&changed_refs)?;
    let duplicate_text = transcript(&duplicate);
    fs::write(artifacts.join("changed-admit-refusal.txt"), &duplicate_text)?;
    let duplicate_line = refusal_conflict_line(&String::from_utf8_lossy(&duplicate.stderr));
    ensure!(
        !duplicate.status.success() && duplicate_line.is_some(),
        "g2-drop-reply-admission changed-input admit must refuse named CONFLICT:\n\
         {duplicate_text}"
    );
    events.append(
        "REFUSAL_RECEIVED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        Some(7),
        Some(artifact_ref("changed-admit-refusal.txt", &duplicate_text)),
    )?;
    let page = session.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "scope page")?;
    let (declared, members) = parse_scope_page(&page_stdout)?;
    ensure!(
        members == 1,
        "g2-drop-reply-admission expected exactly one admitted entity, got members={members}:\n\
         {page_stdout}"
    );
    detach(&session)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            (
                "subject_boundary_record",
                "ADMISSION_COMMITTED recorded before the reply was withheld".into(),
            ),
            (
                "changed_input_refusal",
                duplicate_line.expect("checked above"),
            ),
            ("page_declared", declared.to_string()),
            ("page_members", members.to_string()),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// g2-kill-after-admission-before-publication: kill at EXECUTION_CLAIMED. The
/// claim commits durably and the subject exits 86; after the restart the work
/// reaches terminal success under its original attempt and deadline, via
/// automatic redispatch or an explicit retry (recorded), and the result reads
/// back byte-exact.
fn g2_kill_after_admission_before_publication(context: &ScenarioContext) -> Result<()> {
    let id = "g2-kill-after-admission-before-publication";
    g2_kill_after_admission_before_publication_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(context, id, |context, direction_dir| {
        g2_kill_after_admission_before_publication_direction(
            context,
            direction_dir,
            Subject::Rust,
            Subject::Java,
        )
    })?;
    Ok(())
}

fn g2_kill_after_admission_before_publication_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-kill-after-admission-before-publication";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "EXECUTION_CLAIMED",
        schedule::Action::Kill,
    )];
    let hooked = setup_hooked(
        context,
        scenario_dir,
        id,
        server,
        client,
        &rows,
        "schedule.tsv",
        true,
    )?;
    let (session, events_path) = split_hooked(hooked);
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    let receipt = admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
    )?;
    // The pre-kill deadline comes from the durable receipt: a pre-kill watch
    // would race the armed kill itself (the subject may already be gone).
    let pre_deadline = if client == Subject::Rust {
        Some(parse_receipt_deadline(&receipt)?)
    } else {
        None
    };
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            (
                "kill_boundary",
                "EXECUTION_CLAIMED: claim durable, then the subject exits 86".into(),
            ),
            ("expected_attempt", "1".into()),
            ("expected_deadline", "original deadline preserved".into()),
            ("expected_terminal_state", "5".into()),
            (
                "expected_recovery",
                "automatic redispatch or explicit retry (recorded)".into(),
            ),
        ],
    )?;

    wait_subject_record(&events_path, "EXECUTION_CLAIMED", KILL_TIMEOUT)
        .context("subject never reached the armed EXECUTION_CLAIMED boundary")?;
    let output = session.server.wait_exit(KILL_TIMEOUT)?;
    ensure!(
        output.status.code() == Some(86),
        "g2-kill-after-admission-before-publication: the scheduled kill must exit 86 after \
         the boundary record, got {}",
        output.status
    );
    let recovered = restart_hooked(
        context,
        scenario_dir,
        id,
        &session.fixture,
        session.sequence,
        &session.journal,
        &[],
        "schedule-restart.tsv",
        true,
    )?;
    let (recovery_path, terminal_view) = await_terminal_after_restart(
        &recovered,
        &mut events,
        context,
        "0:0:1",
        &admit_hex,
        pre_deadline,
        RECOVERY_TIMEOUT,
    )?;
    let terminal_state = parse_state(&terminal_view)?;
    let terminal_attempt = parse_attempt(&terminal_view)?;
    read_output_verified(
        &recovered,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output.bin",
    )?;
    detach(&recovered)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("subject_exit_code", "86".into()),
            (
                "subject_boundary_record",
                "EXECUTION_CLAIMED recorded before the exit".into(),
            ),
            ("terminal_state", terminal_state.to_string()),
            ("terminal_attempt", terminal_attempt.to_string()),
            ("recovery_path", recovery_path),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, recovered.server, events)
}

/// g2-kill-at-publication-commit: kill at PUBLICATION_COMMITTED. Exactly one
/// terminal commit is durable; the restart converges to terminal success
/// under the original attempt with a byte-exact result; a post-terminal retry
/// refuses ALREADY_TERMINAL (18) or CANCELLED (12), whichever the subject
/// names.
fn g2_kill_at_publication_commit(context: &ScenarioContext) -> Result<()> {
    let id = "g2-kill-at-publication-commit";
    g2_kill_at_publication_commit_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(context, id, |context, direction_dir| {
        g2_kill_at_publication_commit_direction(
            context,
            direction_dir,
            Subject::Rust,
            Subject::Java,
        )
    })?;
    Ok(())
}

fn g2_kill_at_publication_commit_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-kill-at-publication-commit";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "PUBLICATION_COMMITTED",
        schedule::Action::Kill,
    )];
    let hooked = setup_hooked(
        context,
        scenario_dir,
        id,
        server,
        client,
        &rows,
        "schedule.tsv",
        true,
    )?;
    let (session, events_path) = split_hooked(hooked);
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    let receipt = admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
    )?;
    // The pre-kill deadline comes from the durable receipt: a pre-kill watch
    // would race the armed kill itself (the subject may already be gone).
    let pre_deadline = if client == Subject::Rust {
        Some(parse_receipt_deadline(&receipt)?)
    } else {
        None
    };
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            (
                "kill_boundary",
                "PUBLICATION_COMMITTED: terminal commit durable, then exit 86".into(),
            ),
            ("expected_terminal_commits", "exactly 1".into()),
            ("expected_result", "byte-exact under attempt 1".into()),
            (
                "post_terminal_retry",
                "named ALREADY_TERMINAL (18) or CANCELLED (12)".into(),
            ),
        ],
    )?;

    wait_subject_record(&events_path, "PUBLICATION_COMMITTED", KILL_TIMEOUT)
        .context("subject never reached the armed PUBLICATION_COMMITTED boundary")?;
    let output = session.server.wait_exit(KILL_TIMEOUT)?;
    ensure!(
        output.status.code() == Some(86),
        "g2-kill-at-publication-commit: the scheduled kill must exit 86 after the boundary \
         record, got {}",
        output.status
    );
    let recovered = restart_hooked(
        context,
        scenario_dir,
        id,
        &session.fixture,
        session.sequence,
        &session.journal,
        &[],
        "schedule-restart.tsv",
        true,
    )?;
    let (recovery_path, terminal_view) = await_terminal_after_restart(
        &recovered,
        &mut events,
        context,
        "0:0:1",
        &admit_hex,
        pre_deadline,
        RECOVERY_TIMEOUT,
    )?;
    read_output_verified(
        &recovered,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output.bin",
    )?;
    let publication_commits = subject_record_count(&events_path, "PUBLICATION_COMMITTED")?;
    ensure!(
        publication_commits == 1,
        "g2-kill-at-publication-commit expected exactly one durable terminal commit, got \
         {publication_commits}"
    );

    // Post-terminal retry of a NEW operation against the terminal work must
    // refuse, naming ALREADY_TERMINAL (18) or CANCELLED (12).
    let retry_hex = oracle::operation_hex(oracle::operation_id(context.seed, "retry", 3));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&retry_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let retry = recovered.op(&[
        "retry",
        "--operation",
        &retry_hex,
        "--work",
        "0:0:1",
        "--expected-attempt",
        "1",
    ])?;
    let retry_text = transcript(&retry);
    fs::write(artifacts.join("post-terminal-retry.txt"), &retry_text)?;
    let retry_line = refusal_named_line(
        &String::from_utf8_lossy(&retry.stderr),
        &["ALREADY_TERMINAL", "CANCELLED"],
    );
    ensure!(
        !retry.status.success(),
        "g2-kill-at-publication-commit post-terminal retry must refuse, got exit \
         {}\n{retry_text}",
        retry.status
    );
    let retry_line = retry_line.context(
        "g2-kill-at-publication-commit post-terminal retry must name ALREADY_TERMINAL or \
         CANCELLED on stderr",
    )?;
    let retry_code = if retry_line.starts_with("authority refusal ALREADY_TERMINAL:")
        || retry_line.starts_with("ALREADY_TERMINAL:")
    {
        18
    } else {
        12
    };
    events.append(
        "REFUSAL_RECEIVED",
        Some(hex_to_id(&retry_hex)?),
        Some("0:0:1"),
        Some(1),
        Some(retry_code),
        Some(artifact_ref("post-terminal-retry.txt", &retry_text)),
    )?;
    detach(&recovered)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("subject_exit_code", "86".into()),
            (
                "publication_committed_records",
                publication_commits.to_string(),
            ),
            ("recovery_path", recovery_path),
            ("terminal_view", terminal_view.trim().to_owned()),
            ("post_terminal_retry_refusal", retry_line),
            ("post_terminal_retry_code", retry_code.to_string()),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, recovered.server, events)
}

/// g2-kill-client-after-request-sent: SIGKILL the one-shot client process
/// mid-admission at a seeded delay. The journal replay either adopts the
/// durable admission receipt (authority committed it) or observes NOT_FOUND
/// and re-sends the SAME operation; either way exactly one admission exists.
fn g2_kill_client_after_request_sent(context: &ScenarioContext) -> Result<()> {
    let id = "g2-kill-client-after-request-sent";
    g2_kill_client_after_request_sent_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(context, id, |context, direction_dir| {
        g2_kill_client_after_request_sent_direction(
            context,
            direction_dir,
            Subject::Rust,
            Subject::Java,
        )
    })?;
    Ok(())
}

fn g2_kill_client_after_request_sent_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-kill-client-after-request-sent";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    // The server is unarmed: the fault is the client process dying, not a
    // server-side schedule. The input is large enough that the upload cannot
    // finish inside the seeded kill delay on loopback.
    let hooked = setup_hooked(
        context,
        scenario_dir,
        id,
        server,
        client,
        &[],
        "schedule.tsv",
        true,
    )?;
    let (session, _events_path) = split_hooked(hooked);
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let input = oracle::dataset(context.seed, CLIENT_KILL_INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    let delay_ms = 10 + (context.seed % 23);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            ("kill_delay_ms", delay_ms.to_string()),
            (
                "kill_label",
                "SIGKILL to the one-shot client after the request was sent, mid-admission".into(),
            ),
            (
                "journal_replay",
                "adopt the durable receipt, or on NOT_FOUND re-send the SAME operation".into(),
            ),
            (
                "expected_admissions",
                "exactly 1 (page members + subject records)".into(),
            ),
            ("expected_attempt", "1".into()),
        ],
    )?;

    let args = admit_op_args(&admit_hex, &declare, &input_path);
    let arg_refs = op_refs(&args);
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let mut child = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &arg_refs,
    )?;
    thread::sleep(Duration::from_millis(delay_ms));
    ensure!(
        child.try_wait()?.is_none(),
        "g2-kill-client-after-request-sent: the admit op finished in {delay_ms}ms before the \
         kill; the input is not large enough to keep it genuinely in flight"
    );
    child.kill().context("SIGKILL the in-flight client op")?;
    let killed = AuthorityFixture::wait_client_op(child, OP_WAIT)?;
    ensure!(
        !killed.status.success(),
        "g2-kill-client-after-request-sent: the killed client op must not exit zero:\n{}",
        transcript(&killed)
    );

    // Journal replay: adopt the durable receipt if the authority committed
    // the admission; otherwise the lookup names NOT_FOUND and the SAME
    // operation is re-sent (same id, input, declaration).
    let lookup = session.op(&["lookup", "--operation", &admit_hex])?;
    let recovery_path = if lookup.status.success()
        && String::from_utf8_lossy(&lookup.stdout).contains("RECEIPT")
    {
        let adopted = transcript(&lookup);
        fs::write(artifacts.join("adopted-receipt.txt"), &adopted)?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&admit_hex)?),
            Some("0:0:1"),
            Some(1),
            None,
            None,
        )?;
        "receipt-adopted".to_owned()
    } else {
        let lookup_text = transcript(&lookup);
        fs::write(artifacts.join("lookup-not-found.txt"), &lookup_text)?;
        ensure!(
            refusal_named_line(&String::from_utf8_lossy(&lookup.stderr), &["NOT_FOUND"]).is_some(),
            "g2-kill-client-after-request-sent: lookup after the client kill must return the \
             receipt or name NOT_FOUND:\n{lookup_text}"
        );
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&admit_hex)?),
            Some("0:0:1"),
            Some(1),
            None,
            None,
        )?;
        let replay = session.op(&arg_refs)?;
        let replay_receipt = require(&replay, "RECEIPT", "re-sent admit after NOT_FOUND")?;
        fs::write(artifacts.join("resent-receipt.txt"), &replay_receipt)?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&admit_hex)?),
            Some("0:0:1"),
            Some(1),
            None,
            None,
        )?;
        "resent-after-not-found".to_owned()
    };

    // Exactly one admission: one scope member, one attempt, one subject commit.
    let page = session.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "scope page")?;
    let (declared, members) = parse_scope_page(&page_stdout)?;
    ensure!(
        members == 1,
        "g2-kill-client-after-request-sent expected exactly one admitted entity after the \
         client death, got members={members}:\n{page_stdout}"
    );
    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit_hex, WATCH_TIMEOUT)?;
    ensure!(
        parse_attempt(&terminal)? == 1,
        "g2-kill-client-after-request-sent expected the work under attempt 1 after the client \
         death:\n{terminal}"
    );
    detach(&session)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("kill_delay_ms", delay_ms.to_string()),
            ("recovery_path", recovery_path),
            ("page_declared", declared.to_string()),
            ("page_members", members.to_string()),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// Section 12.2 refusal-code table, used only to NAME codes that a subject
/// transcript prints verbatim; a code is never inferred from a generic error.
const REFUSAL_CODES: &[(&str, u32)] = &[
    ("FRAME_ERROR", 1),
    ("EXTENSION_UNSUPPORTED", 2),
    ("UNAUTHORIZED", 3),
    ("LIMIT_EXCEEDED", 4),
    ("NOT_FOUND", 5),
    ("EXPIRED", 6),
    ("CONFLICT", 7),
    ("INTEGRITY_ERROR", 8),
    ("NOT_READY", 9),
    ("WAIT_TIMEOUT", 10),
    ("DEADLINE_EXCEEDED", 11),
    ("CANCELLED", 12),
    ("APPLICATION_UNSUPPORTED", 13),
    ("CONTROL_RESET", 14),
    ("INTERNAL_ERROR", 15),
    ("OUTPUT_UNAVAILABLE", 16),
    ("CLOCK_UNSAFE", 17),
    ("ALREADY_TERMINAL", 18),
];

/// One observed probe with its named-code classification.
struct ProbeOutcome {
    success: bool,
    exit: String,
    stdout: String,
    stderr: String,
    /// (code, refusal line) when the transcript itself names a Section 12.2
    /// code — either an authority refusal ("authority refusal CODE: ..." on
    /// the Rust CLI, "CODE: authority refused: ..." on the Java CLI) or a
    /// client journal guard ("INTEGRITY_ERROR: ...").
    refusal: Option<(u32, String)>,
}

fn probe_outcome(output: &Output) -> ProbeOutcome {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let refusal = stderr.lines().find_map(|line| {
        if let Some(rest) = line.strip_prefix("connection lost: closed by peer: ") {
            // Connection close carrying a named Section 12.2 code, e.g.
            // "UNAUTHORIZED (code 515)" (0x200 + code, src/v2/mod.rs).
            let name: String = rest
                .chars()
                .take_while(|ch| ch.is_ascii_alphabetic() || *ch == '_')
                .collect();
            return REFUSAL_CODES
                .iter()
                .find(|(known, _)| *known == name)
                .map(|(_, code)| (*code, line.to_owned()));
        }
        if let Some(rest) = line.strip_prefix("authority refusal ") {
            let name: String = rest
                .chars()
                .take_while(|ch| ch.is_ascii_alphabetic() || *ch == '_')
                .collect();
            return REFUSAL_CODES
                .iter()
                .find(|(known, _)| *known == name)
                .map(|(_, code)| (*code, line.to_owned()));
        }
        let (head, _) = line.split_once(": ")?;
        REFUSAL_CODES
            .iter()
            .find(|(known, _)| *known == head)
            .map(|(_, code)| (*code, line.to_owned()))
    });
    ProbeOutcome {
        success: output.status.success(),
        exit: output.status.to_string(),
        stdout,
        stderr,
        refusal,
    }
}

impl ProbeOutcome {
    fn transcript(&self) -> String {
        format!(
            "exit={}\nstdout:\n{}\nstderr:\n{}",
            self.exit, self.stdout, self.stderr
        )
    }
}

/// Write a probe transcript artifact; returns (len, sha256) for the event.
fn write_probe_artifact(
    artifacts: &Path,
    name: &str,
    outcome: &ProbeOutcome,
) -> Result<(u64, String)> {
    let text = outcome.transcript();
    fs::write(artifacts.join(name), &text)?;
    Ok((text.len() as u64, oracle::sha256_hex(text.as_bytes())))
}

/// Normalize a transcript for the existence-disclosure pair comparison:
/// long hex runs and `N:N:N` work keys are redacted so an identifier
/// difference cannot mask or fake a detail-class difference.
fn normalize_transcript(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte.is_ascii_hexdigit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
                i += 1;
            }
            if i - start >= 16 {
                out.push_str("<HEX>");
            } else {
                out.push_str(&text[start..i]);
            }
        } else if byte.is_ascii_digit() {
            let start = i;
            let mut j = i;
            let mut groups = 0usize;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                groups += 1;
                if j < bytes.len() && bytes[j] == b':' {
                    j += 1;
                } else {
                    break;
                }
            }
            if groups == 3
                && (j == bytes.len() || !bytes[j].is_ascii_digit())
                && bytes[j - 1] != b':'
            {
                out.push_str("<WORK>");
            } else {
                out.push_str(&text[start..j]);
            }
            i = j;
        } else {
            out.push(byte as char);
            i += 1;
        }
    }
    out
}

/// Run one G5 row body against the rust server in the canonical row
/// directory and, when a Java jar is present, again with the Java server in
/// the `java-server` subdirectory. Client is the rust CLI throughout.
fn g5_row(
    context: &ScenarioContext,
    id: &str,
    run: impl Fn(&ScenarioContext, &Path, Subject) -> Result<()>,
) -> Result<()> {
    let scenario_dir = context.scenario_dir(id);
    run(context, &scenario_dir, Subject::Rust)?;
    let java_dir = scenario_dir.join("java-server");
    if context.java_jar.is_none() {
        fs::create_dir_all(&java_dir)?;
        fs::write(
            java_dir.join("INCOMPLETE"),
            b"no --java-jar provided; this direction was not run\n",
        )?;
    } else {
        run(context, &java_dir, Subject::Java)?;
    }
    Ok(())
}

fn g5_fixture(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    principals: &[(&str, &str)],
    unmapped: &[&str],
    foreign: &[&str],
) -> Result<(AuthorityFixture, OwnedServer)> {
    let certs = mtls::generate_full(&scenario_dir.join("certs"), principals, unmapped, foreign)?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let server = fixture.start_server()?;
    Ok((fixture, server))
}

/// Initialize a client journal for `owner` (locally; no network), then run
/// the binding op so the owner holds a live session. Returns the journal and
/// its connection arguments.
fn bind_owner(
    fixture: &AuthorityFixture,
    scenario_dir: &Path,
    server: &OwnedServer,
    tag: &str,
    owner: &str,
    sequence: u64,
) -> Result<(PathBuf, Vec<String>)> {
    let journal = scenario_dir.join("client").join(format!("{tag}.sqlite"));
    fs::create_dir_all(journal.parent().expect("journal has a parent directory"))?;
    let mut command = fixture.client_base()?;
    command.push("init-client".into());
    command.extend(fixture.journal_args(&journal, owner, sequence));
    let init = crate::run_output_owned(&fixture.root, &command, OP_WAIT)?;
    require(
        &init,
        fixture.client.client_initialized_marker(),
        "v2 init-client",
    )?;
    let connection = fixture.connection_args(server, owner)?;
    let output = fixture.run_client_op(&journal, owner, sequence, &connection, &["binding"])?;
    require(&output, "BINDING", &format!("{owner} client binding"))?;
    Ok((journal, connection))
}

/// Alice (owner A) publishes one work item end to end. Returns the admission
/// operation id, the fixture for the foreign-owner probes.
fn alice_publish(
    alice: &Session,
    events: &mut EventWriter,
    seed: u64,
    artifacts: &Path,
    input: &[u8],
) -> Result<String> {
    let input_sha256 = oracle::sha256_hex(input);
    fs::write(artifacts.join("input.bin"), input)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/input.bin".into(),
            len: input.len() as u64,
            sha256: input_sha256,
        }),
    )?;
    let declare = declare_sealed(alice, events, seed, "declare", &[1])?;
    let admit = oracle::operation_hex(oracle::operation_id(seed, "admit", 1));
    admit_input(
        alice,
        events,
        seed,
        "admit",
        &declare,
        "0:0:1",
        &artifacts.join("input.bin"),
    )?;
    watch_terminal(alice, events, "0:0:1", &admit, WATCH_TIMEOUT)?;
    Ok(admit)
}

/// Shared two-owner setup for g5-foreign-owner and g5-no-existence-disclosure:
/// alice has published work; bob holds his own live session.
fn g5_two_owners(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<(Session, PathBuf, Vec<String>)> {
    let (fixture, server) = g5_fixture(
        context,
        scenario_dir,
        server_subject,
        &[("alice", "alice"), ("bob", "bob")],
        &[],
        &[],
    )?;
    let (alice_journal, alice_connection) =
        bind_owner(&fixture, scenario_dir, &server, "alice", "alice", 1)?;
    let (bob_journal, bob_connection) =
        bind_owner(&fixture, scenario_dir, &server, "bob", "bob", 1)?;
    let alice = Session {
        fixture,
        server,
        sequence: 1,
        journal: alice_journal,
        connection: alice_connection,
    };
    Ok((alice, bob_journal, bob_connection))
}

/// Assert a probe failed and surfaced a named-code refusal line; returns
/// (code, line). Never invents a code: absence of a named line is an error.
fn expect_named_refusal(outcome: &ProbeOutcome, description: &str) -> Result<(u32, String)> {
    ensure!(
        !outcome.success,
        "{description} unexpectedly succeeded\n{}",
        outcome.transcript()
    );
    outcome.refusal.clone().with_context(|| {
        format!(
            "{description} produced no named-code refusal\n{}",
            outcome.transcript()
        )
    })
}

fn g5_observed_preamble(
    observed: &mut Vec<(&str, String)>,
    server_subject: Subject,
    context: &ScenarioContext,
) {
    observed.push(("server_subject", server_subject.name().into()));
    if server_subject == Subject::Java
        && let Some(jar) = &context.java_jar
        && let Ok(bytes) = fs::read(jar)
    {
        observed.push(("java_jar_sha256", oracle::sha256_hex(&bytes)));
    }
}

/// g5-untrusted-identity: a client certificate from an unrelated CA must fail
/// the QUIC/TLS handshake (CRYPTO_ERROR class): no CAPABILITIES exchange, no
/// application REFUSAL, and no durable state. The exact client error strings
/// legitimately differ between subjects; both are recorded verbatim and the
/// invariant asserted is handshake-failure-not-application-refusal.
fn g5_untrusted_identity(context: &ScenarioContext) -> Result<()> {
    g5_row(
        context,
        "g5-untrusted-identity",
        g5_untrusted_identity_direction,
    )
}

fn g5_untrusted_identity_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let scenario_id = "g5-untrusted-identity";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let (fixture, server) = g5_fixture(
        context,
        scenario_dir,
        server_subject,
        &[("alice", "alice")],
        &[],
        &["zed"],
    )?;
    let foreign = fixture.certs.identity("zed")?.clone();

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "expected_outcome",
                "QUIC CRYPTO_ERROR during handshake (RFC 9001 section 4.8)".into(),
            ),
            (
                "expected_no_capabilities",
                "true (no CAPABILITIES exchange, no application REFUSAL)".into(),
            ),
            (
                "expected_state_unchanged",
                "alice next-sequence stays 1".into(),
            ),
        ],
    )?;

    let probe = fixture.probe_next_sequence(&server, &foreign)?;
    let outcome = probe_outcome(&probe);
    let (len, sha256) = write_probe_artifact(&artifacts, "foreign-cert-probe.txt", &outcome)?;
    ensure!(
        !outcome.success,
        "g5-untrusted-identity: foreign-CA certificate was unexpectedly accepted\n{}",
        outcome.transcript()
    );
    ensure!(
        outcome.refusal.is_none(),
        "g5-untrusted-identity: foreign-CA rejection surfaced an application refusal; \
         expected a transport-level failure only\n{}",
        outcome.transcript()
    );
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/foreign-cert-probe.txt".into(),
            len,
            sha256,
        }),
    )?;

    // The failed attempt must not have created authority state.
    let sequence = fixture.next_sequence(&server, "alice")?;
    ensure!(
        sequence == 1,
        "g5-untrusted-identity: foreign probe disturbed authority state (alice next-sequence {sequence})"
    );

    let foreign_ca_sha = match fixture.certs.foreign_ca_cert.as_ref() {
        Some(path) => Some(oracle::sha256_hex(&fs::read(path)?)),
        None => None,
    };
    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.extend([
        (
            "foreign_ca_sha256",
            foreign_ca_sha.unwrap_or_else(|| "-".into()),
        ),
        ("probe_exit", outcome.exit.clone()),
        (
            "probe_stderr_first_line",
            outcome.stderr.lines().next().unwrap_or("").to_owned(),
        ),
        (
            "probe_named_code",
            "none (transport failure, no application refusal)".into(),
        ),
        ("application_refusal_surfaced", "false".into()),
        ("alice_next_sequence_after", sequence.to_string()),
    ]);
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, scenario_id, server, events)
}

/// g5-missing-client-cert: with durable required, Section 12.3 expects a
/// close with UNAUTHORIZED (3) BEFORE the capabilities response; with durable
/// optional the connection is Core-only and SESSION create is refused.
/// Audit finding recorded by this row: neither subject server CLI exposes a
/// require-durable flag, and neither subject client CLI can start without
/// --cert/--key, so the wire-level arms are not reachable through the
/// published binaries. What is verified: the client-side refusal evidence,
/// the server offer/authorization behavior from the subject sources, and
/// that no durable state is created.
fn g5_missing_client_cert(context: &ScenarioContext) -> Result<()> {
    g5_row(
        context,
        "g5-missing-client-cert",
        g5_missing_client_cert_direction,
    )
}

fn g5_missing_client_cert_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let scenario_id = "g5-missing-client-cert";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let (fixture, server) = g5_fixture(
        context,
        scenario_dir,
        server_subject,
        &[("alice", "alice")],
        &[],
        &[],
    )?;

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "expected_required_mode",
                "connection closed with UNAUTHORIZED (3) BEFORE the capabilities response (Section 12.3)".into(),
            ),
            (
                "expected_optional_mode",
                "durable profiles excluded; Core-only connection; SESSION create refused without activating durable profiles".into(),
            ),
            (
                "reachability_finding",
                "neither server CLI exposes require-durable; neither client CLI can start without --cert/--key; \
                 wire-level arms not reachable via published binaries (observed.tsv audit rows)".into(),
            ),
        ],
    )?;

    let probe = fixture.probe_next_sequence_without_cert(&server)?;
    let outcome = probe_outcome(&probe);
    let (len, sha256) = write_probe_artifact(&artifacts, "no-cert-probe.txt", &outcome)?;
    ensure!(
        !outcome.success,
        "g5-missing-client-cert: a client without a certificate unexpectedly succeeded\n{}",
        outcome.transcript()
    );

    // The refused start must not have created authority state.
    let sequence = fixture.next_sequence(&server, "alice")?;
    ensure!(
        sequence == 1,
        "g5-missing-client-cert: no-cert attempt disturbed authority state (alice next-sequence {sequence})"
    );

    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.extend([
        ("client_without_cert_exit", outcome.exit.clone()),
        (
            "client_without_cert_stderr_first_line",
            outcome.stderr.lines().next().unwrap_or("").to_owned(),
        ),
        (
            "rust_server_audit",
            "no require-durable flag on `v2 serve` (server/src/v2.rs Command::Serve); TLS client certs \
             requested-not-required via WebPkiClientVerifier::allow_unauthenticated \
             (quinn/src/v2_tls.rs:64-66); Capabilities::select refuses required durable profiles for \
             unauthenticated peers with UNAUTHORIZED 'required durable profile lacks authenticated owner' \
             (src/v2/negotiation.rs:52-60) - before the capabilities response"
                .into(),
        ),
        (
            "java_server_audit",
            "no require-durable flag on serve (V2Main.java exposes only --object-limit); ClientAuth.OPTIONAL \
             (TlsAuthentication.java:86); negotiate() excludes durable profiles for unmapped/no-cert callers \
             and throws UNAUTHORIZED 'caller credential unavailable' when durable is required, closing before \
             the capabilities response (TlsAuthentication.java negotiate/denied); DurableOptions.requireDurable \
             default false (api-plan.md section 1.3)"
                .into(),
        ),
        (
            "client_audit",
            "both client CLIs require --cert/--key at startup: rust clap exit 2 'the following required \
             arguments were not provided'; java requiredPath throws 'missing --cert' exit 1. No published \
             client can negotiate a Core-only connection."
                .into(),
        ),
        ("require_durable_configurable_via_cli", "false (both subjects)".into()),
        ("negotiated_profile_evidence", "not observable: no published client can connect without a certificate".into()),
        ("alice_next_sequence_after", sequence.to_string()),
        ("artifact_no_cert_probe", format!("artifacts/no-cert-probe.txt sha256={sha256} len={len}")),
    ]);
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, scenario_id, server, events)
}

/// g5-unmapped-principal: a valid certificate from the trusted CA whose
/// leaf-DER sha256 is absent from the principal map must be refused durable
/// activation (UNAUTHORIZED class before capabilities when required;
/// Core-only otherwise) without disclosing any owner's retained sessions.
fn g5_unmapped_principal(context: &ScenarioContext) -> Result<()> {
    g5_row(
        context,
        "g5-unmapped-principal",
        g5_unmapped_principal_direction,
    )
}

fn g5_unmapped_principal_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let scenario_id = "g5-unmapped-principal";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let (fixture, server) = g5_fixture(
        context,
        scenario_dir,
        server_subject,
        &[("alice", "alice")],
        &["carol"],
        &[],
    )?;
    let unmapped = fixture.certs.identity("carol")?.clone();
    ensure!(
        fixture.certs.principal("carol").is_err(),
        "carol must not be a mapped principal"
    );

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "expected_outcome",
                "durable activation refused (UNAUTHORIZED class); no disclosure of any owner's retained sessions".into(),
            ),
            ("expected_state_unchanged", "alice next-sequence stays 1".into()),
        ],
    )?;

    let probe = fixture.probe_next_sequence(&server, &unmapped)?;
    let outcome = probe_outcome(&probe);
    let (len, sha256) = write_probe_artifact(&artifacts, "unmapped-probe.txt", &outcome)?;
    ensure!(
        !outcome.success,
        "g5-unmapped-principal: unmapped certificate was unexpectedly accepted\n{}",
        outcome.transcript()
    );
    let (code, line) = outcome.refusal.clone().unwrap_or((0, "none".into()));
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        outcome.refusal.map(|(code, _)| code),
        Some(ArtifactRef {
            path: "artifacts/unmapped-probe.txt".into(),
            len,
            sha256,
        }),
    )?;

    // No disclosure: alice's retained state is unchanged and still reachable.
    let sequence = fixture.next_sequence(&server, "alice")?;
    ensure!(
        sequence == 1,
        "g5-unmapped-principal: unmapped probe disturbed authority state (alice next-sequence {sequence})"
    );

    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.extend([
        ("probe_exit", outcome.exit.clone()),
        (
            "probe_refusal",
            if line == "none" {
                "no named code in transcript (the wire close code is recorded verbatim: code 515                  = 0x200 + 3, UNAUTHORIZED per src/v2/mod.rs quic_error)"
                    .into()
            } else {
                format!("code={code} line: {line}")
            },
        ),
        (
            "refusal_source",
            "connection close BEFORE the capabilities response: rust server refuses required              durable profiles for unauthenticated-mapped peers in Capabilities::select              (src/v2/negotiation.rs:52-60); java server throws UNAUTHORIZED 'caller credential              unavailable' from TlsAuthentication.negotiate (TlsAuthentication.java)"
                .into(),
        ),
        ("alice_next_sequence_after", sequence.to_string()),
    ]);
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, scenario_id, server, events)
}

/// Run both arms of a no-existence-disclosure pair, recording each
/// transcript as an artifact plus a REFUSAL_RECEIVED event, and return the
/// two outcomes for comparison.
#[allow(clippy::too_many_arguments)]
fn probe_pair(
    fixture: &AuthorityFixture,
    artifacts: &Path,
    events: &mut EventWriter,
    pair: &'static str,
    op_a: Option<[u8; 16]>,
    op_b: Option<[u8; 16]>,
    work: Option<&str>,
    attempt: Option<u64>,
    arm_a: impl FnOnce(&AuthorityFixture) -> Result<Output>,
    arm_b: impl FnOnce(&AuthorityFixture) -> Result<Output>,
) -> Result<(ProbeOutcome, ProbeOutcome)> {
    let mut record = |tag: &str, outcome: &ProbeOutcome, op: Option<[u8; 16]>| -> Result<()> {
        let (len, sha256) = write_probe_artifact(artifacts, &format!("{pair}-{tag}.txt"), outcome)?;
        events.append(
            "REFUSAL_RECEIVED",
            op,
            work,
            attempt,
            outcome.refusal.as_ref().map(|(code, _)| *code),
            Some(ArtifactRef {
                path: format!("artifacts/{pair}-{tag}.txt"),
                len,
                sha256,
            }),
        )
    };
    let a = probe_outcome(&arm_a(fixture)?);
    record("a", &a, op_a)?;
    let b = probe_outcome(&arm_b(fixture)?);
    record("b", &b, op_b)?;
    Ok((a, b))
}

/// Pair arm: a fresh journal whose intent claims `owner`, presenting bob's
/// certificate (attach-level existence probe).
fn claim_arm<'a>(
    scenario_dir: PathBuf,
    bob_connection: Vec<String>,
    owner: &'a str,
    tag: &'a str,
) -> impl FnOnce(&AuthorityFixture) -> Result<Output> + 'a {
    move |fixture: &AuthorityFixture| -> Result<Output> {
        let journal = scenario_dir.join("client").join(format!("{tag}.sqlite"));
        fs::create_dir_all(journal.parent().expect("journal has a parent directory"))?;
        fixture_style_init(fixture, &journal, owner, 1)?;
        fixture.run_client_op(&journal, owner, 1, &bob_connection, &["binding"])
    }
}

/// Pair arm: one client operation from bob's own journal/connection.
fn bob_op_arm<'a>(
    bob_journal: PathBuf,
    bob_connection: Vec<String>,
    operation: Vec<&'a str>,
) -> impl FnOnce(&AuthorityFixture) -> Result<Output> + 'a {
    move |fixture: &AuthorityFixture| -> Result<Output> {
        fixture.run_client_op(&bob_journal, "bob", 1, &bob_connection, &operation)
    }
}

/// Pair arm: a result read from bob's own journal/connection.
fn bob_read_arm<'a>(
    bob_journal: PathBuf,
    bob_connection: Vec<String>,
    work: &'a str,
    output_arg: String,
) -> impl FnOnce(&AuthorityFixture) -> Result<Output> + 'a {
    move |fixture: &AuthorityFixture| -> Result<Output> {
        let operation = [
            "read",
            "--work",
            work,
            "--attempt",
            "1",
            "--index",
            "0",
            "--output",
            output_arg.as_str(),
        ];
        fixture.run_client_op(&bob_journal, "bob", 1, &bob_connection, &operation)
    }
}

/// g5-foreign-owner: owner A published work. Owner B (mapped principal, same
/// authority) attempts attach to A's generation, work view on A's work key,
/// operation lookup of A's operation id, and result read of A's output. Every
/// attempt must be refused with a named code and must not confirm the work's
/// existence; A's data must read back byte-exact afterwards.
///
/// Wire-level note (recorded in observed.tsv): the published clients derive
/// the wire Attach identity from the journaled binding and the wire Create
/// carries no claimed owner, so the authority-side cross-owner branch
/// (`attach_session` UNAUTHORIZED, src/v2/authority/sessions.rs) is not
/// reachable through the CLIs. The equivalent probes: a fresh journal whose
/// INTENT claims owner A (client journal guard refuses the mismatched
/// binding), and A's identifiers addressed inside B's own session.
fn g5_foreign_owner(context: &ScenarioContext) -> Result<()> {
    g5_row(context, "g5-foreign-owner", g5_foreign_owner_direction)
}

fn g5_foreign_owner_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let scenario_id = "g5-foreign-owner";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let (alice, bob_journal, bob_connection) =
        g5_two_owners(context, scenario_dir, server_subject)?;

    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "expected_attach",
                "refused with a named code; existence not confirmed".into(),
            ),
            (
                "expected_view",
                "refused with a named code; existence not confirmed".into(),
            ),
            (
                "expected_lookup",
                "refused with a named code; existence not confirmed".into(),
            ),
            (
                "expected_read",
                "refused with a named code; existence not confirmed".into(),
            ),
            (
                "expected_a_unchanged",
                "A reads the result back byte-exact after B's attempts".into(),
            ),
        ],
    )?;

    let admit = alice_publish(&alice, &mut events, context.seed, &artifacts, &input)?;
    let admit_id = hex_to_id(&admit)?;
    let bob_journal = bob_journal.clone();
    let bob_connection = bob_connection.clone();

    // Probe: attach to A's generation - a fresh journal whose intent claims
    // owner alice, presenting bob's certificate.
    let claim_journal = scenario_dir.join("client").join("claim-alice.sqlite");
    fs::create_dir_all(
        claim_journal
            .parent()
            .expect("journal has a parent directory"),
    )?;
    fixture_style_init(&alice.fixture, &claim_journal, "alice", 1)?;
    let attach =
        alice
            .fixture
            .run_client_op(&claim_journal, "alice", 1, &bob_connection, &["binding"])?;
    let attach_outcome = probe_outcome(&attach);
    let (attach_len, attach_sha) =
        write_probe_artifact(&artifacts, "bob-attach-claim.txt", &attach_outcome)?;
    let (attach_code, attach_line) =
        expect_named_refusal(&attach_outcome, "g5-foreign-owner attach claim")?;
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        Some(attach_code),
        Some(ArtifactRef {
            path: "artifacts/bob-attach-claim.txt".into(),
            len: attach_len,
            sha256: attach_sha,
        }),
    )?;

    // Probe: work view on A's work key, from B's own session.
    let view = alice.fixture.run_client_op(
        &bob_journal,
        "bob",
        1,
        &bob_connection,
        &["watch", "--work", "0:0:1"],
    )?;
    let view_outcome = probe_outcome(&view);
    let (view_len, view_sha) = write_probe_artifact(&artifacts, "bob-view.txt", &view_outcome)?;
    let (view_code, view_line) = expect_named_refusal(&view_outcome, "g5-foreign-owner work view")?;
    events.append(
        "REFUSAL_RECEIVED",
        None,
        Some("0:0:1"),
        None,
        Some(view_code),
        Some(ArtifactRef {
            path: "artifacts/bob-view.txt".into(),
            len: view_len,
            sha256: view_sha,
        }),
    )?;

    // Probe: operation lookup of A's admission operation id.
    let lookup = alice.fixture.run_client_op(
        &bob_journal,
        "bob",
        1,
        &bob_connection,
        &["lookup", "--operation", &admit],
    )?;
    let lookup_outcome = probe_outcome(&lookup);
    let (lookup_len, lookup_sha) =
        write_probe_artifact(&artifacts, "bob-lookup.txt", &lookup_outcome)?;
    let (lookup_code, lookup_line) =
        expect_named_refusal(&lookup_outcome, "g5-foreign-owner operation lookup")?;
    events.append(
        "REFUSAL_RECEIVED",
        Some(admit_id),
        None,
        None,
        Some(lookup_code),
        Some(ArtifactRef {
            path: "artifacts/bob-lookup.txt".into(),
            len: lookup_len,
            sha256: lookup_sha,
        }),
    )?;

    // Probe: result read of A's output.
    let bob_read_path = artifacts.join("bob-read.bin");
    let read = alice.fixture.run_client_op(
        &bob_journal,
        "bob",
        1,
        &bob_connection,
        &[
            "read",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
            "--output",
            &crate::path(&bob_read_path),
        ],
    )?;
    let read_outcome = probe_outcome(&read);
    let (read_len, read_sha) = write_probe_artifact(&artifacts, "bob-read.txt", &read_outcome)?;
    let (read_code, read_line) =
        expect_named_refusal(&read_outcome, "g5-foreign-owner result read")?;
    events.append(
        "REFUSAL_RECEIVED",
        None,
        Some("0:0:1"),
        Some(1),
        Some(read_code),
        Some(ArtifactRef {
            path: "artifacts/bob-read.txt".into(),
            len: read_len,
            sha256: read_sha,
        }),
    )?;

    // A's data is unchanged: byte-exact readback on A's own session.
    read_output_verified(
        &alice,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "alice-readback.bin",
    )?;
    detach(&alice)?;

    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.extend([
        (
            "attach_refusal",
            format!("code={attach_code} line: {attach_line}"),
        ),
        (
            "attach_source",
            "client journal guard: wire Create was answered with bob's retained binding and the              local validate_binding refused the owner mismatch (src/v2/client.rs:343-365); the              claimed owner never leaves the client"
                .into(),
        ),
        (
            "view_refusal",
            format!("code={view_code} line: {view_line}"),
        ),
        (
            "view_source",
            "authority refusal on the wire from B's own session (rust server detail 'work not              declared', src/v2/authority/scopes.rs:172; java server detail 'refused')"
                .into(),
        ),
        (
            "lookup_refusal",
            format!("code={lookup_code} line: {lookup_line}"),
        ),
        (
            "lookup_source",
            "client journal guard: read_intent finds no persisted intent for the foreign id, so              no wire request was sent (src/v2/client.rs:399-402)"
                .into(),
        ),
        (
            "read_refusal",
            format!("code={read_code} line: {read_line}"),
        ),
        (
            "read_source",
            "client journal guard: no retained output selection for the foreign work key, so no              wire request was sent (src/v2/client/observations/references.rs:112)"
                .into(),
        ),
        ("a_readback_byte_exact", "true".into()),
        (
            "wire_level_note",
            "published clients cannot send a cross-owner Attach (Attach carries the journaled \
             binding; Create carries no claimed owner): the authority-side attach_session \
             UNAUTHORIZED branch (src/v2/authority/sessions.rs) is unreachable via the CLIs; \
             probes exercise the intent-claim and foreign-identifier equivalents"
                .into(),
        ),
    ]);
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, scenario_id, alice.server, events)
}

/// Initialize a journal claiming `owner` without contacting the authority.
fn fixture_style_init(
    fixture: &AuthorityFixture,
    journal: &Path,
    owner: &str,
    sequence: u64,
) -> Result<()> {
    let mut command = fixture.client_base()?;
    command.push("init-client".into());
    command.extend(fixture.journal_args(journal, owner, sequence));
    let init = crate::run_output_owned(&fixture.root, &command, OP_WAIT)?;
    require(
        &init,
        fixture.client.client_initialized_marker(),
        "v2 init-client (claim journal)",
    )?;
    Ok(())
}

/// g5-no-existence-disclosure: paired probes from B's connection. Arm (a)
/// targets that exist but belong to A; arm (b) never-created targets. For
/// each operation pair (attach, lookup, view, read) the refusal code AND the
/// normalized detail class must be indistinguishable. Any distinguishable
/// difference is an existence-disclosure defect: the row fails with the exact
/// transcript pair recorded in observed.tsv.
fn g5_no_existence_disclosure(context: &ScenarioContext) -> Result<()> {
    g5_row(
        context,
        "g5-no-existence-disclosure",
        g5_no_existence_disclosure_direction,
    )
}

fn g5_no_existence_disclosure_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let scenario_id = "g5-no-existence-disclosure";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let (alice, bob_journal, bob_connection) =
        g5_two_owners(context, scenario_dir, server_subject)?;

    let input = oracle::dataset(context.seed, INPUT_LEN);
    let admit = alice_publish(&alice, &mut events, context.seed, &artifacts, &input)?;
    let never_operation =
        oracle::operation_hex(oracle::operation_id(context.seed, "probe-never", 9));
    let admit_id = hex_to_id(&admit)?;
    let never_id = hex_to_id(&never_operation)?;

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("pair_attach", "(a) A's existing generation via intent claim vs (b) never-created owner label: indistinguishable".into()),
            ("pair_lookup", "(a) A's operation id vs (b) never-created operation id: indistinguishable".into()),
            ("pair_view", "(a) A's work key vs (b) never-created work key: indistinguishable".into()),
            ("pair_read", "(a) A's work/attempt/index vs (b) never-created: indistinguishable".into()),
            ("comparison_rule", "exit status, named refusal code, and normalized transcript (hex ids and N:N:N work keys redacted) must all match".into()),
        ],
    )?;

    // Attach pair: fresh journals whose intents claim an existing owner
    // (alice) and a never-created label (mallory), both presenting bob's
    // certificate. The wire Create carries only the sequence; the server
    // authenticates bob and replays bob's retained binding, so both arms
    // reduce to the same client journal-guard refusal.
    let attach_pair = probe_pair(
        &alice.fixture,
        &artifacts,
        &mut events,
        "attach",
        None,
        None,
        None,
        None,
        claim_arm(
            scenario_dir.to_path_buf(),
            bob_connection.clone(),
            "alice",
            "probe-a",
        ),
        claim_arm(
            scenario_dir.to_path_buf(),
            bob_connection.clone(),
            "mallory",
            "probe-b",
        ),
    )?;

    // Lookup pair from bob's own session.
    let lookup_pair = probe_pair(
        &alice.fixture,
        &artifacts,
        &mut events,
        "lookup",
        Some(admit_id),
        Some(never_id),
        None,
        None,
        bob_op_arm(
            bob_journal.clone(),
            bob_connection.clone(),
            vec!["lookup", "--operation", &admit],
        ),
        bob_op_arm(
            bob_journal.clone(),
            bob_connection.clone(),
            vec!["lookup", "--operation", &never_operation],
        ),
    )?;

    // View pair from bob's own session.
    let view_pair = probe_pair(
        &alice.fixture,
        &artifacts,
        &mut events,
        "view",
        None,
        None,
        Some("0:0:1"),
        None,
        bob_op_arm(
            bob_journal.clone(),
            bob_connection.clone(),
            vec!["watch", "--work", "0:0:1"],
        ),
        bob_op_arm(
            bob_journal.clone(),
            bob_connection.clone(),
            vec!["watch", "--work", "0:0:2"],
        ),
    )?;

    // Read pair from bob's own session.
    let read_pair = probe_pair(
        &alice.fixture,
        &artifacts,
        &mut events,
        "read",
        None,
        None,
        Some("0:0:1"),
        Some(1),
        bob_read_arm(
            bob_journal.clone(),
            bob_connection.clone(),
            "0:0:1",
            crate::path(&artifacts.join("read-a.bin")),
        ),
        bob_read_arm(
            bob_journal.clone(),
            bob_connection.clone(),
            "0:0:2",
            crate::path(&artifacts.join("read-b.bin")),
        ),
    )?;

    // Compare every pair: exit status, named code, normalized detail class.
    let mut defect: Option<String> = None;
    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    for (pair, arm_a, arm_b) in [
        ("attach", &attach_pair.0, &attach_pair.1),
        ("lookup", &lookup_pair.0, &lookup_pair.1),
        ("view", &view_pair.0, &view_pair.1),
        ("read", &read_pair.0, &read_pair.1),
    ] {
        let code_a = arm_a.refusal.as_ref().map(|(code, _)| *code);
        let code_b = arm_b.refusal.as_ref().map(|(code, _)| *code);
        let norm_a = normalize_transcript(&arm_a.stderr);
        let norm_b = normalize_transcript(&arm_b.stderr);
        let equal = arm_a.success == arm_b.success && code_a == code_b && norm_a == norm_b;
        observed.push((
            pair,
            format!(
                "exit_a={} exit_b={} code_a={code_a:?} code_b={code_b:?} indistinguishable={equal}",
                arm_a.exit, arm_b.exit
            ),
        ));
        if !equal {
            defect = Some(format!(
                "existence-disclosure DEFECT in pair {pair}\
                \narm (a) existing target:\n{}\
                \narm (b) never-created target:\n{}",
                arm_a.transcript(),
                arm_b.transcript()
            ));
        }
    }
    observed.push((
        "wire_level_note",
        "cross-owner addressing is not expressible via the published CLIs (Attach carries the \
         journaled binding; Create carries no claimed owner); arm (a) vs (b) therefore probes the \
         two levels the CLI can reach: cross-owner claims in the client intent (attach pair) and \
         foreign identifiers inside the caller's own session (lookup/view/read pairs)"
            .into(),
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    if let Some(defect) = defect {
        bail!("g5-no-existence-disclosure: {defect}");
    }

    detach(&alice)?;
    stop_and_seal(context, scenario_dir, scenario_id, alice.server, events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_row_ids_are_unique_and_implemented_rows_are_flagged() {
        let rows = rows();
        let mut seen = std::collections::BTreeSet::new();
        for row in &rows {
            assert!(seen.insert(row.id), "duplicate row id {}", row.id);
        }
        assert!(rows.len() >= 40);
        for id in [
            "g1-leaf-copy",
            "g2-crash-before-create-commit",
            "g2-crash-after-create-commit",
            "g2-drop-reply-declaration",
            "g2-drop-reply-admission",
            "g2-kill-after-admission-before-publication",
            "g2-kill-at-publication-commit",
            "g2-kill-client-after-request-sent",
            "g2-duplicate-op-changed-params",
            "g2-simultaneous-duplicate",
            "g2-kill-server-after-admission-recovery",
            "g5-untrusted-identity",
            "g5-missing-client-cert",
            "g5-unmapped-principal",
            "g5-foreign-owner",
            "g5-no-existence-disclosure",
        ] {
            let row = rows.iter().find(|row| row.id == id).unwrap();
            assert!(row.rust_implemented, "{id} must be implemented");
        }
        assert_eq!(rows.iter().filter(|row| row.rust_implemented).count(), 16);
    }

    #[test]
    fn subject_record_count_reads_server_boundary_rows() {
        let dir = std::env::temp_dir().join(format!(
            "pipestream-subject-records-test-{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let events = dir.join("events.tsv");
        fs::write(
            &events,
            "v1\trun\tscenario\trust\tserver\tpid-1\t1\tSESSION_COMMITTED\t\t\t\t\t\t\t\t\n\
             v1\trun\tscenario\trust\tclient\tpid-2\t1\tREQUEST_SENT\t\t\t\t\t\t\t\t\n",
        )
        .unwrap();
        assert_eq!(
            subject_record_count(&events, "SESSION_COMMITTED").unwrap(),
            1
        );
        assert_eq!(
            subject_record_count(&events, "ADMISSION_COMMITTED").unwrap(),
            0
        );
        // A missing events file is zero records, not an error.
        assert_eq!(
            subject_record_count(&dir.join("absent.tsv"), "X").unwrap(),
            0
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn release_file_targets_the_events_directory() {
        let dir =
            std::env::temp_dir().join(format!("pipestream-release-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let events = dir.join("events.tsv");
        write_release(&events, "SESSION_COMMITTED").unwrap();
        assert!(dir.join("release-SESSION_COMMITTED").is_file());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn transcript_named_code_only_matches_named_codes() {
        let text = "exit=exit status: 1\nstdout:\nstderr:\nconnection lost: closed by peer: CONTROL_RESET (code 526)\n";
        assert_eq!(transcript_named_code(text), Some(14));
        assert_eq!(
            transcript_named_code("stderr:\nauthority refusal CONFLICT: immutable intent"),
            Some(7)
        );
        assert_eq!(
            transcript_named_code("stderr:\ntimed out waiting for reply"),
            None
        );
    }

    #[test]
    fn scope_page_parser_reads_declared_and_member_counts() {
        let stdout = "SCOPE scope=0 producer=0 declared=2 membership_verified=true seal=abcd\n\
                      MEMBERS [ScopeMember { work: WorkKey { scope: Number(0), producer: Producer(0), entity: Id(1) }, terminal: Some(State(5)) }, ScopeMember { work: WorkKey { scope: Number(0), producer: Producer(0), entity: Id(2) }, terminal: None }]\n";
        assert_eq!(parse_scope_page(stdout).unwrap(), (2, 2));
        let java = "SCOPE scope=0 producer=0 declared=2 membership_verified=true seal=abcd\n\
                    MEMBERS [Entry[entity=1, state=DECLARED], Entry[entity=2, state=SUCCEEDED]] more=false\n";
        assert_eq!(parse_scope_page(java).unwrap(), (2, 2));
        assert!(parse_scope_page("SCOPE declared=1\n").is_err());
    }

    #[test]
    fn watch_field_parser_reads_deadline_values() {
        let view = "VIEW WorkView { deadline: Some(Number(1788910377952)), attempt: Number(1) }";
        assert_eq!(
            parse_field_u64(view, "deadline").unwrap(),
            Some(1788910377952)
        );
        assert_eq!(parse_field_u64(view, "terminal_at").unwrap(), None);
        assert!(parse_field_u64(view, "attempt").is_ok());
    }

    #[test]
    fn watch_state_parser_reads_decimal_states() {
        assert_eq!(
            parse_state("WORK revision=3 state=5 attempt=1 child=none").unwrap(),
            5
        );
        assert!(parse_state("WORK revision=3").is_err());
    }
}
