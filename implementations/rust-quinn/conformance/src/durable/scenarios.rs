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
        "g2-duplicate-op-changed-params",
        "g2-simultaneous-duplicate",
        "g2-kill-server-after-admission-recovery",
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
        "g2-duplicate-op-changed-params" => g2_duplicate_op_changed_params(context),
        "g2-simultaneous-duplicate" => g2_simultaneous_duplicate(context),
        "g2-kill-server-after-admission-recovery" => {
            g2_kill_server_after_admission_recovery(context)
        }
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
    let members = members_line.matches("ScopeMember {").count() as u64;
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
            "g2-duplicate-op-changed-params",
            "g2-simultaneous-duplicate",
            "g2-kill-server-after-admission-recovery",
        ] {
            let row = rows.iter().find(|row| row.id == id).unwrap();
            assert!(row.rust_implemented, "{id} must be implemented");
        }
        assert_eq!(rows.iter().filter(|row| row.rust_implemented).count(), 4);
    }

    #[test]
    fn scope_page_parser_reads_declared_and_member_counts() {
        let stdout = "SCOPE scope=0 producer=0 declared=2 membership_verified=true seal=abcd\n\
                      MEMBERS [ScopeMember { work: WorkKey { scope: Number(0), producer: Producer(0), entity: Id(1) }, terminal: Some(State(5)) }, ScopeMember { work: WorkKey { scope: Number(0), producer: Producer(0), entity: Id(2) }, terminal: None }]\n";
        assert_eq!(parse_scope_page(stdout).unwrap(), (2, 2));
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
