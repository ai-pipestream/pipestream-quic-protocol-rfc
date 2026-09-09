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
    process::{Command, Output, Stdio},
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
            "g1-declaration-capacity",
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
            "g3-store-ownership",
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
        "g1-empty-input",
        "g1-zero-output",
        "g1-mode1-branch",
        "g1-mode2-descendants",
        "g1-oversize-payload",
        "g1-out-of-order-pages",
        "g1-declaration-capacity",
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
        "g3-input-before-metadata",
        "g3-orphan-cleanup",
        "g3-restart-same-roots",
        "g3-store-ownership",
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
    if (row.id == "g1-leaf-copy"
        || G1_BATCH_A_ROWS.contains(&row.id)
        || G1_BATCH_B_ROWS.contains(&row.id))
        && context.java_jar.is_some()
    {
        "rust-client/rust-server, rust-client/java-server, java-client/rust-server".to_owned()
    } else if row.id == "g3-store-ownership" && context.java_jar.is_some() {
        "rust-client/rust-server, rust-client/java-server".to_owned()
    } else if G3_BATCH_A_ROWS.contains(&row.id) && context.java_jar.is_some() {
        "rust-client/rust-server, rust-client/java-server, java-client/rust-server".to_owned()
    } else if row.id.starts_with("g5-") && context.java_jar.is_some() {
        "rust-client/rust-server, rust-client/java-server".to_owned()
    } else if JAVA_SERVER_HOOKED_ROWS.contains(&row.id) && context.java_jar.is_some() {
        "rust-client/rust-server, java-client/rust-server, rust-client/java-server".to_owned()
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
        "g1-empty-input" => g1_empty_input(context),
        "g1-zero-output" => g1_zero_output(context),
        "g1-mode1-branch" => g1_mode1_branch(context),
        "g1-mode2-descendants" => g1_mode2_descendants(context),
        "g1-oversize-payload" => g1_oversize_payload(context),
        "g1-out-of-order-pages" => g1_out_of_order_pages(context),
        "g1-declaration-capacity" => g1_declaration_capacity(context),
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
        "g3-input-before-metadata" => g3_input_before_metadata(context),
        "g3-orphan-cleanup" => g3_orphan_cleanup(context),
        "g3-restart-same-roots" => g3_restart_same_roots(context),
        "g3-store-ownership" => g3_store_ownership(context),
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

/// One declare batch. An empty `entities` slice drives the CLI with no
/// `--entities` flag at all: that is the only way both subjects can express
/// an empty batch (sealed or not). `--seal` finalizes the scope membership.
/// Returns the operation id and the validated receipt text.
fn declare_batch(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    domain: &str,
    index: u32,
    entities: &[u64],
    seal: bool,
) -> Result<(String, String)> {
    declare_scoped_batch(session, events, seed, domain, index, 0, entities, seal)
}

/// A declare batch into an explicit scope (nonzero for branch child scopes).
#[allow(clippy::too_many_arguments)]
fn declare_scoped_batch(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    domain: &str,
    index: u32,
    scope: u64,
    entities: &[u64],
    seal: bool,
) -> Result<(String, String)> {
    let declare = oracle::operation_hex(oracle::operation_id(seed, domain, index));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&declare)?),
        None,
        None,
        None,
        None,
    )?;
    let entities_text = entities
        .iter()
        .map(|entity| entity.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let scope_text = scope.to_string();
    let mut operation = vec!["declare", "--operation", &declare];
    if scope != 0 {
        operation.push("--scope");
        operation.push(&scope_text);
    }
    if !entities.is_empty() {
        operation.push("--entities");
        operation.push(&entities_text);
    }
    if seal {
        operation.push("--seal");
    }
    let declared = session.op(&operation)?;
    let receipt = require(&declared, "RECEIPT", "declare operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&declare)?),
        None,
        None,
        None,
        None,
    )?;
    Ok((declare, receipt))
}

fn declare_sealed(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    domain: &str,
    entities: &[u64],
) -> Result<String> {
    declare_batch(session, events, seed, domain, 0, entities, true)
        .map(|(declare, _receipt)| declare)
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
    admit_application(
        session,
        events,
        seed,
        domain,
        declaration,
        work,
        input,
        "copy/v2",
        1,
    )
}

/// Admission with an explicit application and output budget. `--output-count
/// 0` (consume/v2) requests a zero-object result manifest.
#[allow(clippy::too_many_arguments)]
fn admit_application(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    domain: &str,
    declaration: &str,
    work: &str,
    input: &Path,
    application: &str,
    output_count: u64,
) -> Result<String> {
    admit_modeled(
        session,
        events,
        seed,
        domain,
        declaration,
        work,
        input,
        application,
        0,
        output_count,
    )
}

/// Admission with an explicit application mode: mode 1 (reassemble/v2)
/// allocates a producer-0 child scope; mode 2 (chunk-copy/v2) a producer-1
/// child scope. The admission receipt names the allocated scope in the
/// subsequent watch view (`child=S:P`).
#[allow(clippy::too_many_arguments)]
fn admit_modeled(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    domain: &str,
    declaration: &str,
    work: &str,
    input: &Path,
    application: &str,
    mode: u64,
    output_count: u64,
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
        application,
        "--mode",
        &mode.to_string(),
        "--output-count",
        &output_count.to_string(),
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
// G1 lifecycle rows, batch A (milestone 8): no fixture hooks, three directions
// ---------------------------------------------------------------------------

const G1_BATCH_A_ROWS: &[&str] = &[
    "g1-empty-input",
    "g1-zero-output",
    "g1-oversize-payload",
    "g1-out-of-order-pages",
    "g1-declaration-capacity",
];

/// Branch-mode rows (milestone 9): reassemble/v2 mode 1 (caller-expanded)
/// and chunk-copy/v2 mode 2 (authority-expanded), three directions each.
const G1_BATCH_B_ROWS: &[&str] = &["g1-mode1-branch", "g1-mode2-descendants"];

/// Mode-1 row: two caller-supplied child parts, deliberately uneven.
const MODE1_PART_ONE_LEN: usize = 40_000;
const MODE1_PART_TWO_LEN: usize = 25_000;
/// Mode-2 row: 200,000 bytes = four 65,536-byte chunks (the last partial).
const MODE2_INPUT_LEN: usize = 200_000;
const MODE2_CHUNK_LEN: usize = 65_536;
const MODE2_CHILDREN: u64 = 4;

/// Oversize-payload input: 8 MiB, two orders of magnitude over the subjects'
/// 64 KiB-class default stream receive window (v2_flow::Limits default 65536;
/// neither CLI exposes a window flag — recorded as a measurement note).
const OVERSIZE_INPUT_LEN: usize = 8 * 1024 * 1024;
/// Small paging-row admission input.
const PAGES_INPUT_LEN: usize = 1024;
/// SHA-256 of the empty string; the zero-length input/result expectation.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Run one batch-A row in the canonical rust/rust direction plus both mixed
/// directions (each mixed direction in its own subdirectory), mirroring
/// g1-leaf-copy's layout.
fn run_three_directions(
    context: &ScenarioContext,
    row_id: &str,
    direction: fn(&ScenarioContext, &Path, Subject, Subject) -> Result<()>,
) -> Result<()> {
    let scenario_dir = context.scenario_dir(row_id);
    direction(context, &scenario_dir, Subject::Rust, Subject::Rust)?;
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
        direction(context, &direction_dir, server, client)?;
    }
    Ok(())
}

/// Manifest object count across both subjects' renderings: Rust Debug
/// `Output { ... }` inside `outputs: [...]`, Java `Output[...]` inside
/// `outputs=[...]`.
fn manifest_object_count(text: &str) -> usize {
    text.matches("Output {").count() + text.matches("Output[").count()
}

/// Whether the manifest lists an object of length zero (Rust
/// `length: Number(0)`, Java `length=0`).
fn manifest_has_zero_length_object(text: &str) -> bool {
    text.contains("length: Number(0)") || text.contains("length=0")
}

/// g1-empty-input: declare + admit a zero-length input to copy/v2. Admission,
/// terminal success, a one-object manifest, and a zero-byte read whose
/// length/hash/FIN verify are all expected to succeed; a zero-length result
/// must be distinguishable from a refusal (the read installs a zero-byte
/// file and prints VERIFIED, it does not fail).
fn g1_empty_input(context: &ScenarioContext) -> Result<()> {
    run_three_directions(context, "g1-empty-input", g1_empty_input_direction)
}

fn g1_empty_input_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-empty-input";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", "0".into()),
            ("input_sha256", EMPTY_SHA256.into()),
            ("expected_output_sha256", EMPTY_SHA256.into()),
            ("work", "0:0:1".into()),
            ("attempt", "1".into()),
            ("manifest_objects", "1".into()),
            ("output_len", "0".into()),
        ],
    )?;
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, [])?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/input.bin".into(),
            len: 0,
            sha256: EMPTY_SHA256.into(),
        }),
    )?;

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
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

    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    let manifest_text = require(&manifest, "MANIFEST", "manifest operation")?;
    ensure!(
        manifest_object_count(&manifest_text) == 1,
        "zero-length input must produce a one-object manifest:\n{manifest_text}"
    );
    ensure!(
        manifest_has_zero_length_object(&manifest_text),
        "manifest object must have length 0:\n{manifest_text}"
    );
    fs::write(artifacts.join("manifest.txt"), &manifest_text)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/manifest.txt".into(),
            len: manifest_text.len() as u64,
            sha256: oracle::sha256_hex(manifest_text.as_bytes()),
        }),
    )?;

    // Zero-length read: must succeed (VERIFIED) and install zero bytes — the
    // empty result is distinguishable from any refusal, which would exit
    // nonzero and install nothing.
    let actual_sha256 = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &[],
        EMPTY_SHA256,
        &artifacts,
        "output.bin",
    )?;
    ensure!(
        actual_sha256 == EMPTY_SHA256,
        "zero-length read must hash to the empty-string sha256"
    );
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
        ("application", "copy/v2".into()),
        ("input_len", "0".into()),
        ("terminal_state", "5".into()),
        ("manifest_objects", "1".into()),
        ("output_len", "0".into()),
        ("output_sha256", actual_sha256),
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

/// g1-zero-output: admit a non-empty input to consume/v2 (zero outputs).
/// Terminal success must come with an EMPTY manifest; a result read of index
/// 0 refuses NOT_FOUND (5) as a named refusal; and the success is
/// distinguishable from failure through the work view state plus the
/// manifest count, not through the read.
fn g1_zero_output(context: &ScenarioContext) -> Result<()> {
    run_three_directions(context, "g1-zero-output", g1_zero_output_direction)
}

fn g1_zero_output_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-zero-output";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", INPUT_LEN.to_string()),
            ("input_sha256", input_sha256.clone()),
            ("application", "consume/v2".into()),
            ("work", "0:0:1".into()),
            ("attempt", "1".into()),
            ("terminal_state", "5".into()),
            ("manifest_objects", "0".into()),
            (
                "select_index0_refusal",
                if client == Subject::Java {
                    "named deviation: java-client FRAME_ERROR integer outside schema range"
                } else {
                    "NOT_FOUND"
                }
                .into(),
            ),
        ],
    )?;
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/input.bin".into(),
            len: input.len() as u64,
            sha256: input_sha256.clone(),
        }),
    )?;

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let _receipt = admit_application(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
        "consume/v2",
        0,
    )?;
    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit, WATCH_TIMEOUT)?;

    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    let manifest_text = require(&manifest, "MANIFEST", "manifest operation")?;
    ensure!(
        manifest_object_count(&manifest_text) == 0,
        "consume/v2 must produce an empty manifest:\n{manifest_text}"
    );
    fs::write(artifacts.join("manifest.txt"), &manifest_text)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/manifest.txt".into(),
            len: manifest_text.len() as u64,
            sha256: oracle::sha256_hex(manifest_text.as_bytes()),
        }),
    )?;

    // Result read of index 0 on an empty manifest: named refusal. The rust
    // client surfaces the authority's NOT_FOUND (5). The Java client
    // deviates: its select path raises a client-side FRAME_ERROR ("integer
    // outside schema range") instead of surfacing the server's NOT_FOUND —
    // recorded as a named subject deviation, with the rust check unweakened.
    let select = session.op(&[
        "select",
        "--work",
        "0:0:1",
        "--attempt",
        "1",
        "--index",
        "0",
    ])?;
    let select_text = transcript(&select);
    fs::write(artifacts.join("select-index0.txt"), &select_text)?;
    ensure!(
        !select.status.success(),
        "select index 0 on a zero-output manifest must refuse\n{select_text}"
    );
    let (select_refusal, select_code) = if client == Subject::Java {
        let stderr = String::from_utf8_lossy(&select.stderr);
        ensure!(
            stderr.contains("integer outside schema range"),
            "java-client select on an empty manifest must name its FRAME_ERROR deviation\n{select_text}"
        );
        (
            "FRAME_ERROR: integer outside schema range (java-client deviation; server refuses NOT_FOUND)"
                .to_owned(),
            1u32,
        )
    } else {
        let named = refusal_named_line(&String::from_utf8_lossy(&select.stderr), &["NOT_FOUND"]);
        ensure!(
            named.is_some(),
            "select refusal must name NOT_FOUND (5)\n{select_text}"
        );
        (named.expect("checked above"), 5u32)
    };
    events.append(
        "",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        Some(select_code),
        None,
    )?;
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
        ("application", "consume/v2".into()),
        ("input_len", INPUT_LEN.to_string()),
        ("terminal_state", "5".into()),
        ("manifest_objects", "0".into()),
        ("select_index0_refusal", select_refusal),
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

/// g1-oversize-payload: admit an 8 MiB input to copy/v2 against the subjects'
/// 64 KiB-class default stream window. The large transfer is spawned without
/// waiting; a concurrent next-sequence control op from a second connection
/// must complete while the transfer is in flight (overlap and latency
/// recorded), then the transfer completes byte-exact.
fn g1_oversize_payload(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g1-oversize-payload",
        g1_oversize_payload_direction,
    )
}

fn g1_oversize_payload_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-oversize-payload";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    let input = oracle::dataset(context.seed, OVERSIZE_INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", OVERSIZE_INPUT_LEN.to_string()),
            ("input_sha256", input_sha256.clone()),
            ("expected_output_sha256", input_sha256.clone()),
            ("work", "0:0:1".into()),
            ("attempt", "1".into()),
            ("flow_receive_stream_bytes", "65536".into()),
            ("concurrent_control_op", "next-sequence".into()),
        ],
    )?;
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/input.bin".into(),
            len: input.len() as u64,
            sha256: input_sha256.clone(),
        }),
    )?;

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let admit_args = vec![
        "admit".to_owned(),
        "--operation".to_owned(),
        admit.clone(),
        "--declaration".to_owned(),
        declare.clone(),
        "--work".to_owned(),
        "0:0:1".to_owned(),
        "--input".to_owned(),
        crate::path(&input_path),
        "--application".to_owned(),
        "copy/v2".to_owned(),
    ];
    let mut child = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &op_refs(&admit_args),
    )?;

    // Give the transfer a brief head start, then measure a control op on a
    // second connection while the large stream is (hopefully) still in
    // flight. On a fast loopback the transfer may already have finished;
    // that is recorded, never assumed.
    thread::sleep(Duration::from_millis(50));
    let admit_running = child.try_wait().context("poll in-flight admit")?.is_none();
    let control_started = Instant::now();
    let control_sequence = session
        .fixture
        .next_sequence(&session.server, "alice")
        .context("concurrent next-sequence control op")?;
    let control_latency = control_started.elapsed();
    ensure!(
        control_sequence >= 2,
        "control op next-sequence must report an allocated sequence, got {control_sequence}"
    );

    let admitted = AuthorityFixture::wait_client_op(child, RECOVERY_TIMEOUT)?;
    let admitted_text = transcript(&admitted);
    ensure!(
        admitted.status.success() && String::from_utf8_lossy(&admitted.stdout).contains("RECEIPT"),
        "oversize admit did not complete\n{admitted_text}"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    watch_terminal(&session, &mut events, "0:0:1", &admit, RECOVERY_TIMEOUT)?;

    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    let manifest_text = require(&manifest, "MANIFEST", "manifest operation")?;
    ensure!(
        manifest_object_count(&manifest_text) == 1,
        "oversize copy must produce a one-object manifest:\n{manifest_text}"
    );
    fs::write(artifacts.join("manifest.txt"), &manifest_text)?;

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

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
        ("application", "copy/v2".into()),
        ("input_len", OVERSIZE_INPUT_LEN.to_string()),
        (
            "flow_receive_stream_bytes",
            "65536 (subject default; neither CLI exposes a window flag)".into(),
        ),
        (
            "control_op",
            "next-sequence (second connection, journal-free)".into(),
        ),
        (
            "control_latency_ms",
            control_latency.as_millis().to_string(),
        ),
        (
            "control_overlapped_inflight_admit",
            admit_running.to_string(),
        ),
        ("control_sequence", control_sequence.to_string()),
        ("terminal_state", "5".into()),
        ("manifest_objects", "1".into()),
        ("output_len", OVERSIZE_INPUT_LEN.to_string()),
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

/// One observed scope page, parsed from the shared SCOPE/MEMBERS rendering
/// both subject CLIs print.
#[derive(Debug)]
struct PageObservation {
    producer: u64,
    declared: u64,
    membership_verified: bool,
    seal: Option<String>,
    /// (entity id, state name); nonterminal entries render as DECLARED on
    /// both subjects (`terminal: None` on Rust, `state=DECLARED` on Java).
    members: Vec<(u64, String)>,
    /// `more` flag as printed by the Java CLI; `None` where the CLI does not
    /// expose it (Rust) — a named measurement gap, never silently assumed.
    more: Option<bool>,
}

fn parse_token_u64(line: &str, key: &str) -> Result<u64> {
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(key))
        .with_context(|| format!("page line did not report {key:?}:\n{line}"))?
        .parse::<u64>()
        .with_context(|| format!("page {key:?} value is not decimal"))
}

/// Parse the MEMBERS line of either CLI into ordered (entity, state) pairs.
fn parse_page_members(line: &str) -> Result<Vec<(u64, String)>> {
    let mut members = Vec::new();
    if line.contains("ScopeMember {") {
        for chunk in line.split("ScopeMember {").skip(1) {
            let id_text = chunk
                .split("entity: Id(")
                .nth(1)
                .context("rust member lacks entity: Id(")?;
            let id: u64 = id_text
                .split(')')
                .next()
                .context("rust member entity id unterminated")?
                .parse()
                .context("rust member entity id is not decimal")?;
            let state = match chunk.split("terminal: Some(").nth(1) {
                Some(rest) => {
                    // Rust renders the state as `State(5))` inside the member
                    // chunk; translate the terminal codes both subjects name
                    // in text.
                    let raw = rest.split([',', ' ']).next().unwrap_or_default().trim();
                    match raw.strip_prefix("State(") {
                        Some(code) => match code.trim_end_matches(')') {
                            "5" => "SUCCEEDED".to_owned(),
                            "6" => "FAILED".to_owned(),
                            "12" => "CANCELLED".to_owned(),
                            "13" => "SKIPPED".to_owned(),
                            _ => raw.to_owned(),
                        },
                        None => raw.to_owned(),
                    }
                }
                None => "DECLARED".to_owned(),
            };
            members.push((id, state));
        }
        return Ok(members);
    }
    for chunk in line.split("Entry[").skip(1) {
        let id_text = chunk
            .split("entity=")
            .nth(1)
            .context("java member lacks entity=")?;
        let id: u64 = id_text
            .split([',', ']'])
            .next()
            .context("java member entity id empty")?
            .parse()
            .context("java member entity id is not decimal")?;
        let state = chunk
            .split("state=")
            .nth(1)
            .and_then(|rest| rest.split([',', ']']).next())
            .context("java member lacks state=")?
            .trim()
            .to_owned();
        members.push((id, state));
    }
    Ok(members)
}

/// Page scope 0 and parse the SCOPE/MEMBERS rendering.
fn observe_page(session: &Session, after: u64, limit: u64) -> Result<(String, PageObservation)> {
    observe_scope_page(session, 0, after, limit)
}

/// Page an explicit scope and parse the shared SCOPE/MEMBERS rendering both
/// subject CLIs print, including the scope/producer identity fields.
fn observe_scope_page(
    session: &Session,
    scope: u64,
    after: u64,
    limit: u64,
) -> Result<(String, PageObservation)> {
    let output = session.op(&[
        "page",
        "--scope",
        &scope.to_string(),
        "--after",
        &after.to_string(),
        "--limit",
        &limit.to_string(),
    ])?;
    let stdout = require(&output, "SCOPE", "page operation")?;
    let scope_line = stdout
        .lines()
        .find(|line| line.starts_with("SCOPE "))
        .context("page did not print a SCOPE line")?;
    let members_line = stdout
        .lines()
        .find(|line| line.starts_with("MEMBERS "))
        .context("page did not print a MEMBERS line")?;
    let seal = scope_line
        .split_whitespace()
        .find_map(|token| token.strip_prefix("seal="))
        .context("page SCOPE line did not report seal=")?
        .trim_end_matches(';');
    let observation = PageObservation {
        producer: parse_token_u64(scope_line, "producer=")?,
        declared: parse_token_u64(scope_line, "declared=")?,
        membership_verified: scope_line.contains("membership_verified=true"),
        seal: (seal != "none").then(|| seal.to_owned()),
        members: parse_page_members(members_line)?,
        more: members_line
            .split("more=")
            .nth(1)
            .and_then(|token| token.trim_end_matches(';').parse::<bool>().ok()),
    };
    ensure!(
        parse_token_u64(scope_line, "scope=")? == scope,
        "page answered scope {} for a request on scope {scope}",
        parse_token_u64(scope_line, "scope=")?
    );
    Ok((stdout, observation))
}

/// Parse the `child=S:P` token of a watch WORK line; `None` for `child=none`.
fn parse_child_scope(stdout: &str) -> Result<Option<(u64, u64)>> {
    let line = stdout
        .lines()
        .find(|line| line.starts_with("WORK "))
        .context("watch did not print a WORK line")?;
    let token = line
        .split_whitespace()
        .find_map(|token| token.strip_prefix("child="))
        .context("watch WORK line did not report child=")?;
    if token == "none" {
        return Ok(None);
    }
    let (scope, producer) = token
        .split_once(':')
        .context("child token must render as scope:producer")?;
    Ok(Some((
        scope.parse().context("child scope is not decimal")?,
        producer.parse().context("child producer is not decimal")?,
    )))
}

/// Assert one page's shape against the driver's own declared-ids model.
fn ensure_page_shape(
    after: u64,
    limit: u64,
    observation: &PageObservation,
    expected_members: &[(u64, String)],
) -> Result<()> {
    ensure!(
        observation.producer == 0,
        "scope producer must be 0, got {}",
        observation.producer
    );
    ensure!(
        observation.members.len() as u64 <= limit,
        "page after={after} limit={limit} returned {} members, exceeding the limit",
        observation.members.len()
    );
    ensure!(
        observation.members == expected_members,
        "page after={after} limit={limit} expected members {expected_members:?}, got {:?}",
        observation.members
    );
    ensure!(
        observation.members.iter().all(|(id, _)| *id > after),
        "page after={after} returned a member at or before the cursor: {:?}",
        observation.members
    );
    Ok(())
}

/// The `more` expectation derived from the driver's own model: more members
/// exist beyond this page iff the declared ids above the cursor outnumber
/// the page limit. Where the CLI prints the flag (Java) it must match
/// exactly; where it does not (Rust) the gap is recorded via `more_unexposed`.
fn ensure_more_flag(
    after: u64,
    limit: u64,
    observation: &PageObservation,
    declared_ids: &[u64],
    more_unexposed: &mut bool,
) -> Result<()> {
    let expected = declared_ids.iter().filter(|id| **id > after).count() as u64 > limit;
    match observation.more {
        Some(flag) => ensure!(
            flag == expected,
            "page after={after} limit={limit} more flag: expected {expected}, got {flag}"
        ),
        None => *more_unexposed = true,
    }
    Ok(())
}

/// g1-out-of-order-pages: declare three increasing batches (the last one
/// seals), page with small limits from several after-entity cursors while
/// admissions interleave, then verify the committed seal digest against the
/// driver's independently computed pipestream-scope-seal-v2 commitment.
fn g1_out_of_order_pages(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g1-out-of-order-pages",
        g1_out_of_order_pages_direction,
    )
}

fn g1_out_of_order_pages_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-out-of-order-pages";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    let batch_a = [1u64, 2, 3];
    let batch_b = [5u64, 7];
    let batch_c = [9u64, 10, 11, 12];
    let mut declared_ids: Vec<u64> = Vec::new();
    let mut pages_log = String::new();
    let mut more_unexposed = false;

    let expected_seal = |declared_ids: &[u64]| {
        oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, declared_ids)
    };
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "declared_ids",
                batch_a
                    .iter()
                    .chain(&batch_b)
                    .chain(&batch_c)
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            ("scope", "0".into()),
            ("producer", "0".into()),
            ("parent", "none (root scope)".into()),
            (
                "expected_seal_sha256",
                expected_seal(
                    &batch_a
                        .iter()
                        .chain(&batch_b)
                        .chain(&batch_c)
                        .copied()
                        .collect::<Vec<_>>(),
                ),
            ),
        ],
    )?;

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let log_page = |pages_log: &mut String, label: &str, after: u64, limit: u64, stdout: &str| {
        pages_log.push_str(&format!(
            "== {label} page after={after} limit={limit}\n{stdout}\n"
        ));
    };

    // Batch A: three entities, scope still unsealed.
    declare_batch(
        &session,
        &mut events,
        context.seed,
        "declare-a",
        0,
        &batch_a,
        false,
    )?;
    declared_ids.extend_from_slice(&batch_a);
    let (stdout, page) = observe_page(&session, 0, 3)?;
    ensure_page_shape(
        0,
        3,
        &page,
        &[
            (1, "DECLARED".into()),
            (2, "DECLARED".into()),
            (3, "DECLARED".into()),
        ],
    )?;
    ensure!(
        page.declared == 3,
        "declared must be 3, got {}",
        page.declared
    );
    ensure!(page.seal.is_none(), "scope must be unsealed after batch A");
    ensure_more_flag(0, 3, &page, &declared_ids, &mut more_unexposed)?;
    log_page(&mut pages_log, "batch-a", 0, 3, &stdout);

    // Batch B: growth visible in the declared count while still unsealed.
    declare_batch(
        &session,
        &mut events,
        context.seed,
        "declare-b",
        0,
        &batch_b,
        false,
    )?;
    declared_ids.extend_from_slice(&batch_b);
    let (stdout, page) = observe_page(&session, 3, 2)?;
    ensure_page_shape(
        3,
        2,
        &page,
        &[(5, "DECLARED".into()), (7, "DECLARED".into())],
    )?;
    ensure!(
        page.declared == 5,
        "declared must be 5, got {}",
        page.declared
    );
    ensure!(page.seal.is_none(), "scope must be unsealed after batch B");
    ensure_more_flag(3, 2, &page, &declared_ids, &mut more_unexposed)?;
    log_page(&mut pages_log, "batch-b", 3, 2, &stdout);

    // Admit a subset: entity 5 runs to terminal success while the scope is
    // still open; the page must show its terminal state among the declared.
    // Batch A's operation id is recomputed from the same seed/domain/index
    // inputs declare_batch used (mirrors g1-leaf-copy's admit step).
    let input = oracle::dataset(context.seed, PAGES_INPUT_LEN);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    // The covering declaration for entity 5 is batch B (the batch that
    // declared it); an admission's declaration receipt must cover the entity.
    let declare_b = oracle::operation_hex(oracle::operation_id(context.seed, "declare-b", 0));
    let _receipt = admit_application(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare_b,
        "0:0:5",
        &input_path,
        "consume/v2",
        0,
    )?;
    let admit5 = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    watch_terminal(&session, &mut events, "0:0:5", &admit5, WATCH_TIMEOUT)?;
    let (stdout, page) = observe_page(&session, 0, 4)?;
    ensure_page_shape(
        0,
        4,
        &page,
        &[
            (1, "DECLARED".into()),
            (2, "DECLARED".into()),
            (3, "DECLARED".into()),
            (5, "SUCCEEDED".into()),
        ],
    )?;
    ensure!(
        page.members
            .iter()
            .any(|(id, state)| *id == 5 && state == "SUCCEEDED"),
        "admitted entity 5 must render SUCCEEDED on the page:\n{stdout}"
    );
    ensure_more_flag(0, 4, &page, &declared_ids, &mut more_unexposed)?;
    log_page(&mut pages_log, "admitted-5", 0, 4, &stdout);

    // Batch C seals the scope: nine declared entities, seal digest present.
    declare_batch(
        &session,
        &mut events,
        context.seed,
        "declare-c",
        0,
        &batch_c,
        true,
    )?;
    declared_ids.extend_from_slice(&batch_c);
    let (stdout, page) = observe_page(&session, 0, 4)?;
    ensure!(
        page.declared == 9,
        "declared must be 9, got {}",
        page.declared
    );
    ensure!(page.seal.is_some(), "scope must be sealed after batch C");
    ensure_page_shape(
        0,
        4,
        &page,
        &[
            (1, "DECLARED".into()),
            (2, "DECLARED".into()),
            (3, "DECLARED".into()),
            (5, "SUCCEEDED".into()),
        ],
    )?;
    ensure_more_flag(0, 4, &page, &declared_ids, &mut more_unexposed)?;
    log_page(&mut pages_log, "sealed-c", 0, 4, &stdout);

    // Exact more-flag chaining: walk the membership in limit-3 pages; every
    // non-final page must be exactly full and the final page exactly covers
    // the remaining members.
    let mut walk = Vec::new();
    let mut after = 0u64;
    loop {
        let (stdout, page) = observe_page(&session, after, 3)?;
        let expected: Vec<(u64, String)> = declared_ids
            .iter()
            .copied()
            .filter(|id| *id > after)
            .take(3)
            .map(|id| {
                let state = if id == 5 { "SUCCEEDED" } else { "DECLARED" };
                (id, state.to_owned())
            })
            .collect();
        ensure_page_shape(after, 3, &page, &expected)?;
        ensure_more_flag(after, 3, &page, &declared_ids, &mut more_unexposed)?;
        let last = page.members.last().map(|(id, _)| *id);
        walk.extend(page.members.iter().map(|(id, _)| *id));
        log_page(&mut pages_log, &format!("walk-{after}"), after, 3, &stdout);
        let short_page = (page.members.len() as u64) < 3;
        after = last.unwrap_or(after);
        if short_page || walk.len() == declared_ids.len() {
            break;
        }
    }
    ensure!(
        walk == declared_ids,
        "paged membership {walk:?} must equal the declared ids {declared_ids:?}"
    );

    // Empty page past the end: recorded as evidence that an empty page is
    // distinguishable — completeness above came from a short NON-empty page,
    // never from this one.
    let (stdout, tail) = observe_page(&session, *declared_ids.last().expect("declared"), 3)?;
    ensure!(
        tail.members.is_empty(),
        "tail page past the last member must be empty, got {:?}",
        tail.members
    );
    ensure_more_flag(
        *declared_ids.last().expect("declared"),
        3,
        &tail,
        &declared_ids,
        &mut more_unexposed,
    )?;
    log_page(
        &mut pages_log,
        "empty-tail",
        *declared_ids.last().expect("declared"),
        3,
        &stdout,
    );

    // Final full page: the committed seal digest must equal the driver's
    // independently computed commitment over the declared membership.
    let (stdout, final_page) = observe_page(&session, 0, 256)?;
    let expected_members: Vec<(u64, String)> = declared_ids
        .iter()
        .map(|id| {
            (
                *id,
                if *id == 5 { "SUCCEEDED" } else { "DECLARED" }.to_owned(),
            )
        })
        .collect();
    ensure_page_shape(0, 256, &final_page, &expected_members)?;
    ensure_more_flag(0, 256, &final_page, &declared_ids, &mut more_unexposed)?;
    let committed = final_page
        .seal
        .as_ref()
        .context("final page must carry the committed seal digest")?;
    let expected = expected_seal(&declared_ids);
    ensure!(
        committed == &expected,
        "committed scope seal {committed} does not match the independent oracle {expected}"
    );
    ensure!(
        final_page.membership_verified,
        "full final page must verify membership against the seal:\n{stdout}"
    );
    log_page(&mut pages_log, "final", 0, 256, &stdout);

    fs::write(artifacts.join("pages.txt"), &pages_log)?;
    fs::write(artifacts.join("expected-seal.txt"), format!("{expected}\n"))?;
    events.append(
        "",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/pages.txt".into(),
            len: pages_log.len() as u64,
            sha256: oracle::sha256_hex(pages_log.as_bytes()),
        }),
    )?;
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
        ("declared_total", declared_ids.len().to_string()),
        ("committed_seal_sha256", committed.clone()),
        ("seal_matches_oracle", "true".into()),
        (
            "membership_verified",
            final_page.membership_verified.to_string(),
        ),
        (
            "more_flag",
            if more_unexposed {
                "not exposed by the rust CLI; chaining derived from the page walk (named gap)"
                    .into()
            } else {
                "exposed and exact on every page".into()
            },
        ),
        (
            "parent_field",
            "not printed by either page CLI; scope 0 is the root (parent none by construction)"
                .into(),
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

/// g1-declaration-capacity: every declaration violation refuses with a named
/// error; the accepted batches drive capacity accounting; the final empty
/// sealed batch commits the scope seal, verified against the oracle; and
/// declaration alone never admits (no execution evidence).
fn g1_declaration_capacity(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g1-declaration-capacity",
        g1_declaration_capacity_direction,
    )
}

/// The declared total out of a declare receipt: Rust Debug
/// `declared: Number(258)`, Java `declared=258`.
fn parse_declared_count(receipt: &str) -> Result<u64> {
    if let Some(rest) = receipt.split("declared: Number(").nth(1) {
        return rest
            .split(')')
            .next()
            .context("declared count unterminated")?
            .parse()
            .context("declared count is not decimal");
    }
    receipt
        .split("declared=")
        .nth(1)
        .and_then(|rest| rest.split([',', ']']).next())
        .and_then(|digits| digits.parse::<u64>().ok())
        .context("receipt did not report the declared count:\n{receipt}")
}

/// Capture one expected-to-fail declare and return its transcript text.
fn expect_declare_failure(
    session: &Session,
    artifacts: &Path,
    name: &str,
    operation: &[&str],
) -> Result<(Output, String)> {
    let output = session.op(operation)?;
    let text = transcript(&output);
    fs::write(artifacts.join(name), &text)?;
    ensure!(
        !output.status.success(),
        "{name} was expected to refuse but exited zero\n{text}"
    );
    Ok((output, text))
}

#[allow(clippy::too_many_arguments)]
fn g1_declaration_capacity_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-declaration-capacity";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    // The published CLIs fix the authority's physical completion funding, and
    // the rust subject's shipped defaults enforce it in layers (measured, not
    // re-probed here): (1) a single transaction also fits the WAL/shm of the
    // default storage — a 254-entity declare failed `authority refusal
    // LIMIT_EXCEEDED: physical database capacity exhausted` while a 115-entity
    // transaction succeeded even at cumulative 229; (2) cumulative record-
    // completion funding caps the DECLARED-ENTITY total at ~256 on default
    // storage (256 accepted via small batches, 258 refused `authority record
    // completion capacity exhausted`). No per-batch funding limit exists. The
    // protocol's 256/batch schema bound is therefore unreachable at the rust
    // authority (the single-TX physical cap binds first), and the rust clap
    // arity (1..=256) additionally enforces it client-side. Per the matrix's
    // escape clause, this row declares batches of increasing size and records
    // the ACTUAL refusal bound of each server subject rather than asserting a
    // bound the subject cannot exhibit.
    let mut declared_ids: Vec<u64> = Vec::new();

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
    ];

    // (c) within-batch non-increasing IDs. Both subjects validate the
    // declaration commitment client-side before any wire send, so the named
    // refusal is the client-side validation text; the wire-level refusal is
    // unreachable through either published CLI (named measurement gap).
    let bad = oracle::operation_hex(oracle::operation_id(context.seed, "decl-bad-order", 0));
    let (_output, text) = expect_declare_failure(
        &session,
        &artifacts,
        "declare-within-batch-refusal.txt",
        &["declare", "--operation", &bad, "--entities", "5,3"],
    )?;
    let named = if client == Subject::Rust {
        ensure!(
            text.contains("invalid declaration commitment"),
            "within-batch refusal must name the validation failure\nt{text}"
        );
        "client-side: invalid declaration commitment".to_owned()
    } else {
        "client-side refusal (transcript recorded)".to_owned()
    };
    observed.push(("case_c_within_batch", named));

    // (c) across-batch non-increasing IDs reach the authority: the second
    // batch's first id (2) does not exceed the retained last entity (2).
    let (_ok12, _receipt12) = declare_batch(
        &session,
        &mut events,
        context.seed,
        "decl-1-2",
        0,
        &[1, 2],
        false,
    )?;
    declared_ids.extend([1, 2]);
    let dup = oracle::operation_hex(oracle::operation_id(context.seed, "decl-2-3", 0));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&dup)?),
        None,
        None,
        None,
        None,
    )?;
    let (output, text) = expect_declare_failure(
        &session,
        &artifacts,
        "declare-across-batch-refusal.txt",
        &["declare", "--operation", &dup, "--entities", "2,3"],
    )?;
    let conflict = refusal_named_line(&String::from_utf8_lossy(&output.stderr), &["CONFLICT"]);
    ensure!(
        conflict.is_some(),
        "across-batch refusal must name CONFLICT (7)\n{text}"
    );
    events.append("", Some(hex_to_id(&dup)?), None, None, Some(7), None)?;
    observed.push(("case_c_across_batch", conflict.expect("checked above")));

    // (d) accepted batch: a fixed 100-entity batch [3..=102] well under the
    // single-transaction physical cap, then a loop of 50-entity batches
    // starting at id 103 that drives the cumulative completion funding until
    // the authority refuses. The first loop batch must succeed (so the loop
    // exercised at least one acceptance past d1); the refusal that ends the
    // loop is the recorded actual bound.
    let d1_ids: Vec<u64> = (3..=102).collect();
    let (_ok_d1, receipt_d1) = declare_batch(
        &session,
        &mut events,
        context.seed,
        "decl-d1",
        0,
        &d1_ids,
        false,
    )?;
    let declared_count = parse_declared_count(&receipt_d1)?;
    ensure!(
        declared_count == 102,
        "declared total after the accepted d1 batch must be 102, got {declared_count}"
    );
    declared_ids.extend(d1_ids);

    let mut next = 103u64;
    let mut refusal_desc = None;
    for round in 0..12 {
        let ids: Vec<u64> = (next..next + 50).collect();
        let tag = format!("decl-funding-{round}");
        match declare_batch(&session, &mut events, context.seed, &tag, 0, &ids, false) {
            Ok((_ok, receipt)) => {
                let declared_count = parse_declared_count(&receipt)?;
                if round == 0 {
                    ensure!(
                        declared_count == 152,
                        "first funding-loop batch must be accepted (declared 152), got {declared_count}"
                    );
                }
                declared_ids.extend(ids);
                next += 50;
            }
            Err(err) => {
                // Refusals surface as clap/journal errors from the client; the
                // refusal evidence is the failed declare's stderr transcript.
                let bad = oracle::operation_hex(oracle::operation_id(context.seed, &tag, 0));
                events.append(
                    "REQUEST_SENT",
                    Some(hex_to_id(&bad)?),
                    None,
                    None,
                    None,
                    None,
                )?;
                let (_output, text) = expect_declare_failure(
                    &session,
                    &artifacts,
                    "declare-funding-refusal.txt",
                    &[
                        "declare",
                        "--operation",
                        &bad,
                        "--entities",
                        &ids.iter().map(u64::to_string).collect::<Vec<_>>().join(","),
                    ],
                )?;
                let named = refusal_named_line(&text, &["LIMIT_EXCEEDED"]);
                ensure!(
                    named.is_some(),
                    "funding-cap refusal for {} entities must name LIMIT_EXCEEDED (4)\n{text}",
                    ids.len()
                );
                events.append("", Some(hex_to_id(&bad)?), None, None, Some(4), None)?;
                refusal_desc = Some(format!(
                    "declare [{}..={}] ({} entities) refuses LIMIT_EXCEEDED: cumulative completion funding cap; actual bound recorded ({} declared before refusal)",
                    ids[0],
                    ids[ids.len() - 1],
                    ids.len(),
                    declared_ids.len()
                ));
                let _ = err;
                break;
            }
        }
    }
    let case_d_refused = refusal_desc.context(
        "no funding-cap refusal observed within 12 loop batches (702 entities declared); the server accepted everything, a deviation from the measured default-storage bound — re-evaluate the loop bound from the observed receipts rather than weakening this assert",
    )?;
    observed.push((
        "case_d_batch_accepted",
        format!(
            "accepted 100 entities; receipt declared={declared_count}; funding loop accepted up to {} declared",
            declared_ids.len()
        ),
    ));
    observed.push(("case_d_batch_refused", case_d_refused.clone()));

    // The rust clap arity (1..=256) pre-empts a 257-entity batch client-side
    // regardless of the server; the wire-level schema bound is unreachable
    // through the rust CLI (named gap; recorded only for the rust client).
    if client == Subject::Rust {
        let e257: Vec<u64> = (2000..=2256).collect();
        let bad257 = oracle::operation_hex(oracle::operation_id(context.seed, "decl-257", 0));
        let (_output, _text) = expect_declare_failure(
            &session,
            &artifacts,
            "declare-257-clap-refusal.txt",
            &[
                "declare",
                "--operation",
                &bad257,
                "--entities",
                &e257
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            ],
        )?;
        observed.push((
            "case_d_clap_arity",
            "rust client refused 257 entities client-side (clap 1..=256); wire unreachable (named gap)".into(),
        ));
    }

    // Expected model, written after the funding loop so the recorded actual
    // bound (not a subject-unreachable schema constant) is the expectation.
    let expected_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &declared_ids);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("case_c_within_batch", "declare [5,3] refuses".into()),
            (
                "case_c_across_batch",
                "declare [1,2] ok; declare [2,3] refuses CONFLICT".into(),
            ),
            (
                "case_d_batch_accepted",
                "declare [3..=102] ok (100 entities); funding loop accepted batches until the authority refused".into(),
            ),
            ("case_d_batch_refused", case_d_refused),
            (
                "case_d_clap_arity",
                "rust client: 257-entity batch refused client-side by clap (1..=256); wire unreachable (named gap)".into(),
            ),
            ("case_a_empty_no_seal", "declare [] refuses".into()),
            (
                "case_b_empty_seal",
                "declare [] --seal succeeds and seals (rust client only; java CLI requires --entities)".into(),
            ),
            ("expected_declared_total", declared_ids.len().to_string()),
            ("expected_seal_sha256", expected_seal.clone()),
            (
                "declaration_alone_admits",
                "false (no execution evidence)".into(),
            ),
        ],
    )?;

    // (a) empty batch without seal refuses. Rust: client-side validation;
    // Java: the CLI cannot express an empty batch at all (--entities is
    // required) — both are named client-side refusals; the wire-level
    // refusal is unreachable (named measurement gap).
    let empty_no_seal = oracle::operation_hex(oracle::operation_id(context.seed, "decl-empty", 0));
    let (output, text) = expect_declare_failure(
        &session,
        &artifacts,
        "declare-empty-no-seal-refusal.txt",
        &["declare", "--operation", &empty_no_seal],
    )?;
    if client == Subject::Rust {
        ensure!(
            text.contains("invalid declaration commitment"),
            "empty-batch refusal must name the validation failure\n{text}"
        );
        observed.push((
            "case_a_empty_no_seal",
            "client-side: invalid declaration commitment".into(),
        ));
    } else {
        let _ = output;
        observed.push((
            "case_a_empty_no_seal",
            "java CLI requires --entities; empty batch not expressible (named gap)".into(),
        ));
    }

    // (b) empty batch WITH seal succeeds and seals. Unreachable on the Java
    // client (same CLI gap), recorded rather than silently skipped.
    let committed;
    let mut membership_verified = "not measured (java CLI cannot seal an empty batch)".to_owned();
    if client == Subject::Rust {
        declare_batch(
            &session,
            &mut events,
            context.seed,
            "decl-seal",
            0,
            &[],
            true,
        )?;
        let (_stdout, page) = observe_page(&session, 0, 256)?;
        ensure!(
            page.declared as usize == declared_ids.len(),
            "sealed scope must declare {} entities, got {}",
            declared_ids.len(),
            page.declared
        );
        committed = page
            .seal
            .clone()
            .context("sealed scope page must carry the seal digest")?;
        ensure!(
            committed == expected_seal,
            "committed seal {committed} != oracle {expected_seal}"
        );
        membership_verified = page.membership_verified.to_string();
        observed.push(("case_b_empty_seal", "accepted; scope sealed".into()));
    } else {
        committed = expected_seal.clone();
        observed.push((
            "case_b_empty_seal",
            "not expressible on the java CLI (--entities required); named gap".into(),
        ));
    }

    // Declaration alone never admits: declared work shows DECLARED (state 0),
    // attempt 0, and no execution-side evidence exists.
    for work in ["0:0:1", "0:0:10"] {
        let stdout = session.watch(work)?;
        let state = parse_state(&stdout)?;
        let attempt = parse_token_u64(
            stdout
                .lines()
                .find(|line| line.starts_with("WORK "))
                .context("watch did not print a WORK line")?,
            "attempt=",
        )?;
        ensure!(
            state == 0 && attempt == 0,
            "declared-not-admitted {work} must stay DECLARED with attempt 0, got state={state} attempt={attempt}\n{stdout}"
        );
    }
    observed.push((
        "declared_not_admitted",
        "state=0 attempt=0 on 0:0:1 and 0:0:10".into(),
    ));
    observed.push(("expected_seal_sha256", expected_seal.clone()));
    observed.push((
        "seal_matches_oracle",
        (committed == expected_seal).to_string(),
    ));
    observed.push(("membership_verified", membership_verified));

    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    detach(&session)?;
    stop_and_seal(context, scenario_dir, scenario_id, session.server, events)
}

// ---------------------------------------------------------------------------
// G1 branch-mode rows, batch B (milestone 9): reassemble/v2 mode 1 and
// chunk-copy/v2 mode 2, no fixture hooks, three directions each
// ---------------------------------------------------------------------------

/// g1-mode1-branch: caller-expanded branch. The parent is admitted to
/// reassemble/v2 mode 1, which allocates a producer-0 child scope named in
/// the watch view (`child=1:0`). The parent cannot complete while the child
/// scope is open; the caller declares and admits the child parts; the
/// application reassembles them byte-exact against the parent input; the
/// session settles bottom-up (child checkpoint, root checkpoint, complete).
fn g1_mode1_branch(context: &ScenarioContext) -> Result<()> {
    run_three_directions(context, "g1-mode1-branch", g1_mode1_branch_direction)
}

fn g1_mode1_branch_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-mode1-branch";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    // The parent input is the expected complete object; reassemble/v2
    // concatenates child output index 0 in increasing entity order and
    // requires the result to match the parent input's length and hash. The
    // two child parts are an uneven split of the same seeded bytes.
    let parent_bytes = oracle::dataset(context.seed, MODE1_PART_ONE_LEN + MODE1_PART_TWO_LEN);
    let part_one = &parent_bytes[..MODE1_PART_ONE_LEN];
    let part_two = &parent_bytes[MODE1_PART_ONE_LEN..];
    let parent_sha256 = oracle::sha256_hex(&parent_bytes);
    // Child scope 1 (producer 0, parent work 0:0:1) sealed over ids [1,2];
    // root scope 0 sealed over [1].
    let child_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 1, 0, Some([0, 0, 1]), &[1, 2]);
    let root_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1]);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("application", "reassemble/v2".into()),
            ("mode", "1".into()),
            ("parent_work", "0:0:1".into()),
            ("parent_input_len", parent_bytes.len().to_string()),
            ("parent_input_sha256", parent_sha256.clone()),
            ("child_scope", "1:0".into()),
            ("child_ids", "1,2".into()),
            ("child_part_lens", format!("{MODE1_PART_ONE_LEN},{MODE1_PART_TWO_LEN}")),
            ("expected_output_sha256", parent_sha256.clone()),
            ("expected_child_seal_sha256", child_seal.clone()),
            ("expected_root_seal_sha256", root_seal.clone()),
            (
                "open_child_completion",
                "complete/checkpoint refuse while the child scope is open (actual codes recorded)".into(),
            ),
            (
                "child_scope_replacement",
                "declare into the sealed child scope refuses CONFLICT (7)".into(),
            ),
            (
                "leaf_children",
                "named gap: neither CLI can attach children to an existing leaf work; children attach only via branch admission".into(),
            ),
            (
                "settlement",
                "child checkpoint, root checkpoint, complete (bottom-up)".into(),
            ),
        ],
    )?;
    let parent_path = artifacts.join("parent-input.bin");
    fs::write(&parent_path, &parent_bytes)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/parent-input.bin".into(),
            len: parent_bytes.len() as u64,
            sha256: parent_sha256.clone(),
        }),
    )?;
    let part_one_path = artifacts.join("child-part-1.bin");
    fs::write(&part_one_path, part_one)?;
    let part_two_path = artifacts.join("child-part-2.bin");
    fs::write(&part_two_path, part_two)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
    ];

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let receipt = admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &parent_path,
        "reassemble/v2",
        1,
        1,
    )?;
    fs::write(artifacts.join("parent-admit-receipt.txt"), &receipt)?;

    // The admission receipt is the operation receipt; the allocated child
    // scope identity is named by the watch view's child= field.
    let view = session.watch("0:0:1")?;
    let child = parse_child_scope(&view)?
        .context("mode-1 admission must allocate a child scope (child=S:P)")?;
    ensure!(
        child == (1, 0),
        "reassemble/v2 must allocate child scope 1:0 (producer 0), got {child:?}"
    );
    let parent_open_state = parse_state(&view)?;
    ensure!(
        parent_open_state != 5,
        "parent must not be terminal before children are supplied (state {parent_open_state})"
    );
    observed.push(("child_scope", "1:0".into()));
    observed.push(("parent_state_children_open", parent_open_state.to_string()));
    fs::write(artifacts.join("parent-admit-watch.txt"), &view)?;

    // Parent completion while the child scope is open: every completion
    // surface must refuse. The child scope row exists (allocated at
    // admission) but is unsealed; the root is sealed but has open
    // obligations. Record the actual named codes.
    let child_checkpoint = expect_failure(
        &session,
        &artifacts,
        "child-checkpoint-open-refusal.txt",
        &["checkpoint", "--scope", "1", "--seal", &"0".repeat(64)],
    )?;
    let named = refusal_named_line(&child_checkpoint, &["NOT_READY", "NOT_FOUND", "CONFLICT"])
        .context("child checkpoint while open must refuse with a named code\n{child_checkpoint}")?;
    observed.push(("child_checkpoint_open", named));

    let complete_open = expect_failure(
        &session,
        &artifacts,
        "complete-open-refusal.txt",
        &["complete"],
    )?;
    let named = refusal_named_line(&complete_open, &["NOT_READY", "CONFLICT", "WAIT_TIMEOUT"])
        .context(
            "complete while the child scope is open must refuse with a named code\n{complete_open}",
        )?;
    observed.push(("complete_open", named));

    // The exact committed root seal is known from the driver's own oracle;
    // checkpointing the sealed-but-open root must not yield coverage.
    let (_stdout, root_page) = observe_page(&session, 0, 256)?;
    let committed_root_seal = root_page
        .seal
        .clone()
        .context("root scope was declared sealed and must carry a seal")?;
    ensure!(
        committed_root_seal == root_seal,
        "committed root seal {committed_root_seal} != oracle {root_seal}"
    );
    let root_checkpoint_open = expect_failure(
        &session,
        &artifacts,
        "root-checkpoint-open-refusal.txt",
        &["checkpoint", "--scope", "0", "--seal", &committed_root_seal],
    )?;
    let named = refusal_named_line(
        &root_checkpoint_open,
        &["NOT_READY", "WAIT_TIMEOUT", "CONFLICT"],
    )
    .context("root checkpoint while the parent is open must refuse with a named code\n{root_checkpoint_open}")?;
    observed.push(("root_checkpoint_open", named));

    // The caller supplies children under the allocated child scope and seals
    // it; a further declare into the sealed scope refuses (the child scope
    // cannot be replaced or grown).
    let (child_declare, _receipt) = declare_scoped_batch(
        &session,
        &mut events,
        context.seed,
        "declare-child",
        0,
        1,
        &[1, 2],
        true,
    )?;
    let replace = oracle::operation_hex(oracle::operation_id(context.seed, "declare-replace", 0));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&replace)?),
        None,
        None,
        None,
        None,
    )?;
    let replace_text = expect_failure(
        &session,
        &artifacts,
        "child-scope-replacement-refusal.txt",
        &[
            "declare",
            "--operation",
            &replace,
            "--scope",
            "1",
            "--entities",
            "3",
            "--seal",
        ],
    )?;
    let named = refusal_named_line(&replace_text, &["CONFLICT"])
        .context("declaring into the sealed child scope must name CONFLICT (7)\n{replace_text}")?;
    events.append("", Some(hex_to_id(&replace)?), None, None, Some(7), None)?;
    observed.push(("child_scope_replacement", named));

    // Admit the two child parts; the parent must stay nonterminal until both
    // children have closed successful.
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit-child-1",
        &child_declare,
        "1:0:1",
        &part_one_path,
    )?;
    watch_terminal(
        &session,
        &mut events,
        "1:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-child-1", 1)),
        WATCH_TIMEOUT,
    )?;
    let mid_state = parse_state(&session.watch("0:0:1")?)?;
    ensure!(
        mid_state != 5,
        "parent must not settle after only one of two children (state {mid_state})"
    );
    observed.push(("parent_state_one_child_terminal", mid_state.to_string()));
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit-child-2",
        &child_declare,
        "1:0:2",
        &part_two_path,
    )?;
    watch_terminal(
        &session,
        &mut events,
        "1:0:2",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-child-2", 1)),
        WATCH_TIMEOUT,
    )?;
    let terminal = watch_terminal(
        &session,
        &mut events,
        "0:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1)),
        WATCH_TIMEOUT,
    )?;
    ensure!(
        parse_state(&terminal)? == 5,
        "reassembled parent must settle terminal success:\n{terminal}"
    );

    // Byte-exact reassembly against the driver's own expected bytes.
    let output_sha256 = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &parent_bytes,
        &parent_sha256,
        &artifacts,
        "parent-output.bin",
    )?;
    observed.push(("output_sha256", output_sha256.clone()));
    observed.push(("output_matches_input", "true".into()));

    // Descendant membership: the child scope page names producer 0, both
    // children terminal, and the seal the driver computed independently.
    let (child_page_text, child_page) = observe_scope_page(&session, 1, 0, 256)?;
    fs::write(artifacts.join("child-scope-page.txt"), &child_page_text)?;
    ensure!(
        child_page.producer == 0,
        "mode-1 child scope producer must be 0, got {}",
        child_page.producer
    );
    ensure!(
        child_page.declared == 2,
        "child scope must declare 2 entities, got {}",
        child_page.declared
    );
    ensure!(
        child_page.members == vec![(1, "SUCCEEDED".into()), (2, "SUCCEEDED".into())],
        "both children must be terminal successful: {:?}",
        child_page.members
    );
    ensure!(
        child_page.membership_verified,
        "sealed child scope page must verify membership"
    );
    let committed_child_seal = child_page
        .seal
        .clone()
        .context("sealed child scope must carry the seal digest")?;
    ensure!(
        committed_child_seal == child_seal,
        "committed child seal {committed_child_seal} != oracle {child_seal}"
    );
    observed.push((
        "child_page",
        "producer=0 declared=2 members SUCCEEDED,SUCCEEDED".into(),
    ));
    observed.push(("child_seal_matches_oracle", "true".into()));

    // Settlement bottom-up: child checkpoint, root checkpoint, complete.
    let child_coverage = session.op(&[
        "checkpoint",
        "--scope",
        "1",
        "--seal",
        &committed_child_seal,
    ])?;
    require(&child_coverage, "COVERAGE", "child checkpoint")?;
    fs::write(
        artifacts.join("child-coverage.txt"),
        String::from_utf8_lossy(&child_coverage.stdout).into_owned(),
    )?;
    let root_coverage =
        session.op(&["checkpoint", "--scope", "0", "--seal", &committed_root_seal])?;
    require(&root_coverage, "COVERAGE", "root checkpoint")?;
    fs::write(
        artifacts.join("root-coverage.txt"),
        String::from_utf8_lossy(&root_coverage.stdout).into_owned(),
    )?;
    let completed = session.op(&["complete"])?;
    require(&completed, "COMPLETED", "complete operation")?;
    fs::write(
        artifacts.join("complete.txt"),
        String::from_utf8_lossy(&completed.stdout).into_owned(),
    )?;
    observed.push((
        "settlement",
        "child COVERAGE, root COVERAGE, COMPLETED".into(),
    ));

    // The settled parent's output stays pinned and byte-exact.
    let pinned_sha256 = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &parent_bytes,
        &parent_sha256,
        &artifacts,
        "parent-output-pinned.bin",
    )?;
    ensure!(
        pinned_sha256 == output_sha256,
        "post-settlement parent output changed: {pinned_sha256} != {output_sha256}"
    );
    observed.push(("post_settlement_output", "VERIFIED byte-exact".into()));
    observed.push((
        "leaf_children",
        "named gap: neither CLI expresses attaching children to an existing leaf; children attach only via branch admission".into(),
    ));

    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    detach(&session)?;
    stop_and_seal(context, scenario_dir, scenario_id, session.server, events)
}

/// g1-mode2-descendants: authority-expanded branch. A 200,000-byte input to
/// chunk-copy/v2 mode 2 becomes four 65,536-byte-class children declared,
/// admitted and settled by the authority itself inside a producer-1 child
/// scope named in the watch view (`child=1:1`). External clients cannot
/// declare or admit into that scope (UNAUTHORIZED, code 3); the parent
/// settles only after every child is terminal; the aggregate output is
/// byte-exact against the driver's own input bytes.
fn g1_mode2_descendants(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g1-mode2-descendants",
        g1_mode2_descendants_direction,
    )
}

fn g1_mode2_descendants_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g1-mode2-descendants";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let session = setup_session(context, scenario_dir, server, client)?;

    let input = oracle::dataset(context.seed, MODE2_INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let last_chunk = (MODE2_INPUT_LEN % MODE2_CHUNK_LEN) as u64;
    let child_ids: Vec<u64> = (1..=MODE2_CHILDREN).collect();
    // Producer-1 child scope 1 (parent work 0:0:1) sealed over ids [1..=4].
    let child_seal =
        oracle::scope_seal_hex("issuer-a", "alice", 1, 1, 1, Some([0, 0, 1]), &child_ids);
    let root_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1]);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("application", "chunk-copy/v2".into()),
            ("mode", "2".into()),
            ("parent_work", "0:0:1".into()),
            ("input_len", MODE2_INPUT_LEN.to_string()),
            ("input_sha256", input_sha256.clone()),
            ("child_scope", "1:1".into()),
            (
                "child_chunk_lens",
                format!("{MODE2_CHUNK_LEN},{MODE2_CHUNK_LEN},{MODE2_CHUNK_LEN},{last_chunk}"),
            ),
            ("expected_output_sha256", input_sha256.clone()),
            ("expected_child_seal_sha256", child_seal.clone()),
            ("expected_root_seal_sha256", root_seal.clone()),
            (
                "external_fencing",
                "external declare into the producer-1 scope refuses UNAUTHORIZED (3) at the wire; external admit is refused by a client-side named guard on both CLIs (rust: NOT_READY covering-declaration guard; java: FRAME_ERROR authority-producer validation) — wire UNAUTHORIZED unreachable (named gap, transcripts recorded)".into(),
            ),
            (
                "parent_settle_order",
                "parent terminal only after every child is terminal (mid-flight samples recorded)"
                    .into(),
            ),
            (
                "child_output_pinning",
                "external child select recorded; named gaps noted from the actual CLI surfaces"
                    .into(),
            ),
        ],
    )?;
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/input.bin".into(),
            len: input.len() as u64,
            sha256: input_sha256.clone(),
        }),
    )?;
    let probe_path = artifacts.join("probe-input.bin");
    fs::write(&probe_path, oracle::dataset(context.seed ^ 0x9e3779b9, 64))?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
    ];

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let receipt = admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
        "chunk-copy/v2",
        2,
        1,
    )?;
    fs::write(artifacts.join("parent-admit-receipt.txt"), &receipt)?;
    let view = session.watch("0:0:1")?;
    let child = parse_child_scope(&view)?
        .context("mode-2 admission must allocate a child scope (child=S:P)")?;
    ensure!(
        child == (1, 1),
        "chunk-copy/v2 must allocate child scope 1:1 (producer 1), got {child:?}"
    );
    observed.push(("child_scope", "1:1".into()));
    fs::write(artifacts.join("parent-admit-watch.txt"), &view)?;

    // External fencing: neither a declare nor an admission into the
    // producer-1 child scope is owned by the external client.
    let probe_declare =
        oracle::operation_hex(oracle::operation_id(context.seed, "probe-declare", 0));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&probe_declare)?),
        None,
        None,
        None,
        None,
    )?;
    let probe_declare_text = expect_failure(
        &session,
        &artifacts,
        "external-child-declare-refusal.txt",
        &[
            "declare",
            "--operation",
            &probe_declare,
            "--scope",
            "1",
            "--entities",
            "9",
            "--seal",
        ],
    )?;
    let named = refusal_named_line(&probe_declare_text, &["UNAUTHORIZED"]).context(
        "external declare into the producer-1 scope must name UNAUTHORIZED (3)\n{probe_declare_text}",
    )?;
    events.append(
        "",
        Some(hex_to_id(&probe_declare)?),
        None,
        None,
        Some(3),
        None,
    )?;
    observed.push(("external_child_declare", named));

    let probe_admit = oracle::operation_hex(oracle::operation_id(context.seed, "probe-admit", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&probe_admit)?),
        Some("1:1:9"),
        Some(1),
        None,
        None,
    )?;
    let probe_admit_text = expect_failure(
        &session,
        &artifacts,
        "external-child-admit-refusal.txt",
        &[
            "admit",
            "--operation",
            &probe_admit,
            "--declaration",
            &declare,
            "--work",
            "1:1:9",
            "--input",
            &crate::path(&probe_path),
            "--application",
            "copy/v2",
        ],
    )?;
    // The wire-level refusal is UNAUTHORIZED (3), but neither published
    // client can express producer-1 targeting to the wire: the rust client
    // pre-empts with its covering-declaration journal guard (NOT_READY) and
    // the java client with `external input uses authority producer`
    // (FRAME_ERROR). Both client-side guards are named refusals recorded
    // here; the wire code 3 event is journaled only when the refusal
    // actually reached the authority. The declare path above is the
    // wire-level UNAUTHORIZED evidence.
    let named = refusal_named_line(&probe_admit_text, &["UNAUTHORIZED", "NOT_READY", "FRAME_ERROR"]).context(
        "external admit into the producer-1 scope must refuse with a named code\n{probe_admit_text}",
    )?;
    if named.starts_with("UNAUTHORIZED") {
        events.append(
            "",
            Some(hex_to_id(&probe_admit)?),
            None,
            None,
            Some(3),
            None,
        )?;
    }
    observed.push(("external_child_admit", named));

    // Wait for the authority expansion to declare and seal the four children.
    let mut expansion = None;
    for _ in 0..300 {
        let (text, page) = observe_scope_page(&session, 1, 0, 256)?;
        if page.declared == MODE2_CHILDREN {
            expansion = Some((text, page));
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let (expansion_text, expansion_page) =
        expansion.context("authority expansion did not declare the four children within 15s")?;
    fs::write(
        artifacts.join("child-scope-page-midflight.txt"),
        &expansion_text,
    )?;
    ensure!(
        expansion_page.producer == 1,
        "mode-2 child scope producer must be 1, got {}",
        expansion_page.producer
    );
    let committed_child_seal = expansion_page
        .seal
        .clone()
        .context("authority expansion must seal the child scope at declaration")?;
    ensure!(
        committed_child_seal == child_seal,
        "committed child seal {committed_child_seal} != oracle {child_seal}"
    );
    observed.push((
        "expansion_declared",
        format!(
            "declared={} seal_matches_oracle=true",
            expansion_page.declared
        ),
    ));

    // Parent settles only after every child is terminal. Sample the children
    // before the parent each round; when the parent is first seen terminal,
    // all children must already be terminal.
    let mut samples = String::new();
    let mut mid_flight = false;
    let mut rounds = 0;
    let terminal = loop {
        rounds += 1;
        let mut child_states = Vec::new();
        let mut children_terminal = true;
        for entity in 1..=MODE2_CHILDREN {
            let stdout = session.watch(&format!("1:1:{entity}"))?;
            let state = parse_state(&stdout)?;
            if state != 5 {
                children_terminal = false;
            }
            child_states.push(state);
        }
        let parent_state = parse_state(&session.watch("0:0:1")?)?;
        samples.push_str(&format!(
            "round={rounds} children={child_states:?} parent={parent_state}\n"
        ));
        if parent_state == 5 {
            ensure!(
                children_terminal,
                "parent settled while children were nonterminal: {child_states:?}"
            );
            break session.watch("0:0:1")?;
        }
        if !children_terminal {
            mid_flight = true;
        }
        ensure!(
            rounds < 600,
            "parent did not settle within the polling window\n{samples}"
        );
        thread::sleep(Duration::from_millis(100));
    };
    fs::write(artifacts.join("settle-samples.txt"), &samples)?;
    ensure!(
        parse_state(&terminal)? == 5,
        "expanded parent must settle terminal success:\n{terminal}"
    );
    observed.push((
        "settle_order",
        format!(
            "parent terminal after all children terminal; mid_flight_sample={mid_flight}; rounds={rounds}"
        ),
    ));

    // Descendant membership after settlement.
    let (child_page_text, child_page) = observe_scope_page(&session, 1, 0, 256)?;
    fs::write(artifacts.join("child-scope-page.txt"), &child_page_text)?;
    let expected_members: Vec<(u64, String)> = child_ids
        .iter()
        .map(|id| (*id, "SUCCEEDED".into()))
        .collect();
    ensure!(
        child_page.producer == 1,
        "mode-2 child scope producer must be 1, got {}",
        child_page.producer
    );
    ensure!(
        child_page.members == expected_members,
        "all four children must be terminal successful: {:?}",
        child_page.members
    );
    ensure!(
        child_page.membership_verified,
        "sealed child scope page must verify membership"
    );
    ensure!(
        child_page.seal.as_deref() == Some(committed_child_seal.as_str()),
        "child scope seal changed after settlement"
    );
    observed.push((
        "child_page",
        "producer=1 declared=4 members SUCCEEDED×4".into(),
    ));

    // Aggregate output byte-exact against the driver's own input bytes.
    let output_sha256 = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output.bin",
    )?;
    observed.push(("output_sha256", output_sha256.clone()));
    observed.push(("output_matches_input", "true".into()));

    // Child outputs pinned through parent settlement: record exactly what the
    // external CLI surface exposes for an authority-side child result.
    let child_select = session.op(&[
        "select",
        "--work",
        "1:1:1",
        "--attempt",
        "1",
        "--index",
        "0",
    ])?;
    let child_select_text = transcript(&child_select);
    fs::write(
        artifacts.join("external-child-select.txt"),
        &child_select_text,
    )?;
    if child_select.status.success() {
        require(&child_select, "REFERENCE", "external child select")?;
        observed.push((
            "child_output_select",
            "exit 0: external child select succeeded (REFERENCE retained)".into(),
        ));
    } else {
        let named = refusal_named_line(
            &child_select_text,
            &["NOT_FOUND", "UNAUTHORIZED", "CONFLICT"],
        )
        .unwrap_or_else(|| "refusal without a named code (transcript recorded)".into());
        observed.push(("child_output_select", format!("refused: {named}")));
    }

    // Settlement bottom-up; the parent's output stays pinned afterwards.
    let child_coverage = session.op(&[
        "checkpoint",
        "--scope",
        "1",
        "--seal",
        &committed_child_seal,
    ])?;
    require(&child_coverage, "COVERAGE", "child checkpoint")?;
    fs::write(
        artifacts.join("child-coverage.txt"),
        String::from_utf8_lossy(&child_coverage.stdout).into_owned(),
    )?;
    let (_stdout, root_page) = observe_page(&session, 0, 256)?;
    let committed_root_seal = root_page
        .seal
        .clone()
        .context("root scope was declared sealed and must carry a seal")?;
    ensure!(
        committed_root_seal == root_seal,
        "committed root seal {committed_root_seal} != oracle {root_seal}"
    );
    let root_coverage =
        session.op(&["checkpoint", "--scope", "0", "--seal", &committed_root_seal])?;
    require(&root_coverage, "COVERAGE", "root checkpoint")?;
    fs::write(
        artifacts.join("root-coverage.txt"),
        String::from_utf8_lossy(&root_coverage.stdout).into_owned(),
    )?;
    let completed = session.op(&["complete"])?;
    require(&completed, "COMPLETED", "complete operation")?;
    fs::write(
        artifacts.join("complete.txt"),
        String::from_utf8_lossy(&completed.stdout).into_owned(),
    )?;
    observed.push((
        "settlement",
        "child COVERAGE, root COVERAGE, COMPLETED".into(),
    ));

    let pinned_sha256 = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-pinned.bin",
    )?;
    ensure!(
        pinned_sha256 == output_sha256,
        "post-settlement parent output changed: {pinned_sha256} != {output_sha256}"
    );
    observed.push(("post_settlement_output", "VERIFIED byte-exact".into()));

    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    detach(&session)?;
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

/// Run one cross-implementation direction of a hooked G2 row when a jar is
/// present; a failure is recorded as an INCOMPLETE marker in the direction
/// directory (named `label`) instead of failing the row — the
/// rust-client/rust-server direction is the row evidence.
fn run_hooked_direction(
    context: &ScenarioContext,
    row_id: &str,
    label: &str,
    direction: impl Fn(&ScenarioContext, &Path) -> Result<()>,
) -> Result<()> {
    let Some(jar) = &context.java_jar else {
        return Ok(());
    };
    let direction_dir = context.scenario_dir(row_id).join(label);
    fs::create_dir_all(&direction_dir)?;
    if let Err(error) = direction(context, &direction_dir) {
        fs::write(
            direction_dir.join("INCOMPLETE"),
            format!(
                "{label} direction failed; the row evidence is the rust-client/rust-server \
                 direction. Error:\n{error:#}\njava_jar_sha256={}\n",
                oracle::sha256_hex(&fs::read(jar)?)
            ),
        )?;
        println!("INCOMPLETE {row_id} {label}: {error:#}");
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

/// The hooked rows whose Java-server direction Claude's FixtureMain supports
/// (drop-reply at the session/declaration/admission reply pairs, kill at the
/// execution/publication commit boundaries).
const JAVA_SERVER_HOOKED_ROWS: &[&str] = &[
    "g2-crash-after-create-commit",
    "g2-drop-reply-declaration",
    "g2-drop-reply-admission",
    "g2-kill-after-admission-before-publication",
    "g2-kill-at-publication-commit",
];

/// The process exit code a scheduled server kill produces: 86 on the Rust
/// subject (interface-v1), 137 on Claude's Java FixtureMain
/// (Runtime.halt(137), FixtureMain.java).
fn kill_exit_code(server: Subject) -> i32 {
    match server {
        Subject::Rust => 86,
        Subject::Java => 137,
    }
}

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
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_crash_after_create_commit_direction(
                context,
                direction_dir,
                Subject::Rust,
                Subject::Java,
            )
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g2_crash_after_create_commit_direction(
                context,
                direction_dir,
                Subject::Java,
                Subject::Rust,
            )
        },
    )?;
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
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_crash_before_create_commit_direction(
                context,
                direction_dir,
                Subject::Rust,
                Subject::Java,
            )
        },
    )?;
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
        output.status.code() == Some(kill_exit_code(server)),
        "g2-crash-before-create-commit: the scheduled kill must exit with the subject kill code after the boundary \
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
            ("subject_exit_code", kill_exit_code(server).to_string()),
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
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_drop_reply_declaration_direction(
                context,
                direction_dir,
                Subject::Rust,
                Subject::Java,
            )
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g2_drop_reply_declaration_direction(
                context,
                direction_dir,
                Subject::Java,
                Subject::Rust,
            )
        },
    )?;
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
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_drop_reply_admission_direction(context, direction_dir, Subject::Rust, Subject::Java)
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g2_drop_reply_admission_direction(context, direction_dir, Subject::Java, Subject::Rust)
        },
    )?;
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
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_kill_after_admission_before_publication_direction(
                context,
                direction_dir,
                Subject::Rust,
                Subject::Java,
            )
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g2_kill_after_admission_before_publication_direction(
                context,
                direction_dir,
                Subject::Java,
                Subject::Rust,
            )
        },
    )?;
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
        output.status.code() == Some(kill_exit_code(server)),
        "g2-kill-after-admission-before-publication: the scheduled kill must exit with the subject kill code after \
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
            ("subject_exit_code", kill_exit_code(server).to_string()),
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
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_kill_at_publication_commit_direction(
                context,
                direction_dir,
                Subject::Rust,
                Subject::Java,
            )
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g2_kill_at_publication_commit_direction(
                context,
                direction_dir,
                Subject::Java,
                Subject::Rust,
            )
        },
    )?;
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
        output.status.code() == Some(kill_exit_code(server)),
        "g2-kill-at-publication-commit: the scheduled kill must exit with the subject kill code after the boundary \
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
            ("subject_exit_code", kill_exit_code(server).to_string()),
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
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_kill_client_after_request_sent_direction(
                context,
                direction_dir,
                Subject::Rust,
                Subject::Java,
            )
        },
    )?;
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

// ---------------------------------------------------------------------------
// G3 storage rows batch A (milestone 10)
// ---------------------------------------------------------------------------

/// The three batch-A rows that run every direction when a jar is provided.
/// g3-store-ownership lists its own coverage: it is a per-server row, so the
/// java-client direction is a named gap rather than a third run.
const G3_BATCH_A_ROWS: &[&str] = &[
    "g3-input-before-metadata",
    "g3-orphan-cleanup",
    "g3-restart-same-roots",
];

/// Object-directory metrics for the private-storage supplementary probes:
/// (file count, content bytes, allocated 512-byte blocks). The walk never
/// mutates the store; every probe is recorded next to the black-box network
/// observation it pairs with, labelled `scope=private-storage-supplementary`.
fn storage_metrics(object_dir: &Path) -> Result<(u64, u64, u64)> {
    use std::os::unix::fs::MetadataExt;

    let mut files = 0u64;
    let mut bytes = 0u64;
    let mut blocks = 0u64;
    let mut stack = vec![object_dir.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("read object dir {}", directory.display()))?
        {
            let entry = entry?;
            let metadata = entry
                .metadata()
                .with_context(|| format!("stat {}", entry.path().display()))?;
            if metadata.is_dir() {
                stack.push(entry.path());
                continue;
            }
            files += 1;
            bytes += metadata.len();
            blocks += metadata.blocks();
        }
    }
    Ok((files, bytes, blocks))
}

fn metrics_text(metrics: (u64, u64, u64)) -> String {
    format!(
        "files={} bytes={} allocated_blocks={}",
        metrics.0, metrics.1, metrics.2
    )
}

/// g3-input-before-metadata: kill the server after the payload is staged but
/// before the admission commits, then restart. The black-box operation lookup
/// names NOT_FOUND (the admission never committed) and the SAME immutable
/// operation re-admits cleanly through the journaled replay. The armed
/// `:after` window at the staging boundary exists only on the Java subject
/// (INPUT_INSTALLED, emitted between payload staging and the admission
/// commit); the Rust subject deliberately keeps supplementary commit keys
/// (`prepare-input`) unarmable (milestone 5), so the rust-server directions
/// use an uncontrolled SIGKILL mid-input-stream and record that named gap.
/// The armed `admit-input:after` lost-reply arm is g2-drop-reply-admission,
/// referenced here rather than redone. A private-storage supplementary probe
/// (object-dir file count/bytes before and after the re-admission) pairs
/// with the lookup/replay network observation.
fn g3_input_before_metadata(context: &ScenarioContext) -> Result<()> {
    let id = "g3-input-before-metadata";
    g3_input_before_metadata_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g3_input_before_metadata_direction(context, direction_dir, Subject::Java, Subject::Rust)
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g3_input_before_metadata_direction(context, direction_dir, Subject::Rust, Subject::Java)
        },
    )?;
    Ok(())
}

fn g3_input_before_metadata_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g3-input-before-metadata";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = if server == Subject::Java {
        vec![g2_schedule_row(
            context,
            id,
            "INPUT_INSTALLED",
            schedule::Action::Kill,
        )]
    } else {
        Vec::new()
    };
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
    let input = oracle::dataset(context.seed, CLIENT_KILL_INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    let args = admit_op_args(&admit_hex, &declare, &input_path);
    let arg_refs = op_refs(&args);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            (
                "fault_window",
                if server == Subject::Java {
                    "armed kill at INPUT_INSTALLED: payload staged, admission NOT committed".into()
                } else {
                    "uncontrolled SIGKILL mid-input-stream; named gap: the rust hook cannot arm \
                     the supplementary prepare-input commit key (milestone 5)"
                        .into()
                },
            ),
            (
                "lookup_after_restart",
                "named NOT_FOUND (the admission never committed)".into(),
            ),
            (
                "same_op_readmission",
                "journaled replay of the SAME operation id succeeds".into(),
            ),
            (
                "g2_reference",
                "g2-drop-reply-admission covers the armed admit-input:after lost-reply arm".into(),
            ),
            (
                "storage_probe_scope",
                "private-storage-supplementary (object-dir metrics; paired with the \
                 lookup/replay network observation)"
                    .into(),
            ),
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
    let attempted = if server == Subject::Java {
        // The armed kill halts the server between payload staging and the
        // admission commit. Drive the admit as a spawned op, wait for the
        // boundary record and the subject's own exit, then retire the
        // stranded transport process (the fault under test is the server
        // death, never the client's reply wait).
        let mut child = session.fixture.spawn_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &session.connection,
            &arg_refs,
        )?;
        wait_subject_record(&events_path, "INPUT_INSTALLED", KILL_TIMEOUT)
            .context("subject never reached the armed INPUT_INSTALLED boundary")?;
        let output = session.server.wait_exit(KILL_TIMEOUT)?;
        ensure!(
            output.status.code() == Some(kill_exit_code(server)),
            "g3-input-before-metadata: the scheduled kill must exit {} after the boundary \
             record, got {}",
            kill_exit_code(server),
            output.status
        );
        child.kill().context("SIGKILL the stranded client op")?;
        let killed = AuthorityFixture::wait_client_op(child, OP_WAIT)?;
        ensure!(
            !killed.status.success(),
            "g3-input-before-metadata: the interrupted client op must not exit zero:\n{}",
            transcript(&killed)
        );
        killed
    } else {
        let mut child = session.fixture.spawn_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &session.connection,
            &arg_refs,
        )?;
        let delay_ms = 10 + (context.seed % 23);
        thread::sleep(Duration::from_millis(delay_ms));
        ensure!(
            child.try_wait()?.is_none(),
            "g3-input-before-metadata: the admit op finished in {delay_ms}ms before the kill; \
             the input is not genuinely in flight"
        );
        session
            .server
            .kill()
            .context("SIGKILL the server mid-input-stream")?;
        // The fault under test is the server death. A client whose upload
        // fits the transport buffers before the kill can outlive the server
        // blocked on the lost reply; that stranded transport process is
        // killed too, never reaped as if it were subject evidence.
        child.kill().context("SIGKILL the stranded client op")?;
        let killed = AuthorityFixture::wait_client_op(child, OP_WAIT)?;
        ensure!(
            !killed.status.success(),
            "g3-input-before-metadata: the interrupted client op must not exit zero:\n{}",
            transcript(&killed)
        );
        killed
    };
    let attempted_text = transcript(&attempted);
    fs::write(artifacts.join("admit-interrupted.txt"), &attempted_text)?;
    events.append("", None, Some("0:0:1"), Some(1), None, None)?;

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

    // Black-box observation: the interrupted operation must not resolve.
    let lookup = recovered.op(&["lookup", "--operation", &admit_hex])?;
    let lookup_text = transcript(&lookup);
    fs::write(artifacts.join("lookup-after-restart.txt"), &lookup_text)?;
    ensure!(
        !lookup.status.success(),
        "g3-input-before-metadata: lookup of the interrupted operation must not succeed:\n\
         {lookup_text}"
    );
    let named = refusal_named_line(&String::from_utf8_lossy(&lookup.stderr), &["NOT_FOUND"])
        .with_context(|| {
            format!(
                "g3-input-before-metadata: lookup after the pre-admission kill must name \
                 NOT_FOUND:\n{lookup_text}"
            )
        })?;
    events.append(
        "",
        Some(hex_to_id(&admit_hex)?),
        None,
        None,
        Some(5),
        Some(artifact_ref("lookup-after-restart.txt", &lookup_text)),
    )?;

    // Supplementary storage probe #1: right after the restart, before the
    // re-admission, so the replay's footprint is measured against it.
    let before = storage_metrics(&recovered.fixture.object_dir)?;

    // The SAME immutable operation re-admits: the journaled replay first,
    // then the identical admit arguments when the interrupted op never
    // journaled its intent.
    let mut replay_args = vec![
        "replay".to_owned(),
        "--operation".to_owned(),
        admit_hex.clone(),
        "--input".to_owned(),
        crate::path(&input_path),
    ];
    if client == Subject::Rust {
        replay_args.push("--declaration".into());
        replay_args.push(declare.clone());
    }
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let replay = recovered.op(&op_refs(&replay_args))?;
    let recovery_path =
        if replay.status.success() && String::from_utf8_lossy(&replay.stdout).contains("RECEIPT") {
            "journaled-replay".to_owned()
        } else {
            let replay_text = transcript(&replay);
            fs::write(artifacts.join("replay-fallback.txt"), &replay_text)?;
            let resend = recovered.op(&arg_refs)?;
            require(
                &resend,
                "RECEIPT",
                "re-sent identical admit after the interrupted admission",
            )?;
            "identical-op-resent".to_owned()
        };
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    let terminal = watch_terminal(
        &recovered,
        &mut events,
        "0:0:1",
        &admit_hex,
        RECOVERY_TIMEOUT,
    )?;
    ensure!(
        parse_attempt(&terminal)? == 1,
        "g3-input-before-metadata: the re-admitted work must settle under its original \
         attempt:\n{terminal}"
    );
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

    // Supplementary storage probe #2: the re-admission must not leak payload
    // bytes without bound. The bound covers the committed input object plus
    // the copy/v2 result object of equal length and per-object metadata files.
    let after = storage_metrics(&recovered.fixture.object_dir)?;
    ensure!(
        after.1 <= before.1 + 2 * input.len() as u64 + 64 * 1024,
        "g3-input-before-metadata: object-dir bytes grew without bound across the \
         re-admission: before={} after={}",
        before.1,
        after.1
    );
    detach(&recovered)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            (
                "fault_window",
                if server == Subject::Java {
                    "armed INPUT_INSTALLED kill (payload staged, admission not committed)".into()
                } else {
                    "uncontrolled SIGKILL mid-input-stream (rust staging boundary not armable)"
                        .into()
                },
            ),
            ("subject_exit_code", kill_exit_code(server).to_string()),
            ("lookup_after_restart", named),
            ("same_op_readmission", recovery_path),
            ("terminal_attempt", "1".into()),
            ("output_matches_input", "true".into()),
            (
                "storage_probe_scope",
                "private-storage-supplementary".into(),
            ),
            ("object_dir_after_restart", metrics_text(before)),
            ("object_dir_after_readmission", metrics_text(after)),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, recovered.server, events)
}

/// g3-orphan-cleanup: several kill/restart iterations orphan staged payloads
/// (each with a FRESH operation id), then the row verifies the committed
/// objects are untouched (the known-good result re-reads byte-exact after
/// every restart), capacity stays consistent (a subsequent admission
/// succeeds), and records the orphan-reclaim behavior actually observed.
/// Where a subject runs no orphan cleanup on restart, the row records that
/// behavior and whether an operator command exists; it never deletes store
/// files itself. The rust store library exposes `collect_payload_orphans`
/// but `v2 serve` startup does not run it and the v2 CLI has no operator
/// cleanup command; the Java behavior is measured, not assumed.
fn g3_orphan_cleanup(context: &ScenarioContext) -> Result<()> {
    let id = "g3-orphan-cleanup";
    g3_orphan_cleanup_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g3_orphan_cleanup_direction(context, direction_dir, Subject::Java, Subject::Rust)
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g3_orphan_cleanup_direction(context, direction_dir, Subject::Rust, Subject::Java)
        },
    )?;
    Ok(())
}

fn g3_orphan_cleanup_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g3-orphan-cleanup";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let armed_rows = |context: &ScenarioContext| {
        vec![g2_schedule_row(
            context,
            id,
            "INPUT_INSTALLED",
            schedule::Action::Kill,
        )]
    };
    // The server starts unarmed: the known-good admission below must commit.
    // Each orphan attempt restarts the server fresh, armed at the staging
    // boundary for the Java subject.
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
    let (mut session, events_path) = split_hooked(hooked);
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let (declare, _receipt) = declare_batch(
        &session,
        &mut events,
        context.seed,
        "declare",
        0,
        &[1, 2, 3, 4],
        true,
    )?;

    // The known-good committed object: re-read byte-exact after every kill.
    let good_input = oracle::dataset(context.seed, INPUT_LEN);
    let good_sha256 = oracle::sha256_hex(&good_input);
    let good_path = artifacts.join("known-good-input.bin");
    fs::write(&good_path, &good_input)?;
    let good_admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &good_path,
    )?;
    watch_terminal(&session, &mut events, "0:0:1", &good_admit, WATCH_TIMEOUT)?;
    read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &good_input,
        &good_sha256,
        &artifacts,
        "known-good-output.bin",
    )?;
    let baseline = storage_metrics(&session.fixture.object_dir)?;

    let orphan_input = oracle::dataset(context.seed.wrapping_add(1), OVERSIZE_INPUT_LEN);
    let orphan_path = artifacts.join("orphan-input.bin");
    fs::write(&orphan_path, &orphan_input)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("known_good_input_len", good_input.len().to_string()),
            ("known_good_input_sha256", good_sha256.clone()),
            (
                "orphan_fault",
                "kill between payload staging and the admission commit, one FRESH operation \
                 id per iteration"
                    .into(),
            ),
            ("iterations", "2".into()),
            (
                "expected_committed",
                "known-good result byte-exact after every restart".into(),
            ),
            (
                "expected_capacity",
                "subsequent admission succeeds after the kills".into(),
            ),
            (
                "expected_reclaim",
                "orphan bytes reclaimed where the subject cleans up on restart; otherwise the \
                 actual behavior and operator-command surface are recorded"
                    .into(),
            ),
            (
                "storage_probe_scope",
                "private-storage-supplementary (object-dir metrics; paired with the \
                 known-good read and liveness admission)"
                    .into(),
            ),
        ],
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        (
            "fault_window",
            "kill between payload staging and admission commit".into(),
        ),
        (
            "storage_probe_scope",
            "private-storage-supplementary".into(),
        ),
        ("object_dir_baseline", metrics_text(baseline)),
    ];
    // The initial server retires before the iteration restarts; each fresh
    // process reopens the same roots.
    session
        .server
        .kill()
        .context("retire the initial server before the orphan iterations")?;
    for iteration in 0..2u32 {
        // A fresh server per attempt: armed at the staging boundary for the
        // Java subject (its hook kills exactly at INPUT_INSTALLED), a plain
        // restart for the rust subjects (the kill there is uncontrolled).
        let rows = if server == Subject::Java {
            armed_rows(context)
        } else {
            Vec::new()
        };
        let schedule_name = format!("schedule-iteration-{iteration}.tsv");
        let fixture = session.fixture.clone();
        let sequence = session.sequence;
        let journal = session.journal.clone();
        session = restart_hooked(
            context,
            scenario_dir,
            id,
            &fixture,
            sequence,
            &journal,
            &rows,
            &schedule_name,
            true,
        )?;
        let work = format!("0:0:{}", 2 + iteration);
        let domain = format!("orphan-{iteration}");
        let op_hex = oracle::operation_hex(oracle::operation_id(context.seed, &domain, 1));
        let args = vec![
            "admit".to_owned(),
            "--operation".to_owned(),
            op_hex.clone(),
            "--declaration".to_owned(),
            declare.clone(),
            "--work".to_owned(),
            work.clone(),
            "--input".to_owned(),
            crate::path(&orphan_path),
            "--application".to_owned(),
            "copy/v2".to_owned(),
        ];
        let arg_refs = op_refs(&args);
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&op_hex)?),
            Some(work.as_str()),
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
        if server == Subject::Java {
            wait_subject_record(&events_path, "INPUT_INSTALLED", KILL_TIMEOUT).with_context(
                || {
                    format!(
                        "subject never reached the armed INPUT_INSTALLED boundary (iteration {iteration})"
                    )
                },
            )?;
            let output = session.server.wait_exit(KILL_TIMEOUT)?;
            ensure!(
                output.status.code() == Some(kill_exit_code(server)),
                "g3-orphan-cleanup: the scheduled kill must exit {} after the boundary record, \
                 got {}",
                kill_exit_code(server),
                output.status
            );
        } else {
            // Long enough that genuine payload bytes are staged before the
            // kill (an 8 MiB loopback upload cannot finish inside this
            // window), short enough that the admission never commits.
            let delay_ms = 100 + (context.seed % 300);
            thread::sleep(Duration::from_millis(delay_ms));
            ensure!(
                child.try_wait()?.is_none(),
                "g3-orphan-cleanup: the orphaned admit op finished in {delay_ms}ms before the \
                 kill; the input is not genuinely in flight"
            );
            session
                .server
                .kill()
                .context("SIGKILL the server mid-input-stream")?;
        }
        // The fault under test is the server death; a client whose upload
        // fits the transport buffers can outlive the server blocked on the
        // lost reply, so the stranded transport process is killed too, never
        // reaped as if it were subject evidence.
        child.kill().context("SIGKILL the stranded client op")?;
        let killed = AuthorityFixture::wait_client_op(child, OP_WAIT)?;
        ensure!(
            !killed.status.success(),
            "g3-orphan-cleanup: the interrupted client op must not exit zero:\n{}",
            transcript(&killed)
        );
        events.append("", None, Some(work.as_str()), Some(1), None, None)?;
    }

    // Plain restart after the last kill: allow a cleanup pass to run, then
    // measure, prove capacity with a fresh admission, and re-read the
    // committed object byte-exact.
    let fixture = session.fixture.clone();
    let sequence = session.sequence;
    let journal = session.journal.clone();
    session = restart_hooked(
        context,
        scenario_dir,
        id,
        &fixture,
        sequence,
        &journal,
        &[],
        "schedule-final.tsv",
        true,
    )?;
    let post_restart = storage_metrics(&session.fixture.object_dir)?;
    thread::sleep(Duration::from_millis(500));
    let post_cleanup = storage_metrics(&session.fixture.object_dir)?;
    observed.push(("object_dir_after_final_restart", metrics_text(post_restart)));
    observed.push((
        "object_dir_after_cleanup_window",
        metrics_text(post_cleanup),
    ));
    observed.push((
        "restart_cleanup",
        if post_cleanup.1 < post_restart.1 {
            format!(
                "orphan bytes reclaimed at restart ({} -> {})",
                post_restart.1, post_cleanup.1
            )
        } else {
            format!(
                "no restart-time cleanup observed ({} -> {}); rust operator command: none in \
                 the v2 CLI (collect_payload_orphans is library-only, not run by serve startup)",
                post_restart.1, post_cleanup.1
            )
        },
    ));

    // Capacity consistency: a fresh admission succeeds after all the kills.
    let live_input = oracle::dataset(context.seed, 4096);
    let live_sha256 = oracle::sha256_hex(&live_input);
    let live_path = artifacts.join("liveness-input.bin");
    fs::write(&live_path, &live_input)?;
    let live_admit = oracle::operation_hex(oracle::operation_id(context.seed, "liveness", 1));
    admit_input(
        &session,
        &mut events,
        context.seed,
        "liveness",
        &declare,
        "0:0:4",
        &live_path,
    )?;
    watch_terminal(&session, &mut events, "0:0:4", &live_admit, WATCH_TIMEOUT)?;
    read_output_verified(
        &session,
        &mut events,
        "0:0:4",
        1,
        &live_input,
        &live_sha256,
        &artifacts,
        "liveness-output.bin",
    )?;
    let final_metrics = storage_metrics(&session.fixture.object_dir)?;
    observed.push(("object_dir_final", metrics_text(final_metrics)));
    observed.push((
        "reclaim",
        if final_metrics.1 < post_cleanup.1 {
            format!(
                "orphan bytes reclaimed during subsequent store operations ({} -> {})",
                post_cleanup.1, final_metrics.1
            )
        } else {
            format!(
                "orphan bytes retained across subsequent admissions ({} -> {})",
                post_cleanup.1, final_metrics.1
            )
        },
    ));
    observed.push((
        "capacity_after_kills",
        "liveness admission succeeded".into(),
    ));

    // The committed object is untouched: byte-exact against the oracle.
    read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &good_input,
        &good_sha256,
        &artifacts,
        "known-good-output-after-kills.bin",
    )?;
    observed.push((
        "committed_untouched",
        "known-good result byte-exact after every restart".into(),
    ));
    detach(&session)?;
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// g3-restart-same-roots: build a loaded session (a sealed root declaration
/// with one entity admitted and published in mode 0, a mode-1 branch with a
/// sealed child scope and both children settled, a mode-2 authority expansion
/// in flight, a real fence via the child-scope checkpoint, and one declared
/// but unadmitted entity), kill the server uncontrolled at a seeded delay,
/// and restart against the same roots. Readiness must serve an op
/// immediately, the work views must come back with identical attempts and
/// deadlines (never a fabricated outcome), the sealed scope seals must still
/// match the independent oracle, the in-flight expansion must settle after
/// the restart, and only then does new capacity admit. All three directions.
fn g3_restart_same_roots(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g3-restart-same-roots",
        g3_restart_same_roots_direction,
    )
}

fn g3_restart_same_roots_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g3-restart-same-roots";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let session = setup_session(context, scenario_dir, server, client)?;
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    // Sealed root over four entities: 0:0:1 publishes before the kill, 0:0:2
    // runs the mode-1 branch, 0:0:3 the mode-2 expansion, and 0:0:4 stays
    // declared-but-unadmitted for the post-restart admission.
    let root_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1, 2, 3, 4]);
    let child_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 1, 0, Some([0, 0, 2]), &[1, 2]);
    let leaf_input = oracle::dataset(context.seed, INPUT_LEN);
    let leaf_sha256 = oracle::sha256_hex(&leaf_input);
    let leaf_path = artifacts.join("leaf-input.bin");
    fs::write(&leaf_path, &leaf_input)?;
    let parent_bytes = oracle::dataset(context.seed, MODE1_PART_ONE_LEN + MODE1_PART_TWO_LEN);
    let parent_sha256 = oracle::sha256_hex(&parent_bytes);
    let parent_path = artifacts.join("parent-input.bin");
    fs::write(&parent_path, &parent_bytes)?;
    let part_one_path = artifacts.join("child-part-1.bin");
    fs::write(&part_one_path, &parent_bytes[..MODE1_PART_ONE_LEN])?;
    let part_two_path = artifacts.join("child-part-2.bin");
    fs::write(&part_two_path, &parent_bytes[MODE1_PART_ONE_LEN..])?;
    let mode2_input = oracle::dataset(context.seed, MODE2_INPUT_LEN);
    let mode2_sha256 = oracle::sha256_hex(&mode2_input);
    let mode2_path = artifacts.join("mode2-input.bin");
    fs::write(&mode2_path, &mode2_input)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("root_seal_sha256", root_seal.clone()),
            ("mode1_child_seal_sha256", child_seal.clone()),
            ("leaf_input_sha256", leaf_sha256.clone()),
            ("mode1_parent_sha256", parent_sha256.clone()),
            ("mode2_input_sha256", mode2_sha256.clone()),
            (
                "loaded_session",
                "mode 0 published; mode 1 child scope sealed, both children settled; mode 2 \
                 expansion in flight; child-scope checkpoint fenced; 0:0:4 declared but unadmitted"
                    .into(),
            ),
            (
                "crash_label",
                "seeded uncontrolled SIGKILL after the fence - process death, not power loss, not a boundary test"
                    .into(),
            ),
            (
                "expected_readiness",
                "an op is served immediately after the restart is ready (recorded)".into(),
            ),
            (
                "expected_views",
                "identical attempts and deadlines across the restart; in-flight expansion settles"
                    .into(),
            ),
            (
                "expected_seals",
                "sealed scope seals still match the oracle after the restart".into(),
            ),
            (
                "expected_new_capacity",
                "0:0:4 (declared before the kill) admits only after reconciliation".into(),
            ),
        ],
    )?;

    let declare = declare_sealed(
        &session,
        &mut events,
        context.seed,
        "declare",
        &[1, 2, 3, 4],
    )?;
    let leaf_admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &leaf_path,
    )?;
    watch_terminal(&session, &mut events, "0:0:1", &leaf_admit, WATCH_TIMEOUT)?;
    let published_sha256 = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &leaf_input,
        &leaf_sha256,
        &artifacts,
        "leaf-output.bin",
    )?;

    // Mode-1 branch: parent admits, child scope 1:0 seals over [1,2], both
    // children settle, the parent follows, and the child-scope checkpoint is
    // the real fence committed before the crash.
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit-m1",
        &declare,
        "0:0:2",
        &parent_path,
        "reassemble/v2",
        1,
        1,
    )?;
    let view = session.watch("0:0:2")?;
    let child = parse_child_scope(&view)?
        .context("mode-1 admission must allocate a child scope (child=S:P)")?;
    ensure!(
        child == (1, 0),
        "g3-restart-same-roots: reassemble/v2 must allocate child scope 1:0, got {child:?}"
    );
    let (child_declare, _receipt) = declare_scoped_batch(
        &session,
        &mut events,
        context.seed,
        "declare-child",
        0,
        1,
        &[1, 2],
        true,
    )?;
    let child_one_admit =
        oracle::operation_hex(oracle::operation_id(context.seed, "admit-child-1", 1));
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit-child-1",
        &child_declare,
        "1:0:1",
        &part_one_path,
    )?;
    watch_terminal(
        &session,
        &mut events,
        "1:0:1",
        &child_one_admit,
        WATCH_TIMEOUT,
    )?;
    let child_two_admit =
        oracle::operation_hex(oracle::operation_id(context.seed, "admit-child-2", 1));
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit-child-2",
        &child_declare,
        "1:0:2",
        &part_two_path,
    )?;
    watch_terminal(
        &session,
        &mut events,
        "1:0:2",
        &child_two_admit,
        WATCH_TIMEOUT,
    )?;
    let mode1_admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit-m1", 1));
    watch_terminal(&session, &mut events, "0:0:2", &mode1_admit, WATCH_TIMEOUT)?;
    let (_stdout, child_page) = observe_scope_page(&session, 1, 0, 256)?;
    ensure!(
        child_page.seal.as_deref() == Some(child_seal.as_str()),
        "g3-restart-same-roots: committed child seal {:?} != oracle {child_seal}",
        child_page.seal
    );
    let fence = session.op(&["checkpoint", "--scope", "1", "--seal", &child_seal])?;
    require(&fence, "COVERAGE", "pre-crash child-scope checkpoint fence")?;
    fs::write(
        artifacts.join("fence-coverage.txt"),
        String::from_utf8_lossy(&fence.stdout).into_owned(),
    )?;

    // Mode-2 authority expansion: admitted last so it is in flight when the
    // seeded kill lands.
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit-m2",
        &declare,
        "0:0:3",
        &mode2_path,
        "chunk-copy/v2",
        2,
        1,
    )?;
    let mode2_view = session.watch("0:0:3")?;
    let mode2_child = parse_child_scope(&mode2_view)?
        .context("mode-2 admission must allocate a child scope (child=S:P)")?;

    // Pre-crash durable observations: per-work views and the sealed pages.
    let pre_views = [
        ("0:0:1", session.watch("0:0:1")?),
        ("0:0:2", session.watch("0:0:2")?),
        ("0:0:3", mode2_view),
    ];
    let mut pre = Vec::new();
    for (work, view) in &pre_views {
        // Terminal views retire the deadline (None); in-flight views carry
        // it. Both must come back identical after the restart.
        pre.push((
            *work,
            parse_state(view)?,
            parse_attempt(view)?,
            parse_field_u64(view, "deadline")?,
        ));
    }
    let (_stdout, root_page) = observe_page(&session, 0, 256)?;
    let committed_root_seal = root_page
        .seal
        .clone()
        .context("sealed root scope must carry the seal digest")?;
    ensure!(
        committed_root_seal == root_seal,
        "g3-restart-same-roots: committed root seal {committed_root_seal} != oracle {root_seal}"
    );
    let delay_ms = 20 + (context.seed % 481);
    thread::sleep(Duration::from_millis(delay_ms));
    session.server.kill()?;
    events.append("", None, Some("0:0:3"), Some(1), None, None)?;

    // Same roots, fresh process. A live process plus ready file plus one
    // authenticated op (inside start_server) is the only readiness accepted;
    // the first watch below is the recorded immediate-op probe.
    let restarted_process = session.fixture.start_server()?;
    let connection = session
        .fixture
        .connection_args(&restarted_process, "alice")?;
    let recovered = Session {
        fixture: session.fixture,
        connection,
        server: restarted_process,
        sequence: session.sequence,
        journal: session.journal,
    };

    let mut observed: Vec<(&str, String)> = Vec::new();

    // Readiness probe: the first op right after ready is served.
    let first_watch = recovered.watch("0:0:1")?;
    fs::write(
        artifacts.join("first-watch-after-restart.txt"),
        &first_watch,
    )?;
    let first_state = parse_state(&first_watch)?;
    let first_attempt = parse_attempt(&first_watch)?;
    let first_deadline = parse_field_u64(&first_watch, "deadline")?;
    let pre_leaf = pre[0];
    ensure!(
        first_state == pre_leaf.1 && first_attempt == pre_leaf.2 && first_deadline == pre_leaf.3,
        "g3-restart-same-roots: the published work must come back intact immediately after \
         the restart: expected state={} attempt={} deadline={:?}, got state={first_state} \
         attempt={first_attempt} deadline={first_deadline:?}",
        pre_leaf.1,
        pre_leaf.2,
        pre_leaf.3
    );

    // Work views intact: identical attempts and deadlines per work; the
    // in-flight mode-2 expansion settles under its original attempt.
    for (work, pre_state, pre_attempt, pre_deadline) in &pre {
        let settle_deadline = Instant::now() + RECOVERY_TIMEOUT;
        let mut view = recovered.watch(work)?;
        let mut state = parse_state(&view)?;
        let mut attempt = parse_attempt(&view)?;
        let mut deadline_ms = parse_field_u64(&view, "deadline")?;
        while state != 5 {
            ensure!(
                state != 6,
                "g3-restart-same-roots: restart fabricated a failure outcome for {work}: {view}"
            );
            ensure!(
                attempt == *pre_attempt,
                "g3-restart-same-roots: restart created a new wire attempt for {work}: {view}"
            );
            if let Some(pre_deadline) = pre_deadline {
                ensure!(
                    deadline_ms == Some(*pre_deadline),
                    "g3-restart-same-roots: {work} changed its deadline while nonterminal: \
                     expected {pre_deadline}, got {deadline_ms:?}:\n{view}"
                );
            }
            ensure!(
                Instant::now() < settle_deadline,
                "g3-restart-same-roots: {work} did not settle within {RECOVERY_TIMEOUT:?} \
                 after the restart\nlast view:\n{view}"
            );
            thread::sleep(Duration::from_millis(200));
            view = recovered.watch(work)?;
            state = parse_state(&view)?;
            attempt = parse_attempt(&view)?;
            deadline_ms = parse_field_u64(&view, "deadline")?;
        }
        ensure!(
            attempt == *pre_attempt,
            "g3-restart-same-roots: {work} changed its attempt across the restart: expected \
             {pre_attempt}, got {attempt}:\n{view}"
        );
        if let (Some(pre_deadline), Some(deadline_ms)) = (pre_deadline, deadline_ms) {
            ensure!(
                deadline_ms == *pre_deadline,
                "g3-restart-same-roots: {work} changed its deadline across the restart: \
                 expected {pre_deadline}, got {deadline_ms}"
            );
        }
        observed.push((
            Box::leak(format!("view_{work}").into_boxed_str()),
            format!(
                "state=5 attempt={attempt} deadline={deadline_ms:?} (pre-crash state={pre_state})"
            ),
        ));
    }

    // Sealed scope seals still match the independent oracle after the crash.
    let (_stdout, root_page) = observe_page(&recovered, 0, 256)?;
    ensure!(
        root_page.seal.as_deref() == Some(root_seal.as_str()),
        "g3-restart-same-roots: root seal after restart {:?} != oracle {root_seal}",
        root_page.seal
    );
    let (_stdout, child_page) = observe_scope_page(&recovered, 1, 0, 256)?;
    ensure!(
        child_page.seal.as_deref() == Some(child_seal.as_str()),
        "g3-restart-same-roots: child seal after restart {:?} != oracle {child_seal}",
        child_page.seal
    );
    observed.push(("root_seal_matches_oracle_after_restart", "true".into()));
    observed.push(("child_seal_matches_oracle_after_restart", "true".into()));

    // Publications stay pinned and byte-exact across the restart.
    let republished = read_output_verified(
        &recovered,
        &mut events,
        "0:0:1",
        1,
        &leaf_input,
        &leaf_sha256,
        &artifacts,
        "leaf-output-after-restart.bin",
    )?;
    ensure!(
        republished == published_sha256,
        "g3-restart-same-roots: the published output changed across the restart"
    );
    read_output_verified(
        &recovered,
        &mut events,
        "0:0:2",
        1,
        &parent_bytes,
        &parent_sha256,
        &artifacts,
        "parent-output-after-restart.bin",
    )?;
    read_output_verified(
        &recovered,
        &mut events,
        "0:0:3",
        1,
        &mode2_input,
        &mode2_sha256,
        &artifacts,
        "mode2-output-after-restart.bin",
    )?;

    // New capacity: the entity declared before the kill admits only after
    // reconciliation has settled the retained work.
    let late_input = oracle::dataset(context.seed, 8192);
    let late_sha256 = oracle::sha256_hex(&late_input);
    let late_path = artifacts.join("late-input.bin");
    fs::write(&late_path, &late_input)?;
    let late_admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit-late", 1));
    admit_input(
        &recovered,
        &mut events,
        context.seed,
        "admit-late",
        &declare,
        "0:0:4",
        &late_path,
    )?;
    watch_terminal(&recovered, &mut events, "0:0:4", &late_admit, WATCH_TIMEOUT)?;
    read_output_verified(
        &recovered,
        &mut events,
        "0:0:4",
        1,
        &late_input,
        &late_sha256,
        &artifacts,
        "late-output.bin",
    )?;
    observed.push((
        "new_capacity_after_reconcile",
        "0:0:4 admitted and settled".into(),
    ));

    // Settlement of the expansion's child scope, then the root, then
    // complete. Every child's terminal work view must be materialized by
    // watching it (g1-mode2-descendants settles the same way) before the
    // scope checkpoint yields coverage.
    let (mode2_page_text, mode2_page) = observe_scope_page(&recovered, mode2_child.0, 0, 256)?;
    fs::write(artifacts.join("mode2-scope-page.txt"), &mode2_page_text)?;
    for (entity, _state) in &mode2_page.members {
        let child_work = format!("{}:{}:{}", mode2_child.0, mode2_child.1, entity);
        let settle_deadline = Instant::now() + WATCH_TIMEOUT;
        loop {
            let view = recovered.watch(&child_work)?;
            let state = parse_state(&view)?;
            if state == 5 {
                break;
            }
            ensure!(
                state != 6,
                "g3-restart-same-roots: expanded child {child_work} fabricated a failure \
                 outcome: {view}"
            );
            ensure!(
                Instant::now() < settle_deadline,
                "g3-restart-same-roots: expanded child {child_work} did not settle within \
                 {WATCH_TIMEOUT:?}\nlast view:\n{view}"
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
    let mode2_child_seal = mode2_page
        .seal
        .clone()
        .context("settled mode-2 child scope must carry the seal digest")?;
    let mode2_checkpoint = recovered.op(&[
        "checkpoint",
        "--scope",
        &mode2_child.0.to_string(),
        "--seal",
        &mode2_child_seal,
    ])?;
    require(&mode2_checkpoint, "COVERAGE", "mode-2 child checkpoint")?;
    let root_checkpoint = recovered.op(&["checkpoint", "--scope", "0", "--seal", &root_seal])?;
    require(&root_checkpoint, "COVERAGE", "root checkpoint")?;
    let completed = recovered.op(&["complete"])?;
    require(&completed, "COMPLETED", "complete operation")?;
    observed.push((
        "settlement",
        "mode-2 child COVERAGE, root COVERAGE, COMPLETED".into(),
    ));

    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .context("a Java direction requires --java-jar")?;
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    detach(&recovered)?;
    stop_and_seal(context, scenario_dir, id, recovered.server, events)
}

/// g3-store-ownership: a live server owns its roots exclusively. A second
/// server process against the SAME roots must fail its startup (the exact
/// exit and message are recorded); the first server keeps serving; after a
/// graceful stop the second starts cleanly against the retained roots and
/// serves the retained session. Per-server row: the rust client runs against
/// both server subjects; the java-client direction is a named gap (the client
/// subject never owns the store). The duplicate-startup capture is a
/// private-storage supplementary probe paired with the black-box "first
/// server unaffected" observation.
fn g3_store_ownership(context: &ScenarioContext) -> Result<()> {
    let id = "g3-store-ownership";
    g3_store_ownership_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(
        context,
        id,
        "rust-client-java-server",
        |context, direction_dir| {
            g3_store_ownership_direction(context, direction_dir, Subject::Java, Subject::Rust)
        },
    )?;
    let gap_dir = context.scenario_dir(id).join("java-client-rust-server");
    fs::create_dir_all(&gap_dir)?;
    fs::write(
        gap_dir.join("INCOMPLETE"),
        b"named gap: g3-store-ownership is a per-server storage row; the \
          java-client/rust-server direction differs only in the client subject, which never \
          owns the store. Primary directions: rust-client on both server subjects.\n",
    )?;
    Ok(())
}

/// Spawn a second server against the same roots while the first is live and
/// capture its startup refusal: (exit status text, server log text). An
/// unexpected readiness or a hang is a row failure, never a silent skip.
fn g3_duplicate_server_probe(fixture: &AuthorityFixture) -> Result<(String, String)> {
    let serial = unique_suffix();
    let ready = fixture.root.join(format!("ready-dup-{serial:x}"));
    let log_path = fixture.root.join(format!("server-dup-{serial:x}.log"));
    let mut command = fixture.base()?;
    command.push("serve".into());
    command.extend(fixture.storage_args());
    command.extend([
        "--bind".into(),
        "127.0.0.1:0".into(),
        "--cert".into(),
        crate::path(&fixture.certs.server.cert),
        "--key".into(),
        crate::path(&fixture.certs.server.key),
        "--client-ca".into(),
        crate::path(&fixture.certs.ca_cert),
        "--result-authority".into(),
        "localhost:7443".into(),
        "--ready-file".into(),
        crate::path(&ready),
    ]);
    let log_file = File::create(&log_path)?;
    let mut child = Command::new(&command[0])
        .args(&command[1..])
        .current_dir(&fixture.root)
        .stdout(Stdio::from(log_file.try_clone()?))
        .stderr(Stdio::from(log_file))
        .spawn()
        .with_context(|| format!("start duplicate {}", command.join(" ")))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(text) = fs::read_to_string(&ready)
            && text.trim().parse::<std::net::SocketAddr>().is_ok()
        {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "the duplicate server became ready against live roots; expected an ownership \
                 refusal\nlog:\n{}",
                fs::read_to_string(&log_path).unwrap_or_default()
            );
        }
        if let Some(status) = child.try_wait()? {
            ensure!(
                !status.success(),
                "the duplicate server exited zero against live roots; expected an ownership \
                 refusal\nlog:\n{}",
                fs::read_to_string(&log_path).unwrap_or_default()
            );
            return Ok((
                status.to_string(),
                fs::read_to_string(&log_path).unwrap_or_default(),
            ));
        }
        ensure!(
            Instant::now() < deadline,
            "the duplicate server neither refused nor became ready within 30s\nlog:\n{}",
            fs::read_to_string(&log_path).unwrap_or_default()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn g3_store_ownership_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g3-store-ownership";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let session = setup_session(context, scenario_dir, server, client)?;
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
    )?;
    watch_terminal(&session, &mut events, "0:0:1", &admit_hex, WATCH_TIMEOUT)?;
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
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("live_server", server.name().into()),
            (
                "expected_duplicate",
                "second server against the same roots refuses startup (exact exit recorded)".into(),
            ),
            (
                "expected_first_unaffected",
                "subsequent op on the first server succeeds".into(),
            ),
            (
                "expected_after_stop",
                "after SIGTERM on the first, the second starts cleanly and serves the retained \
                 session"
                    .into(),
            ),
            (
                "storage_probe_scope",
                "private-storage-supplementary (duplicate-server startup capture; paired with \
                 the first-server op observation)"
                    .into(),
            ),
        ],
    )?;

    // Supplementary probe: the duplicate startup refusal.
    let (dup_status, dup_log) = g3_duplicate_server_probe(&session.fixture)?;
    let dup_text = format!("exit={dup_status}\nlog:\n{dup_log}");
    fs::write(artifacts.join("duplicate-server-refusal.txt"), &dup_text)?;

    // Black-box pairing: the first server is unaffected.
    let view = session.watch("0:0:1")?;
    ensure!(
        parse_state(&view)? == 5,
        "g3-store-ownership: the first server must keep serving after the duplicate refusal:\n\
         {view}"
    );
    events.append(
        "OBSERVATION_JOURNALED",
        Some(hex_to_id(&admit_hex)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    // Graceful stop of the owner, then the duplicate starts cleanly.
    session.server.stop()?;
    let second = session.fixture.start_server()?;
    let connection = session.fixture.connection_args(&second, "alice")?;
    let retained = Session {
        fixture: session.fixture,
        connection,
        server: second,
        sequence: session.sequence,
        journal: session.journal,
    };
    let retained_view = retained.watch("0:0:1")?;
    ensure!(
        parse_state(&retained_view)? == 5,
        "g3-store-ownership: the retained session must be served by the second server:\n\
         {retained_view}"
    );
    read_output_verified(
        &retained,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-second-server.bin",
    )?;
    let lookup = retained.op(&["lookup", "--operation", &admit_hex])?;
    require(
        &lookup,
        "RECEIPT",
        "retained operation lookup on the second server",
    )?;
    detach(&retained)?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("live_server", server.name().into()),
            ("duplicate_startup", format!("refused: {dup_status}")),
            (
                "duplicate_refusal_log",
                dup_log.lines().next().unwrap_or("").to_owned(),
            ),
            (
                "storage_probe_scope",
                "private-storage-supplementary".into(),
            ),
            ("first_server_unaffected", "watch state=5".into()),
            (
                "second_server_after_stop",
                "started cleanly; retained session served".into(),
            ),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, retained.server, events)
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
            "g1-empty-input",
            "g1-zero-output",
            "g1-oversize-payload",
            "g1-out-of-order-pages",
            "g1-declaration-capacity",
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
            "g3-input-before-metadata",
            "g3-orphan-cleanup",
            "g3-restart-same-roots",
            "g3-store-ownership",
            "g5-untrusted-identity",
            "g5-missing-client-cert",
            "g5-unmapped-principal",
            "g5-foreign-owner",
            "g5-no-existence-disclosure",
        ] {
            let row = rows.iter().find(|row| row.id == id).unwrap();
            assert!(row.rust_implemented, "{id} must be implemented");
        }
        assert_eq!(rows.iter().filter(|row| row.rust_implemented).count(), 27);
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
