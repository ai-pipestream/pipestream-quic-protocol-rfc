//! Scenario matrix and the milestone-1/2 rows (rust client against rust
//! server, driven purely through spawned V2 CLI binaries).

use crate::durable::events::{ArtifactRef, EventWriter};
use crate::durable::mtls;
use crate::durable::oracle;
use crate::durable::process::{AuthorityFixture, OwnedServer, Subject};
use crate::durable::rawclient::{
    self, CODE_INTEGRITY_ERROR, Close, FRAME_CAPABILITIES, FRAME_DRAIN, FRAME_REFUSAL,
    FRAME_RESULT, FRAME_SCOPE, FRAME_SESSION, FRAME_WORK, Frame, Peer, QUIC_CONTROL_RESET,
    QUIC_EXTENSION_UNSUPPORTED, QUIC_FRAME_ERROR, RawConn,
};
use crate::durable::schedule;
use crate::{hex, unique_suffix};
use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
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
        // `g2-drop-reply-publication` is retired (owner decision 2026-09-13,
        // milestone 20): publication is watch-observed on both subjects, not
        // a correlated reply, so there is no reply to withhold; the kill
        // variant g2-kill-at-publication-commit is the boundary's evidence.
        // The retirement is recorded in scenario-matrix-g2.md and handoff §3k.
        &[
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
            "g3-nonreusable-history",
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
            "g4-revocation-vs-publication",
            "g4-eventual-settlement",
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
            "g6-direction-and-correlation",
            "g6-stream-identity-and-fin",
            "g6-stopped-control-and-transfers",
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
            "g7-no-deadline-extension",
        ],
    );
    push(
        &mut rows,
        "G8",
        &[
            "g8-exact-root-complete",
            "g8-child-cut-conflict",
            "g8-complete-with-pending",
            "g8-detach-drains",
            "g8-half-close-preserves-responses",
            "g8-timeout-no-completion-claim",
        ],
    );
    push(
        &mut rows,
        // Row ids are the CANONICAL matrix names of
        // scenario-matrix-g6-resource.md. Three milestone-16 placeholders
        // (`r-pending-ceiling`, `r-staging-quota`, `r-journal-bounds`) were
        // never implemented under those ids and are retired here; the
        // mapping is recorded in traceability.md.
        "R",
        &[
            "r-capability-manifest",
            "r-connection-ceiling",
            "r-stalled-principal-progress",
            "r-memory-ladder",
            "r-staging-and-journal-bounds",
            "r-network-bytes",
            "r-native-credit",
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
        "r-capability-manifest",
        "r-connection-ceiling",
        "r-stalled-principal-progress",
        "r-memory-ladder",
        "r-staging-and-journal-bounds",
        "r-network-bytes",
        "r-native-credit",
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
        "g2-not-found-in-flight",
        "g3-input-before-metadata",
        "g3-orphan-cleanup",
        "g3-restart-same-roots",
        "g3-store-ownership",
        "g3-terminal-cleanup",
        "g3-partial-retirement",
        "g3-nonreusable-history",
        "g7-receipt-before-output-expiry",
        "g7-output-before-receipt-expiry",
        "g7-no-deadline-extension",
        "g7-read-pin-past-expiry",
        "g7-deadline-queue-time",
        "g7-unsafe-clock-refusal",
        "g7-cleanup-interrupted-refund",
        "g4-publication-vs-cancel",
        "g4-publication-vs-skip",
        "g4-stale-attempt-retry",
        "g4-ancestor-fence-publication",
        "g4-deadline-settlement",
        "g4-revocation-vs-publication",
        "g4-eventual-settlement",
        "g5-untrusted-identity",
        "g5-missing-client-cert",
        "g5-unmapped-principal",
        "g5-foreign-owner",
        "g5-no-existence-disclosure",
        "g5-cert-rotation-same-owner",
        "g5-remapped-owner",
        "g5-cross-authority-reference",
        "g5-expired-identity",
        "g8-exact-root-complete",
        "g8-child-cut-conflict",
        "g8-complete-with-pending",
        "g8-detach-drains",
        "g8-half-close-preserves-responses",
        "g8-timeout-no-completion-claim",
        "g6-canonical-violations",
        "g6-direction-and-correlation",
        "g6-stream-identity-and-fin",
        "g6-stopped-control-and-transfers",
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
    } else if row.id == "g2-not-found-in-flight" && context.java_jar.is_some() {
        "rust-raw-peer+rust-client/rust-server, rust-raw-peer+rust-client/java-server, \
         rust-raw-peer+java-client/rust-server"
            .to_owned()
    } else if row.id == "g7-read-pin-past-expiry" && context.java_jar.is_some() {
        "rust-client+rust-raw-reader/rust-server, rust-client+rust-raw-reader/java-server"
            .to_owned()
    } else if row.id == "g7-deadline-queue-time" && context.java_jar.is_some() {
        "rust-client/rust-server (hook-free load queue, named gap), rust-client/java-server \
         (EXECUTION_CLAIMED pauses)"
            .to_owned()
    } else if row.id == "g3-store-ownership" && context.java_jar.is_some() {
        "rust-client/rust-server, rust-client/java-server".to_owned()
    } else if G3_BATCH_A_ROWS.contains(&row.id) && context.java_jar.is_some() {
        "rust-client/rust-server, rust-client/java-server, java-client/rust-server".to_owned()
    } else if row.id == "g4-revocation-vs-publication" && context.java_jar.is_some() {
        "rust-client/rust-server (rust-client/java-server: named gap — the Java subject \
         exposes no operator revoke command)"
            .to_owned()
    } else if row.id.starts_with("g8-") && context.java_jar.is_some() {
        if row.id == "g8-timeout-no-completion-claim" {
            "rust-client/rust-server (kill variants: lost-reply outcome recording and strict \
             restart-sequence assertions), rust-client/java-server (kill variant)"
                .to_owned()
        } else if row.id == "g8-half-close-preserves-responses" {
            "rust-client/rust-server, rust-client/java-server, java-client/rust-server, \
             raw-probe/rust-server, raw-probe/java-server (wire-level FIN MUST)"
                .to_owned()
        } else {
            "rust-client/rust-server, rust-client/java-server, java-client/rust-server".to_owned()
        }
    } else if row.id.starts_with("g6-") && context.java_jar.is_some() {
        "rust-probe/rust-server, rust-probe/java-server".to_owned()
    } else if row.id.starts_with("r-") && context.java_jar.is_some() {
        if row.id == "r-capability-manifest" {
            "host-capability measurement (no subject direction; informs every R row)".to_owned()
        } else if row.id == "r-connection-ceiling"
            || row.id == "r-memory-ladder"
            || row.id == "r-staging-and-journal-bounds"
            || row.id == "r-network-bytes"
            || row.id == "r-native-credit"
        {
            "rust-raw-client/rust-server, rust-raw-client/java-server".to_owned()
        } else {
            "rust-cli-client(bob)+rust-raw-client(alice)/rust-server, \
             rust-cli-client(bob)+rust-raw-client(alice)/java-server"
                .to_owned()
        }
    } else if (G3_BATCH_B_ROWS.contains(&row.id)
        || G7_EXPIRY_ROWS.contains(&row.id)
        || G4_ROWS.contains(&row.id)
        || row.id.starts_with("g5-"))
        && context.java_jar.is_some()
    {
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
            if let Some(missing) = error.downcast_ref::<MissingCapability>() {
                // The row ran and wrote its evidence; what it lacks is a
                // subject capability, named here rather than "not implemented".
                let message = format!("{} INCOMPLETE: {missing}", row.id);
                return if dev {
                    DirectionOutcome::Incomplete(message)
                } else {
                    DirectionOutcome::Fail(message)
                };
            }
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
        "g2-not-found-in-flight" => g2_not_found_in_flight(context),
        "g5-cert-rotation-same-owner" => g5_cert_rotation_same_owner(context),
        "g5-remapped-owner" => g5_remapped_owner(context),
        "g5-cross-authority-reference" => g5_cross_authority_reference(context),
        "g5-expired-identity" => g5_expired_identity(context),
        "g7-read-pin-past-expiry" => g7_read_pin_past_expiry(context),
        "g7-deadline-queue-time" => g7_deadline_queue_time(context),
        "g7-unsafe-clock-refusal" => g7_unsafe_clock_refusal(context),
        "g7-cleanup-interrupted-refund" => g7_cleanup_interrupted_refund(context),
        "g3-input-before-metadata" => g3_input_before_metadata(context),
        "g3-orphan-cleanup" => g3_orphan_cleanup(context),
        "g3-restart-same-roots" => g3_restart_same_roots(context),
        "g3-store-ownership" => g3_store_ownership(context),
        "g3-terminal-cleanup" => g3_terminal_cleanup(context),
        "g3-partial-retirement" => g3_partial_retirement(context),
        "g3-nonreusable-history" => g3_nonreusable_history(context),
        "g7-receipt-before-output-expiry" => g7_receipt_before_output_expiry(context),
        "g7-output-before-receipt-expiry" => g7_output_before_receipt_expiry(context),
        "g7-no-deadline-extension" => g7_no_deadline_extension(context),
        "g4-publication-vs-cancel" => g4_publication_vs_cancel(context),
        "g4-publication-vs-skip" => g4_publication_vs_skip(context),
        "g4-stale-attempt-retry" => g4_stale_attempt_retry(context),
        "g4-ancestor-fence-publication" => g4_ancestor_fence_publication(context),
        "g4-deadline-settlement" => g4_deadline_settlement(context),
        "g4-revocation-vs-publication" => g4_revocation_vs_publication(context),
        "g4-eventual-settlement" => g4_eventual_settlement(context),
        "g5-untrusted-identity" => g5_untrusted_identity(context),
        "g5-missing-client-cert" => g5_missing_client_cert(context),
        "g5-unmapped-principal" => g5_unmapped_principal(context),
        "g5-foreign-owner" => g5_foreign_owner(context),
        "g5-no-existence-disclosure" => g5_no_existence_disclosure(context),
        "g8-exact-root-complete" => g8_exact_root_complete(context),
        "g8-child-cut-conflict" => g8_child_cut_conflict(context),
        "g8-complete-with-pending" => g8_complete_with_pending(context),
        "g8-detach-drains" => g8_detach_drains(context),
        "g8-half-close-preserves-responses" => g8_half_close_preserves_responses(context),
        "g8-timeout-no-completion-claim" => g8_timeout_no_completion_claim(context),
        "g6-canonical-violations" => g6_canonical_violations(context),
        "g6-direction-and-correlation" => g6_direction_and_correlation(context),
        "g6-stream-identity-and-fin" => g6_stream_identity_and_fin(context),
        "g6-stopped-control-and-transfers" => g6_stopped_control_and_transfers(context),
        "r-capability-manifest" => r_capability_manifest(context),
        "r-connection-ceiling" => r_connection_ceiling(context),
        "r-stalled-principal-progress" => r_stalled_principal_progress(context),
        "r-memory-ladder" => r_memory_ladder(context),
        "r-staging-and-journal-bounds" => r_staging_and_journal_bounds(context),
        "r-network-bytes" => r_network_bytes(context),
        "r-native-credit" => r_native_credit(context),
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
    /// Short-session-policy triple redeclared on every client invocation
    /// (empty for default-policy sessions).
    policy_args: Vec<String>,
}

impl Session {
    fn op(&self, operation: &[&str]) -> Result<Output> {
        self.fixture.run_client_op_with(
            &self.journal,
            "alice",
            self.sequence,
            &self.policy_args,
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
    setup_session_serve_args(context, scenario_dir, server, client, &[])
}

/// `setup_session` whose server process carries extra serve flags. The G4
/// skip rows pass `--allow-skip` (a rust Storage open flag, a java serve
/// flag); every other row starts servers with the fixed argument set.
fn setup_session_serve_args(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
    serve_args: &[&str],
) -> Result<Session> {
    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        client,
    )?
    .with_extra_serve_args(serve_args);
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
        policy_args: Vec::new(),
    })
}

/// Policy flag spelling per client subject. The Java client spells the
/// execution limit `--execution-ms` (ClientCommands.java usage); the Rust
/// client spells it `--max-execution-ms` (server/src/v2.rs ClientJournal).
/// Both take the two retention flags identically.
/// The execution-limit policy flag as each client spells it. A probe that
/// uses the Rust spelling against the Java client changes nothing: the Java
/// option parser ignores unknown keys and the intent keeps the default
/// policy, so a "changed-policy" replay is an identical replay.
fn execution_limit_flag(client: Subject) -> &'static str {
    match client {
        Subject::Rust => "--max-execution-ms",
        Subject::Java => "--execution-ms",
    }
}

fn policy_args(
    client: Subject,
    max_execution_ms: u64,
    output_retention_ms: u64,
    receipt_retention_ms: u64,
) -> Vec<String> {
    let execution_flag = execution_limit_flag(client);
    vec![
        execution_flag.to_owned(),
        max_execution_ms.to_string(),
        "--output-retention-ms".to_owned(),
        output_retention_ms.to_string(),
        "--receipt-retention-ms".to_owned(),
        receipt_retention_ms.to_string(),
    ]
}

/// `setup_session` with an explicit short-session policy [execution-limit-ms,
/// output-retention-ms, receipt-retention-ms] set at client init/create. The
/// triple is redeclared on every op through `Session.policy_args` (the Java
/// client refuses CONFLICT when an op's flags differ from its journal).
fn setup_session_policy(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
    max_execution_ms: u64,
    output_retention_ms: u64,
    receipt_retention_ms: u64,
) -> Result<Session> {
    let policy_args = policy_args(
        client,
        max_execution_ms,
        output_retention_ms,
        receipt_retention_ms,
    );
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
    command.extend(policy_args.iter().cloned());
    let init = crate::run_output_owned(&fixture.root, &command, OP_WAIT)?;
    require(&init, client.client_initialized_marker(), "v2 init-client")?;
    let connection = fixture.connection_args(&server, "alice")?;
    Ok(Session {
        fixture,
        server,
        sequence,
        journal,
        connection,
        policy_args,
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

/// One `u64` field of a printed work view or receipt, in either client's
/// rendering. The Rust CLI prints `Debug` forms: `deadline: Some(Number(N))`
/// in a `WorkView`, `deadline: Number(N)` in an `Outcome::Admitted` receipt,
/// and `deadline: None` for an absent optional. The Java CLI prints a Java
/// record (`Records.WorkView`, fields `work, state, attempt, input,
/// admittedAt, deadline, terminalAt, receiptUntil, outputUntil, child,
/// manifest, diagnostic`), so the same field reads `deadline=N` or
/// `deadline=null`. `field` is always the Rust snake_case name; the Java
/// camelCase spelling is derived from it. Absent or null is `Ok(None)`; a
/// present field that is not decimal is an error.
fn parse_field_u64(stdout: &str, field: &str) -> Result<Option<u64>> {
    for pattern in [
        format!("{field}: Some(Number("),
        format!("{field}: Number("),
    ] {
        if let Some(start) = stdout.find(&pattern) {
            let digits = &stdout[start + pattern.len()..];
            let end = digits
                .find(')')
                .with_context(|| format!("malformed {field} value in watch view"))?;
            return digits[..end]
                .parse::<u64>()
                .map(Some)
                .with_context(|| format!("malformed {field} decimal"));
        }
    }
    let camel = snake_to_camel(field);
    let pattern = format!("{camel}=");
    let mut search = 0;
    while let Some(found) = stdout[search..].find(&pattern) {
        let start = search + found;
        // A field name is preceded by a record opener or a separator, never
        // by another identifier character (`deadline=` must not match inside
        // `xdeadline=`).
        let bounded = start == 0
            || stdout[..start]
                .chars()
                .next_back()
                .is_some_and(|previous| matches!(previous, ' ' | '[' | ','));
        if !bounded {
            search = start + pattern.len();
            continue;
        }
        let value = &stdout[start + pattern.len()..];
        if value.starts_with("null") {
            return Ok(None);
        }
        let end = value
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(value.len());
        return value[..end]
            .parse::<u64>()
            .map(Some)
            .with_context(|| format!("malformed {field} decimal in the Java record form"));
    }
    Ok(None)
}

/// `admitted_at` -> `admittedAt`: the Java record spelling of a Rust field.
fn snake_to_camel(field: &str) -> String {
    let mut camel = String::with_capacity(field.len());
    let mut upper_next = false;
    for ch in field.chars() {
        if ch == '_' {
            upper_next = true;
        } else if upper_next {
            camel.extend(ch.to_uppercase());
            upper_next = false;
        } else {
            camel.push(ch);
        }
    }
    camel
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
    let input_sha256 = oracle::sha256_hex(&input);
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
    let input_sha256 = oracle::sha256_hex(&input);
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
                            "7" => "CANCELLED".to_owned(),
                            "8" => "SKIPPED".to_owned(),
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
    let input_sha256 = oracle::sha256_hex(&input);
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
        policy_args: session.policy_args,
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
    let arming = write_arming(context, scenario_dir, id, schedule_rows, schedule_name)?;
    setup_armed(context, scenario_dir, server, client, arming, probe)
}

/// `setup_hooked` with fixture events but NO schedule: nothing pauses or
/// dies, and a subject that records every reached boundary (the Java
/// FixtureMain) leaves its boundary records in the shared events file.
fn setup_recording(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    server: Subject,
    client: Subject,
) -> Result<Hooked> {
    let arming = crate::durable::process::FixtureArming {
        events: scenario_dir.join("events.tsv"),
        run_id: context.run_id.clone(),
        scenario_id: id.to_owned(),
        schedule: None,
    };
    setup_armed(context, scenario_dir, server, client, Some(arming), true)
}

fn setup_armed(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
    arming: Option<crate::durable::process::FixtureArming>,
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
            policy_args: Vec::new(),
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
        policy_args: Vec::new(),
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
/// The two subjects spell the file differently: the Rust hooks poll
/// `release-<BOUNDARY>` (src/v2/fixture.rs `reply_gate`), Claude's Java
/// FixtureMain polls `release-<target>-<BOUNDARY>` with the fixture target
/// `server` (the interface-v1 reconciliation is still proposed, not landed).
/// Both names are written so a release reaches whichever subject is paused;
/// the pause rows of milestone 19 are the first drivers of this function.
fn write_release(events: &Path, boundary: &str) -> Result<()> {
    for name in release_file_names(boundary) {
        let release = events
            .parent()
            .context("events path has a parent directory")?
            .join(name);
        fs::write(&release, b"released by the neutral driver\n")?;
    }
    Ok(())
}

/// Remove a release written by [`write_release`] so a LATER pause row on the
/// same boundary holds again (the Java hold proceeds as soon as the file
/// exists, so a stale release file would let the next hold through).
fn clear_release(events: &Path, boundary: &str) -> Result<()> {
    for name in release_file_names(boundary) {
        let release = events
            .parent()
            .context("events path has a parent directory")?
            .join(name);
        match fs::remove_file(&release) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// The release file names both subjects poll for one boundary: the Rust
/// spelling first, the Java FixtureMain spelling second.
fn release_file_names(boundary: &str) -> [String; 2] {
    [
        format!("release-{boundary}"),
        format!("release-server-{boundary}"),
    ]
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
    let flag = execution_limit_flag(session.fixture.client);
    init.extend([flag.into(), max_execution_ms.to_string()]);
    crate::run_output_owned(&session.fixture.root, &init, OP_WAIT)?;
    let mut command = session.fixture.client_base()?;
    command.push("client".into());
    command.extend(
        session
            .fixture
            .journal_args(journal, "alice", creation_sequence),
    );
    command.extend([flag.into(), max_execution_ms.to_string()]);
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
        policy_args: session.policy_args,
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
        policy_args: session.policy_args,
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

// ---------------------------------------------------------------------------
// G3 batch B + G7 expiry rows (milestone 11)
// ---------------------------------------------------------------------------

/// The three G3 batch-B rows: short-session-policy retirement rows, two
/// directions each (rust-client/rust-server plus rust-client/java-server).
const G3_BATCH_B_ROWS: &[&str] = &[
    "g3-terminal-cleanup",
    "g3-partial-retirement",
    "g3-nonreusable-history",
];

/// The three G7 rows this milestone implements. Like the batch-B rows they
/// run the canonical direction plus the rust-client/java-server direction;
/// the client subject cannot influence server-side expiry.
const G7_EXPIRY_ROWS: &[&str] = &[
    "g7-receipt-before-output-expiry",
    "g7-output-before-receipt-expiry",
    "g7-no-deadline-extension",
];

/// The seven G4 rows this milestone implements: publication/fence races, the
/// stale-attempt retry, the deadline race, revocation, and eventual
/// settlement. Like the batch-B/expiry rows they run the canonical direction
/// plus rust-client/java-server; g4-revocation-vs-publication is rust/rust
/// only (the Java subject has no operator revoke command — a named gap).
const G4_ROWS: &[&str] = &[
    "g4-publication-vs-cancel",
    "g4-publication-vs-skip",
    "g4-stale-attempt-retry",
    "g4-ancestor-fence-publication",
    "g4-deadline-settlement",
    "g4-revocation-vs-publication",
    "g4-eventual-settlement",
];

/// Retirement surface per subject, recorded in every retirement row:
/// neither CLI exposes an operator retirement command (the Rust `v2` Command
/// enum has none; Java V2Main/ClientCommands have none); both servers retire
/// automatically once the root is closed and every promise expired — the
/// Rust runtime's maintenance loop (quinn v2_authority/runtime.rs,
/// 20 ms interval, batch 32) and the Java RetentionService (1 s poll,
/// DurableHost RetentionLimits(64, 1000)).
const RETIREMENT_MECHANISM: &str = "operator retirement command: named gap on both CLIs; \
                                    both servers retire automatically (rust: v2_authority \
                                    runtime maintenance, 20ms interval; java: \
                                    RetentionService, 1s poll)";

fn utc_now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("host clock precedes the Unix epoch")
        .as_millis() as u64
}

/// Real-elapsed wait until host UTC passes `when_ms` + `margin`
/// (clock_mode=real-short-policy; both subjects run --trust-system-clock).
fn sleep_until_utc(when_ms: u64, margin: Duration) {
    let target = when_ms + margin.as_millis() as u64;
    let now = utc_now_millis();
    if now < target {
        thread::sleep(Duration::from_millis(target - now));
    }
}

/// The VIEW line of a watch stdout (subject-agnostic; both clients print
/// `VIEW <work view>` after the WORK summary line).
fn view_line(stdout: &str) -> Result<&str> {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("VIEW "))
        .context("watch did not print a VIEW line")
}

/// Declare one sealed entity, admit the deterministic copy/v2 input, and
/// watch to terminal success. Writes `input.bin` and returns
/// (declaration, admit hex, terminal watch stdout, receipt).
fn publish_copy_work(
    session: &Session,
    events: &mut EventWriter,
    context: &ScenarioContext,
    artifacts: &Path,
) -> Result<(String, String, String, String)> {
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
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
            sha256: input_sha256,
        }),
    )?;
    let declare = declare_sealed(session, events, context.seed, "declare", &[1])?;
    let admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let receipt = admit_input(
        session,
        events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &artifacts.join("input.bin"),
    )?;
    let terminal = watch_terminal(session, events, "0:0:1", &admit, WATCH_TIMEOUT)?;
    Ok((declare, admit, terminal, receipt))
}

/// A result read that must refuse the NAMED EXPIRED code after output
/// availability passes: never OUTPUT_UNAVAILABLE, never a silent failure.
fn expect_expired_read(
    session: &Session,
    artifacts: &Path,
    name: &str,
    output_name: &str,
) -> Result<String> {
    let output_path = artifacts.join(output_name);
    let text = expect_failure(
        session,
        artifacts,
        name,
        &[
            "read",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
            "--output",
            &crate::path(&output_path),
        ],
    )?;
    let code = transcript_named_code(&text);
    ensure!(
        code == Some(6),
        "{name} must refuse the named EXPIRED (6) code, got {code:?}\n{text}"
    );
    ensure!(
        !text.contains("OUTPUT_UNAVAILABLE"),
        "{name} must never name OUTPUT_UNAVAILABLE for an expired read\n{text}"
    );
    Ok(text)
}

/// Creation probe from a fresh journal carrying an explicit sequence and the
/// session's short policy, via `init-client` + `client binding`. Returns the
/// raw output so the caller can record a named refusal or a successful
/// replay binding; never asserts.
fn creation_probe(session: &Session, journal: &Path, sequence: u64) -> Result<Output> {
    let mut init = session.fixture.client_base()?;
    init.push("init-client".into());
    init.extend(session.fixture.journal_args(journal, "alice", sequence));
    init.extend(session.policy_args.iter().cloned());
    crate::run_output_owned(&session.fixture.root, &init, OP_WAIT)?;
    let mut command = session.fixture.client_base()?;
    command.push("client".into());
    command.extend(session.fixture.journal_args(journal, "alice", sequence));
    command.extend(session.policy_args.iter().cloned());
    command.extend(session.connection.iter().cloned());
    command.push("binding".into());
    crate::run_output_owned(&session.fixture.root, &command, OP_WAIT)
}

/// Root-close a session the way the completion protocol requires: record
/// root coverage with the committed (oracle-verified) root seal, then
/// complete. `complete` refuses NOT_READY without the coverage checkpoint.
fn root_close(session: &Session) -> Result<()> {
    let (_stdout, root_page) = observe_page(session, 0, 256)?;
    let committed = root_page
        .seal
        .clone()
        .context("sealed root scope must carry a committed seal")?;
    let expected = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1]);
    ensure!(
        committed == expected,
        "committed root seal {committed} != oracle {expected}"
    );
    let checkpoint = session.op(&["checkpoint", "--scope", "0", "--seal", &committed])?;
    require(&checkpoint, "COVERAGE", "root checkpoint")?;
    let completed = session.op(&["complete"])?;
    require(&completed, "COMPLETED", "complete operation")?;
    Ok(())
}

/// One retirement-window observation: after a root close, poll the (terminal)
/// work until the session refuses access or host UTC passes `until_ms`.
/// Both servers retire automatically (rust runtime maintenance, Java
/// RetentionService) so a refusal is expected well inside the window; while
/// serving, the terminal view must never change (no re-execution, no
/// deadline drift).
struct RetirementObservation {
    refused: bool,
    code: Option<u32>,
    line: String,
}

fn observe_retirement_window(
    session: &Session,
    work: &str,
    until_ms: u64,
) -> Result<RetirementObservation> {
    let baseline = view_line(&session.watch(work)?)?.to_owned();
    let mut serving_views = 0u64;
    loop {
        let output = session.op(&["watch", "--work", work])?;
        if !output.status.success() {
            let outcome = probe_outcome(&output);
            let (code, line) = outcome
                .refusal
                .unwrap_or((u32::MAX, "unnamed refusal".to_owned()));
            ensure!(
                code != 16,
                "retirement must never surface OUTPUT_UNAVAILABLE (16): {line}"
            );
            return Ok(RetirementObservation {
                refused: true,
                code: Some(code),
                line,
            });
        }
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        ensure!(
            parse_state(&stdout)? == 5 && parse_attempt(&stdout)? == 1,
            "terminal work changed during the retirement window: {stdout}"
        );
        ensure!(
            view_line(&stdout)? == baseline,
            "terminal view drifted during the retirement window:\n\
             baseline: {baseline}\nactual: {stdout}"
        );
        serving_views += 1;
        if utc_now_millis() >= until_ms {
            return Ok(RetirementObservation {
                refused: false,
                code: None,
                line: format!("serving_views={serving_views}"),
            });
        }
        thread::sleep(Duration::from_secs(1));
    }
}

/// Post-retirement probes shared by the retirement rows: operation lookup,
/// creation replay with the retired sequence, and the owner creation
/// high-water. Both servers retire automatically, so every probe must
/// refuse: the session-scoped lookup names EXPIRED (6) or NOT_FOUND (5) —
/// the matrix sanctions NOT_FOUND once metadata removal completes — and the
/// creation replay names EXPIRED (6) ("creation receipt retired"), proving
/// the retired sequence was never reissued. The high-water read answers on
/// both subjects.
fn retired_access_probes(
    session: &Session,
    scenario_dir: &Path,
    artifacts: &Path,
    admit: &str,
) -> Result<Vec<(&'static str, String)>> {
    let mut observed: Vec<(&'static str, String)> = Vec::new();
    let lookup = session.op(&["lookup", "--operation", admit])?;
    let lookup_text = transcript(&lookup);
    fs::write(artifacts.join("retired-lookup.txt"), &lookup_text)?;
    let outcome = probe_outcome(&lookup);
    let (code, line) = outcome.refusal.unwrap_or((u32::MAX, String::new()));
    let success = lookup.status.success();
    ensure!(
        !success && (code == 6 || code == 5),
        "operation lookup on the retired session must refuse a named EXPIRED (6) or \
         NOT_FOUND (5) code, got success={success} code={code:?} line={line}\n{lookup_text}"
    );
    observed.push((
        "retired_operation_lookup",
        format!(
            "refused {} ({code})",
            if code == 6 { "EXPIRED" } else { "NOT_FOUND" }
        ),
    ));

    let replay_journal = scenario_dir.join("client").join("probe-replay.sqlite");
    let replay = creation_probe(session, &replay_journal, 1)?;
    let replay_text = transcript(&replay);
    fs::write(artifacts.join("retired-creation-replay.txt"), &replay_text)?;
    let outcome = probe_outcome(&replay);
    let (code, line) = outcome.refusal.unwrap_or((u32::MAX, String::new()));
    let success = replay.status.success();
    ensure!(
        !success && code == 6,
        "creation replay with the retired sequence must refuse named EXPIRED (6), \
         got success={success} code={code:?} line={line}\n{replay_text}"
    );
    observed.push(("retired_creation_replay", "refused EXPIRED (6)".into()));

    let high_water = session.fixture.next_sequence(&session.server, "alice")?;
    ensure!(
        high_water == 2,
        "owner creation high-water must be preserved at 2 after retirement, got {high_water}"
    );
    observed.push((
        "owner_high_water",
        format!("next-sequence={high_water} (never reissued)"),
    ));
    Ok(observed)
}

/// Both servers retire automatically once the root is closed and every
/// promise expired: an unrefused retirement window is a defect, and the
/// refusal must name EXPIRED (6) or NOT_FOUND (5) — the matrix sanctions
/// NOT_FOUND once metadata removal completes.
fn require_retired(observation: &RetirementObservation, what: &str) -> Result<String> {
    ensure!(
        observation.refused,
        "{what}: session kept serving past the retirement window ({})",
        observation.line
    );
    ensure!(
        observation.code == Some(6) || observation.code == Some(5),
        "{what}: refusal must name EXPIRED (6) or NOT_FOUND (5), got {:?}: {}",
        observation.code,
        observation.line
    );
    Ok(format!("refused: {}", observation.line))
}

/// Run one expiry row: canonical rust/rust direction in the row directory,
/// rust-client/java-server in a subdirectory (INCOMPLETE marker instead of a
/// failure, per the cross-implementation gap policy).
fn run_expiry_row(
    context: &ScenarioContext,
    row_id: &str,
    direction: fn(&ScenarioContext, &Path, Subject, Subject) -> Result<()>,
) -> Result<()> {
    direction(
        context,
        &context.scenario_dir(row_id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(
        context,
        row_id,
        "rust-client-java-server",
        |context, dir| direction(context, dir, Subject::Java, Subject::Rust),
    )?;
    Ok(())
}

/// Shared expiry-row preamble: events, no-fault enforcement, short-policy
/// session, expected.tsv with the policy triple (written BEFORE any wait),
/// and the published copy/v2 work. Returns the pieces every expiry row needs.
#[allow(clippy::too_many_arguments)]
fn expiry_preamble(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    server: Subject,
    client: Subject,
    policy: (u64, u64, u64),
    expected_extra: &[(&str, String)],
) -> Result<(
    Session,
    EventWriter,
    PathBuf,
    String,
    String,
    String,
    String,
)> {
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    let session = setup_session_policy(
        context,
        scenario_dir,
        server,
        client,
        policy.0,
        policy.1,
        policy.2,
    )?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let mut expected: Vec<(&str, String)> = vec![
        ("policy_execution_limit_ms", policy.0.to_string()),
        ("policy_output_retention_ms", policy.1.to_string()),
        ("policy_receipt_retention_ms", policy.2.to_string()),
        ("clock_mode", "real-short-policy".into()),
        ("work", "0:0:1".into()),
        ("attempt", "1".into()),
        ("input_len", INPUT_LEN.to_string()),
        ("input_sha256", input_sha256.clone()),
        ("expected_output_sha256", input_sha256.clone()),
    ];
    expected.extend(
        expected_extra
            .iter()
            .map(|(key, value)| (*key, value.clone())),
    );
    write_kv(scenario_dir, "expected.tsv", &expected)?;
    let (declare, admit, terminal, _receipt) =
        publish_copy_work(&session, &mut events, context, &artifacts)?;
    Ok((
        session,
        events,
        artifacts,
        declare,
        admit,
        terminal,
        input_sha256,
    ))
}

/// Rust-client-only field checks proving the policy triple was honored:
/// deadline = admitted + execution limit; receipt/output until = terminal +
/// retention. Java view rendering differs, so the mixed direction relies on
/// the behavioral expiry timings instead.
fn check_policy_offsets(terminal_view: &str, policy: (u64, u64, u64)) -> Result<()> {
    let admitted_at = parse_field_u64(terminal_view, "admitted_at")?
        .context("terminal view did not report admitted_at")?;
    let deadline = parse_field_u64(terminal_view, "deadline")?
        .context("terminal view did not report a deadline")?;
    let terminal_at = parse_field_u64(terminal_view, "terminal_at")?
        .context("terminal view did not report terminal_at")?;
    let receipt_until = parse_field_u64(terminal_view, "receipt_until")?
        .context("terminal view did not report receipt_until")?;
    let output_until = parse_field_u64(terminal_view, "output_until")?
        .context("terminal view did not report output_until")?;
    ensure!(
        deadline == admitted_at + policy.0,
        "execution deadline {deadline} != admitted_at {admitted_at} + {}ms",
        policy.0
    );
    ensure!(
        receipt_until == terminal_at + policy.2,
        "receipt deadline {receipt_until} != terminal_at {terminal_at} + {}ms",
        policy.2
    );
    ensure!(
        output_until == terminal_at + policy.1,
        "output deadline {output_until} != terminal_at {terminal_at} + {}ms",
        policy.1
    );
    Ok(())
}

/// g7-receipt-before-output-expiry: policy output-retention (5s) <
/// receipt-retention (20s). Publish, read inside availability, then wait
/// past output availability while the receipt still holds: the fresh result
/// read refuses the named EXPIRED (6) code, never OUTPUT_UNAVAILABLE; the
/// manifest and work view stay readable; the terminal outcome never flips
/// (no re-execution on read); the operation lookup still answers.
fn g7_receipt_before_output_expiry(context: &ScenarioContext) -> Result<()> {
    run_expiry_row(
        context,
        "g7-receipt-before-output-expiry",
        g7_receipt_before_output_expiry_direction,
    )
}

fn g7_receipt_before_output_expiry_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g7-receipt-before-output-expiry";
    let policy = (60_000u64, 5_000u64, 20_000u64);
    let (session, mut events, artifacts, _declare, admit, terminal, input_sha256) =
        expiry_preamble(
            context,
            scenario_dir,
            id,
            server,
            client,
            policy,
            &[(
                "expected_read_after_output_expiry",
                "named EXPIRED (6), never OUTPUT_UNAVAILABLE (16)".into(),
            )],
        )?;
    if client == Subject::Rust {
        check_policy_offsets(&terminal, policy)
            .context("g7-receipt-before-output-expiry policy offsets")?;
    }
    let terminal_view = view_line(&terminal)?.to_owned();
    let output_until = parse_field_u64(&terminal, "output_until")?
        .context("terminal view did not report output_until")?;
    let receipt_until = parse_field_u64(&terminal, "receipt_until")?
        .context("terminal view did not report receipt_until")?;
    if client == Subject::Rust {
        ensure!(
            output_until < receipt_until,
            "policy order requires output availability to expire before the receipt"
        );
    }
    let input = oracle::dataset(context.seed, INPUT_LEN);
    read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-before-expiry.bin",
    )?;

    // Wait past output availability (still inside receipt retention).
    sleep_until_utc(output_until, Duration::from_secs(2));
    expect_expired_read(
        &session,
        &artifacts,
        "read-after-output-expiry.txt",
        "expired-read.bin",
    )?;

    // Manifest and work view remain readable; the terminal outcome is frozen.
    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    require(&manifest, "MANIFEST", "manifest after output expiry")?;
    let after_view = session.watch("0:0:1")?;
    ensure!(
        parse_state(&after_view)? == 5 && parse_attempt(&after_view)? == 1,
        "terminal outcome must not flip after output expiry: {after_view}"
    );
    ensure!(
        view_line(&after_view)? == terminal_view,
        "terminal view changed after output expiry (re-execution on read?)"
    );
    let lookup = session.op(&["lookup", "--operation", &admit])?;
    require(&lookup, "RECEIPT", "operation lookup after output expiry")?;
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("clock_mode", "real-short-policy".into()),
        (
            "read_after_output_expiry",
            "named EXPIRED (6), never OUTPUT_UNAVAILABLE (16) \
             (artifacts/read-after-output-expiry.txt)"
                .into(),
        ),
        ("manifest_after_output_expiry", "readable (MANIFEST)".into()),
        (
            "work_view_after_output_expiry",
            "state=5 attempt=1, view unchanged".into(),
        ),
        ("lookup_after_output_expiry", "RECEIPT answers".into()),
        ("receipt_until", receipt_until.to_string()),
    ];
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// g3-terminal-cleanup: the full cleanup sequence. Phase 1 repeats the
/// g7-receipt-before-output-expiry probes (output 5s < receipt 20s). Phase 2
/// root-closes the session (complete) and waits past the receipt retention:
/// automatic retirement then refuses session access with a named code on
/// both servers (EXPIRED (6) while the retiring flag fences access, NOT_FOUND
/// (5) once metadata removal completes). The owner creation high-water is
/// preserved on both subjects.
fn g3_terminal_cleanup(context: &ScenarioContext) -> Result<()> {
    run_expiry_row(
        context,
        "g3-terminal-cleanup",
        g3_terminal_cleanup_direction,
    )
}

fn g3_terminal_cleanup_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g3-terminal-cleanup";
    let policy = (60_000u64, 5_000u64, 20_000u64);
    let (session, mut events, artifacts, _declare, admit, terminal, input_sha256) =
        expiry_preamble(
            context,
            scenario_dir,
            id,
            server,
            client,
            policy,
            &[
                (
                    "expected_read_after_output_expiry",
                    "named EXPIRED (6), never OUTPUT_UNAVAILABLE (16)".into(),
                ),
                (
                    "expected_after_receipt_expiry",
                    "both servers: session retired, access refuses named EXPIRED (6) or \
                     NOT_FOUND (5); creation replay refuses EXPIRED (6)"
                        .into(),
                ),
            ],
        )?;
    if client == Subject::Rust {
        check_policy_offsets(&terminal, policy).context("g3-terminal-cleanup policy offsets")?;
    }
    let terminal_view = view_line(&terminal)?.to_owned();
    let output_until = parse_field_u64(&terminal, "output_until")?
        .context("terminal view did not report output_until")?;
    let receipt_until = parse_field_u64(&terminal, "receipt_until")?
        .context("terminal view did not report receipt_until")?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-before-expiry.bin",
    )?;

    // Phase 1: past output availability, inside receipt retention.
    sleep_until_utc(output_until, Duration::from_secs(2));
    expect_expired_read(
        &session,
        &artifacts,
        "read-after-output-expiry.txt",
        "expired-read.bin",
    )?;
    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    require(&manifest, "MANIFEST", "manifest after output expiry")?;
    let after_view = session.watch("0:0:1")?;
    ensure!(
        parse_state(&after_view)? == 5 && parse_attempt(&after_view)? == 1,
        "terminal outcome must not flip after output expiry: {after_view}"
    );
    ensure!(
        view_line(&after_view)? == terminal_view,
        "terminal view changed after output expiry (re-execution on read?)"
    );
    let lookup = session.op(&["lookup", "--operation", &admit])?;
    require(&lookup, "RECEIPT", "operation lookup after output expiry")?;
    detach(&session)?;

    // Phase 2: root-close, then wait past the receipt retention.
    root_close(&session)?;
    let observation = observe_retirement_window(
        &session,
        "0:0:1",
        receipt_until + Duration::from_secs(15).as_millis() as u64,
    )?;
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("clock_mode", "real-short-policy".into()),
        ("retirement_mechanism", RETIREMENT_MECHANISM.into()),
        (
            "read_after_output_expiry",
            "named EXPIRED (6), never OUTPUT_UNAVAILABLE (16)".into(),
        ),
        ("manifest_after_output_expiry", "readable (MANIFEST)".into()),
        (
            "work_view_after_output_expiry",
            "state=5 attempt=1, view unchanged".into(),
        ),
        ("lookup_after_output_expiry", "RECEIPT answers".into()),
    ];
    observed.push((
        "session_after_receipt_expiry",
        require_retired(
            &observation,
            "g3-terminal-cleanup session after receipt expiry",
        )?,
    ));
    observed.extend(retired_access_probes(
        &session,
        scenario_dir,
        &artifacts,
        &admit,
    )?);
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// g7-output-before-receipt-expiry: the inverse policy (receipt 5s <
/// output 20s); both subjects accept it at create (only upper bounds are
/// validated). After the receipt deadline the operation lookup still answers
/// on both subjects (named gap: neither enforces lazy receipt expiry; the
/// Java server defers retirement until the root closes and every promise
/// resolves), the retained output still reads byte-exact while its own
/// availability holds, identity/digest are retained, and replays never
/// reapply. The row then root-closes and waits out the output promise to
/// observe the retired-session refusals.
fn g7_output_before_receipt_expiry(context: &ScenarioContext) -> Result<()> {
    run_expiry_row(
        context,
        "g7-output-before-receipt-expiry",
        g7_output_before_receipt_expiry_direction,
    )
}

fn g7_output_before_receipt_expiry_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g7-output-before-receipt-expiry";
    let policy = (60_000u64, 20_000u64, 5_000u64);
    let (session, mut events, artifacts, declare, admit, terminal, input_sha256) = expiry_preamble(
        context,
        scenario_dir,
        id,
        server,
        client,
        policy,
        &[
            (
                "policy_accepted_at_create",
                "both subjects (upper-bound validation only)".into(),
            ),
            (
                "expected_lookup_after_receipt_expiry",
                "actual recorded: receipt retained until retirement (named gap, no lazy \
                     receipt-expiry refusal)"
                    .into(),
            ),
            (
                "expected_read_inside_output_availability",
                "byte-exact VERIFIED".into(),
            ),
        ],
    )?;
    if client == Subject::Rust {
        check_policy_offsets(&terminal, policy)
            .context("g7-output-before-receipt-expiry policy offsets")?;
        let receipt_until = parse_field_u64(&terminal, "receipt_until")?
            .context("terminal view did not report receipt_until")?;
        let output_until = parse_field_u64(&terminal, "output_until")?
            .context("terminal view did not report output_until")?;
        ensure!(
            receipt_until < output_until,
            "inverse policy requires the receipt to expire before output availability"
        );
    }
    let terminal_view = view_line(&terminal)?.to_owned();
    let receipt_until = parse_field_u64(&terminal, "receipt_until")?
        .context("terminal view did not report receipt_until")?;
    let output_until = parse_field_u64(&terminal, "output_until")?
        .context("terminal view did not report output_until")?;
    let input = oracle::dataset(context.seed, INPUT_LEN);

    // Sanity read inside both promises.
    read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-before-receipt-expiry.bin",
    )?;

    // After the receipt deadline, inside output availability.
    sleep_until_utc(receipt_until, Duration::from_secs(2));
    let lookup = session.op(&["lookup", "--operation", &admit])?;
    let lookup_ok =
        lookup.status.success() && String::from_utf8_lossy(&lookup.stdout).contains("RECEIPT");
    if !lookup_ok {
        let text = transcript(&lookup);
        fs::write(artifacts.join("lookup-after-receipt-expiry.txt"), &text)?;
        let code = transcript_named_code(&text);
        ensure!(
            code == Some(6),
            "if lookup refuses after the receipt deadline it must name EXPIRED (6), \
             got {code:?}\n{text}"
        );
    }
    let retained_read = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-after-receipt-expiry.bin",
    )?;
    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    require(&manifest, "MANIFEST", "manifest after receipt expiry")?;
    // The replay never reapplies: the scope page still holds exactly one
    // admitted member and the terminal view is unchanged.
    let input_path = crate::path(&artifacts.join("input.bin"));
    let replay_args: Vec<&str> = if client == Subject::Rust {
        vec![
            "replay",
            "--operation",
            &admit,
            "--input",
            &input_path,
            "--declaration",
            &declare,
        ]
    } else {
        vec!["replay", "--operation", &admit, "--input", &input_path]
    };
    let replay = session.op(&replay_args)?;
    let replay_text = transcript(&replay);
    fs::write(
        artifacts.join("replay-after-receipt-expiry.txt"),
        &replay_text,
    )?;
    ensure!(
        replay.status.success() && replay_text.contains("RECEIPT"),
        "admission replay after receipt expiry must not reapply or vanish:\n{replay_text}"
    );
    let page = session.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "scope page after replay")?;
    let (declared, members) = parse_scope_page(&page_stdout)?;
    ensure!(
        declared == 1 && members == 1,
        "replay must never create new work: declared={declared} members={members}:\n{page_stdout}"
    );
    let after_view = session.watch("0:0:1")?;
    ensure!(
        parse_state(&after_view)? == 5 && parse_attempt(&after_view)? == 1,
        "terminal outcome must not flip: {after_view}"
    );
    ensure!(
        view_line(&after_view)? == terminal_view,
        "terminal view changed"
    );
    detach(&session)?;

    // Root-close and wait out the output promise to reach retirement.
    root_close(&session)?;
    let observation = observe_retirement_window(
        &session,
        "0:0:1",
        output_until + Duration::from_secs(15).as_millis() as u64,
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("clock_mode", "real-short-policy".into()),
        ("policy_accepted_at_create", "true (both subjects)".into()),
        (
            "lookup_after_receipt_expiry",
            if lookup_ok {
                "RECEIPT answers (named gap: no lazy receipt-expiry refusal; receipts are \
                 retained until retirement)"
                    .into()
            } else {
                "refused named EXPIRED (6)".into()
            },
        ),
        (
            "output_read_after_receipt_expiry",
            format!("VERIFIED byte-exact sha256={retained_read}"),
        ),
        (
            "manifest_after_receipt_expiry",
            "readable (MANIFEST)".into(),
        ),
        (
            "replay_after_receipt_expiry",
            "RECEIPT idempotent, no new work (page 1/1)".into(),
        ),
        ("retirement_mechanism", RETIREMENT_MECHANISM.into()),
    ];
    observed.push((
        "session_after_output_expiry",
        require_retired(&observation, "g7-output-before-receipt-expiry session")?,
    ));
    observed.extend(retired_access_probes(
        &session,
        scenario_dir,
        &artifacts,
        &admit,
    )?);
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// g7-no-deadline-extension: for one admitted work, reconnect, re-read the
/// manifest, replay the admission operation, and (only if retryable) retry —
/// then compare every deadline field. The explicit retry leg is skipped with
/// a note: the settled copy/v2 work is terminal-success, not retryable. The
/// full rendered view must be byte-identical after every leg and the
/// rust-client direction additionally checks deadline = admitted +
/// execution-ms and receipt/output until = terminal + policy.
fn g7_no_deadline_extension(context: &ScenarioContext) -> Result<()> {
    run_expiry_row(
        context,
        "g7-no-deadline-extension",
        g7_no_deadline_extension_direction,
    )
}

fn g7_no_deadline_extension_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g7-no-deadline-extension";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    let session = setup_session(context, scenario_dir, server, client)?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("execution_limit_ms", "60000".into()),
            ("output_retention_ms", "3600000".into()),
            ("receipt_retention_ms", "86400000".into()),
            ("input_sha256", input_sha256),
            (
                "expected_deadline_relation",
                "deadline = admitted_at + execution_limit; receipt_until/output_until = \
                 terminal_at + retention; identical after every leg"
                    .into(),
            ),
            (
                "retry_leg",
                "skipped: settled copy/v2 work is terminal-success, not retryable".into(),
            ),
        ],
    )?;
    let (declare, admit, terminal, receipt) =
        publish_copy_work(&session, &mut events, context, &artifacts)?;
    let terminal_view = view_line(&terminal)?.to_owned();
    if client == Subject::Rust {
        check_policy_offsets(&terminal, (60_000, 3_600_000, 86_400_000))
            .context("g7-no-deadline-extension policy offsets")?;
    }
    let manifest = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    require(&manifest, "MANIFEST", "manifest operation")?;

    // Leg 1: reconnect (a fresh authenticated binding on a new connection).
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "reconnect binding")?;
    let leg_view = session.watch("0:0:1")?;
    ensure!(
        view_line(&leg_view)? == terminal_view,
        "reconnect changed the work view:\n{leg_view}\nbaseline:\n{terminal_view}"
    );

    // Leg 2: re-read the manifest.
    let reread = session.op(&["manifest", "--work", "0:0:1", "--attempt", "1"])?;
    require(&reread, "MANIFEST", "manifest re-read")?;
    let leg_view = session.watch("0:0:1")?;
    ensure!(
        view_line(&leg_view)? == terminal_view,
        "manifest re-read changed the work view:\n{leg_view}"
    );

    // Leg 3: replay the admission operation; the receipt is identical.
    let leg_input_path = crate::path(&artifacts.join("input.bin"));
    let replay_args: Vec<&str> = if client == Subject::Rust {
        vec![
            "replay",
            "--operation",
            &admit,
            "--input",
            &leg_input_path,
            "--declaration",
            &declare,
        ]
    } else {
        vec!["replay", "--operation", &admit, "--input", &leg_input_path]
    };
    let replay = session.op(&replay_args)?;
    let replay_stdout = require(&replay, "RECEIPT", "admission replay")?;
    ensure!(
        replay_stdout.trim() == receipt.trim(),
        "replayed admission returned a different receipt:\n{}\noriginal:\n{}",
        replay_stdout.trim(),
        receipt.trim()
    );
    let leg_view = session.watch("0:0:1")?;
    ensure!(
        view_line(&leg_view)? == terminal_view,
        "admission replay changed the work view:\n{leg_view}"
    );
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("reconnect_leg", "BINDING ok, view identical".into()),
        ("manifest_reread_leg", "MANIFEST ok, view identical".into()),
        (
            "admission_replay_leg",
            "identical RECEIPT, view identical".into(),
        ),
        (
            "retry_leg",
            "skipped with note: work is terminal-success, not retryable".into(),
        ),
        (
            "deadline_fields_after_all_legs",
            "deadline/terminal_at/receipt_until/output_until unchanged".into(),
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

    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// Shared g3-partial-retirement / g3-nonreusable-history build-up:
/// short promises (6s/6s), publish, root-close, wait out every promise, and
/// observe the retirement window. Returns the observation plus the object
/// metrics delta (supplementary private-storage evidence).
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn retire_session_buildup(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    server: Subject,
    client: Subject,
) -> Result<(
    Session,
    EventWriter,
    PathBuf,
    String,
    String,
    String,
    RetirementObservation,
    String,
)> {
    let policy = (60_000u64, 6_000u64, 6_000u64);
    let (session, events, artifacts, declare, admit, terminal, _input_sha256) = expiry_preamble(
        context,
        scenario_dir,
        id,
        server,
        client,
        policy,
        &[(
            "expected_after_promises_expire",
            "authorized requests refuse named EXPIRED (6) or NOT_FOUND (5) once retired \
                 (both servers automatic); creation replay refuses EXPIRED (6); high-water \
                 preserved on both"
                .into(),
        )],
    )?;
    if client == Subject::Rust {
        check_policy_offsets(&terminal, policy).context(format!("{id} policy offsets"))?;
    }
    let receipt_until = parse_field_u64(&terminal, "receipt_until")?
        .context("terminal view did not report receipt_until")?;
    let output_until = parse_field_u64(&terminal, "output_until")?
        .context("terminal view did not report output_until")?;
    let metrics_before = metrics_text(storage_metrics(&session.fixture.object_dir)?);
    root_close(&session)?;
    detach(&session)?;
    let wait_until = receipt_until.max(output_until) + Duration::from_secs(15).as_millis() as u64;
    let observation = observe_retirement_window(&session, "0:0:1", wait_until)?;
    let metrics_after = metrics_text(storage_metrics(&session.fixture.object_dir)?);
    Ok((
        session,
        events,
        artifacts,
        declare,
        admit,
        metrics_before,
        observation,
        metrics_after,
    ))
}

/// g3-partial-retirement: root-close a session, wait out the short
/// receipt+output promises, and observe what actually happens. Neither CLI
/// exposes a retirement operator command (named gap); both servers retire
/// automatically (rust runtime maintenance, Java RetentionService) with the
/// lifecycle transition durably recorded before metadata removal: authorized
/// requests refuse a named code, never partial replay. Object-store metrics
/// before/after are recorded as supplementary private-storage evidence.
fn g3_partial_retirement(context: &ScenarioContext) -> Result<()> {
    run_expiry_row(
        context,
        "g3-partial-retirement",
        g3_partial_retirement_direction,
    )
}

fn g3_partial_retirement_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g3-partial-retirement";
    let (session, events, artifacts, _declare, admit, metrics_before, observation, metrics_after) =
        retire_session_buildup(context, scenario_dir, id, server, client)?;
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("clock_mode", "real-short-policy".into()),
        ("retirement_mechanism", RETIREMENT_MECHANISM.into()),
        ("object_metrics_before", metrics_before),
        ("object_metrics_after", metrics_after),
    ];
    observed.push((
        "authorized_requests_after_promises",
        require_retired(&observation, "g3-partial-retirement authorized requests")?,
    ));
    observed.extend(retired_access_probes(
        &session,
        scenario_dir,
        &artifacts,
        &admit,
    )?);
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// g3-nonreusable-history: after full retirement/expiry, history must be
/// non-reusable. Probes: creation replay with the retired sequence (named
/// EXPIRED (6) on both servers), operation replay with the retired operation
/// id (a named refusal, never reapplied), attach to the retired generation
/// (named EXPIRED (6); the never-created sequence refuses CONFLICT (7)
/// instead, so retired history is not conflated with nonexistence), and
/// owner creation high-water preserved (the next creation binds sequence 2 —
/// never a reissued sequence 1).
fn g3_nonreusable_history(context: &ScenarioContext) -> Result<()> {
    run_expiry_row(
        context,
        "g3-nonreusable-history",
        g3_nonreusable_history_direction,
    )
}

fn g3_nonreusable_history_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g3-nonreusable-history";
    let (session, events, artifacts, declare, admit, metrics_before, observation, metrics_after) =
        retire_session_buildup(context, scenario_dir, id, server, client)?;
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("clock_mode", "real-short-policy".into()),
        ("retirement_mechanism", RETIREMENT_MECHANISM.into()),
        ("object_metrics_before", metrics_before),
        ("object_metrics_after", metrics_after),
    ];
    observed.push((
        "retirement_window",
        require_retired(&observation, "g3-nonreusable-history retirement window")?,
    ));
    observed.extend(retired_access_probes(
        &session,
        scenario_dir,
        &artifacts,
        &admit,
    )?);

    // Operation replay with the retired operation id: a named refusal on both
    // servers, never reapplied.
    let retired_input_path = crate::path(&artifacts.join("input.bin"));
    let replay_args: Vec<&str> = if client == Subject::Rust {
        vec![
            "replay",
            "--operation",
            &admit,
            "--input",
            &retired_input_path,
            "--declaration",
            &declare,
        ]
    } else {
        vec![
            "replay",
            "--operation",
            &admit,
            "--input",
            &retired_input_path,
        ]
    };
    let replay = session.op(&replay_args)?;
    let replay_text = transcript(&replay);
    fs::write(artifacts.join("retired-operation-replay.txt"), &replay_text)?;
    let outcome = probe_outcome(&replay);
    let (code, line) = outcome.refusal.unwrap_or((u32::MAX, String::new()));
    let success = replay.status.success();
    ensure!(
        !success && (code == 6 || code == 5),
        "operation replay on the retired session must refuse a named EXPIRED (6) or \
         NOT_FOUND (5) code, got success={success} code={code:?} line={line}\n{replay_text}"
    );
    observed.push((
        "retired_operation_replay",
        "refused, never reapplied".into(),
    ));

    // Attach to a retired generation vs a never-created generation: the
    // refusal classes differ by design (existence rules) and the retired
    // attach never succeeds.
    let attach_journal = scenario_dir.join("client").join("probe-attach.sqlite");
    let attach = creation_probe(&session, &attach_journal, 1)?;
    let attach_text = transcript(&attach);
    fs::write(
        artifacts.join("attach-retired-generation.txt"),
        &attach_text,
    )?;
    let outcome = probe_outcome(&attach);
    let (code, line) = outcome.refusal.unwrap_or((u32::MAX, String::new()));
    let success = attach.status.success();
    ensure!(
        !success && code == 6,
        "attach to the retired generation must refuse named EXPIRED (6), \
         got success={success} code={code:?} line={line}\n{attach_text}"
    );
    observed.push(("attach_retired_generation", "refused EXPIRED (6)".into()));
    let never_journal = scenario_dir.join("client").join("probe-never.sqlite");
    let never = creation_probe(&session, &never_journal, 99)?;
    let never_text = transcript(&never);
    fs::write(
        artifacts.join("attach-never-created-generation.txt"),
        &never_text,
    )?;
    let never_code = transcript_named_code(&never_text);
    let never_success = never.status.success();
    ensure!(
        !never_success && never_code == Some(7),
        "attach to a never-created generation must refuse named CONFLICT (7), \
         got success={never_success} code={never_code:?}\n{never_text}"
    );
    observed.push((
        "attach_never_created_generation",
        "refused CONFLICT (7)".into(),
    ));

    // Owner creation high-water preserved: the next creation binds sequence 2
    // (never a reissued sequence 1) and the high-water advances past it.
    let next_journal = scenario_dir.join("client").join("probe-next.sqlite");
    let next = creation_probe(&session, &next_journal, 2)?;
    let next_text = transcript(&next);
    ensure!(
        next.status.success() && next_text.contains("BINDING"),
        "creation with the next sequence must succeed after retirement:\n{next_text}"
    );
    let high_water = session.fixture.next_sequence(&session.server, "alice")?;
    ensure!(
        high_water == 3,
        "owner creation high-water must advance to 3 after binding sequence 2, got {high_water}"
    );
    observed.push((
        "high_water_create_after_retirement",
        "sequence 2 bound; next-sequence=3".into(),
    ));

    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;

    stop_and_seal(context, scenario_dir, id, session.server, events)
}

// ---------------------------------------------------------------------------
// G4: publication races, fences, settlement (milestone 12)
// ---------------------------------------------------------------------------

/// Run one G4 row in the canonical rust/rust direction plus the
/// rust-client/java-server direction when a jar is present (the
/// `run_expiry_row` shape; the row function itself journals both legs).
fn run_g4_row(
    context: &ScenarioContext,
    row_id: &str,
    direction: fn(&ScenarioContext, &Path, Subject, Subject) -> Result<()>,
) -> Result<()> {
    direction(
        context,
        &context.scenario_dir(row_id),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(
        context,
        row_id,
        "rust-client-java-server",
        |context, dir| direction(context, dir, Subject::Java, Subject::Rust),
    )?;
    Ok(())
}

/// Fence-receipt disposition, subject-agnostic: the rust client prints
/// `disposition: Disposition(0|1)` (Debug), the java client prints
/// `disposition=0|1` (record toString). 0 = accepted fence, 1 = the work was
/// already terminal when the fence arrived.
fn receipt_disposition(receipt_stdout: &str) -> Result<u64> {
    for (needle, value) in [("Disposition(1)", 1u64), ("Disposition(0)", 0)] {
        if receipt_stdout.contains(needle) {
            return Ok(value);
        }
    }
    for (needle, value) in [("disposition=1", 1u64), ("disposition=0", 0)] {
        if receipt_stdout.contains(needle) {
            return Ok(value);
        }
    }
    bail!("fence receipt does not render a disposition:\n{receipt_stdout}")
}

/// Fence-receipt state-at-commit rendered name, when present: rust prints
/// `state_at_commit: State(4)`, java prints `state=CANCELLING`. Disposition-0
/// fence receipts may already carry a terminal state (spec contradiction #8);
/// the rows record which.
fn receipt_state_at_commit(receipt_stdout: &str) -> String {
    if let Some(rest) = receipt_stdout.split("state_at_commit: State(").nth(1) {
        let code = rest.split(')').next().unwrap_or_default();
        return state_code_name(code).to_owned();
    }
    if let Some(rest) = receipt_stdout.split("state=").nth(1) {
        return rest
            .split([',', ']'])
            .next()
            .unwrap_or_default()
            .trim()
            .to_owned();
    }
    "unrendered".to_owned()
}

fn state_code_name(code: &str) -> &'static str {
    match code {
        "0" => "DECLARED",
        "1" => "ACTIVE",
        "2" => "AWAITING_RETRY",
        "3" => "WAITING_CHILDREN",
        "4" => "CANCELLING",
        "5" => "SUCCEEDED",
        "6" => "FAILED",
        "7" => "CANCELLED",
        "8" => "SKIPPED",
        _ => "UNKNOWN",
    }
}

/// Diagnostic code carried by a watch VIEW line, subject-agnostic: rust
/// renders `DiagnosticCode(11)`, java `Diagnostic[code=11, ...]`.
fn view_diagnostic_code(view: &str) -> Option<u64> {
    if let Some(rest) = view.split("DiagnosticCode(").nth(1) {
        return rest.split(')').next()?.parse().ok();
    }
    view.split("code=")
        .nth(1)?
        .split([',', ']'])
        .next()?
        .parse()
        .ok()
}

fn refusal_code_name(code: u64) -> &'static str {
    REFUSAL_CODES
        .iter()
        .find(|(_, known)| u64::from(*known) == code)
        .map(|(name, _)| *name)
        .unwrap_or("UNKNOWN")
}

/// One read probe against a work that must refuse: the read must fail with a
/// NAMED Section 12.2 code, never a silent failure. Returns the observed
/// refusal line for the journal.
fn expect_named_read_refusal(
    session: &Session,
    artifacts: &Path,
    work: &str,
    name: &str,
) -> Result<String> {
    let output = session.op(&[
        "read",
        "--work",
        work,
        "--attempt",
        "1",
        "--index",
        "0",
        "--output",
        &crate::path(&artifacts.join(name)),
    ])?;
    let probe = probe_outcome(&output);
    let text = probe.transcript();
    fs::write(artifacts.join(name), &text)?;
    ensure!(
        !probe.success,
        "read of fenced work {work} was expected to refuse but exited zero\n{text}"
    );
    let (code, line) = probe
        .refusal
        .with_context(|| format!("read of fenced work {work} must name a refusal code\n{text}"))?;
    Ok(format!(
        "refused {} ({code}): {line}",
        refusal_code_name(u64::from(code))
    ))
}

/// The outcome of one statistical race iteration, recorded into observed.tsv.
struct RaceIteration {
    index: u32,
    input_len: usize,
    disposition: u64,
    winning_order: &'static str,
    final_state: u64,
    state_at_commit: String,
    read_outcome: String,
}

/// One publication-vs-fence iteration: admit a copy/v2 work whose input size
/// seeds the race window, then immediately race one fence op (`cancel` or
/// `skip`) against the in-flight publication. The per-iteration input sizes
/// create the order asymmetry the subject hooks cannot (neither subject can
/// pause inside the application callback — the matrix's hook-free statistical
/// variant). Per-iteration spec conformance: exactly one terminal state;
/// publication-won (disposition 1) ends SUCCEEDED with a byte-exact result
/// read; fence-won (disposition 0) ends CANCELLED/SKIPPED with the result
/// read refusing a named code. Returns the classified iteration.
#[allow(clippy::too_many_arguments)]
fn g4_fence_race_iteration(
    session: &Session,
    events: &mut EventWriter,
    context: &ScenarioContext,
    artifacts: &Path,
    declare: &str,
    index: u32,
    input_len: usize,
    fence: &str,
    fence_won_state: u64,
) -> Result<RaceIteration> {
    let work = format!("0:0:{index}");
    let seed = context.seed ^ (u64::from(index) * 0x9e37_79b9);
    let input = oracle::dataset(seed, input_len);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join(format!("input-{index}.bin"));
    fs::write(&input_path, &input)?;
    events.append(
        "",
        None,
        Some(&work),
        Some(1),
        None,
        Some(ArtifactRef {
            path: format!("artifacts/input-{index}.bin"),
            len: input.len() as u64,
            sha256: input_sha256.clone(),
        }),
    )?;
    let admit = oracle::operation_hex(oracle::operation_id(seed, "admit", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit)?),
        Some(&work),
        Some(1),
        None,
        None,
    )?;
    // Spawn the admission without waiting; the sized stream keeps the
    // server-side publication in flight after the receipt returns.
    let admit_child = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &[
            "admit",
            "--operation",
            &admit,
            "--declaration",
            declare,
            "--work",
            &work,
            "--input",
            &crate::path(&input_path),
            "--application",
            "copy/v2",
        ],
    )?;
    let admitted = AuthorityFixture::wait_client_op(admit_child, OP_WAIT)?;
    let _admit_receipt = require(&admitted, "RECEIPT", "admit operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some(&work),
        Some(1),
        None,
        None,
    )?;

    // Immediately race the fence against the in-flight publication.
    let fence_op = oracle::operation_hex(oracle::operation_id(seed, fence, 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&fence_op)?),
        Some(&work),
        Some(1),
        None,
        None,
    )?;
    let fence_out = session.op(&[fence, "--operation", &fence_op, "--work", &work])?;
    let fence_stdout = require(&fence_out, "RECEIPT", &format!("{fence} operation"))?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&fence_op)?),
        Some(&work),
        Some(1),
        None,
        None,
    )?;
    let disposition = receipt_disposition(&fence_stdout)?;
    let state_at_commit = receipt_state_at_commit(&fence_stdout);

    // Poll the work to its single terminal state.
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let (final_state, final_view) = loop {
        let stdout = session.watch(&work)?;
        let state = parse_state(&stdout)?;
        if (5..=8).contains(&state) {
            break (state, stdout);
        }
        ensure!(
            Instant::now() < deadline,
            "g4 fence race iteration {index} ({fence}): work {work} did not settle within \
             {RECOVERY_TIMEOUT:?}\nlast view:\n{stdout}"
        );
        thread::sleep(Duration::from_millis(50));
    };
    fs::write(
        artifacts.join(format!("iteration-{index}-terminal-view.txt")),
        &final_view,
    )?;
    events.append(
        "OBSERVATION_JOURNALED",
        None,
        Some(&work),
        Some(1),
        None,
        None,
    )?;

    let (winning_order, read_outcome) = if disposition == 1 {
        // Publication won: the work was already terminal when the fence
        // arrived. The only legal terminal state for this row is success;
        // a failure here would be a fabricated outcome.
        ensure!(
            final_state == 5,
            "g4 {fence} race iteration {index}: disposition 1 (publication won) but the \
             terminal state is {} ({}) — never a fabricated failure\n{final_view}",
            final_state,
            state_code_name(&final_state.to_string())
        );
        let sha = read_output_verified(
            session,
            events,
            &work,
            1,
            &input,
            &input_sha256,
            artifacts,
            &format!("iteration-{index}-output.bin"),
        )?;
        (
            "publication",
            format!("result read byte-exact (sha256={sha})"),
        )
    } else {
        // Fence won: the outcome is the fence's, never SUCCEEDED, never a
        // fabricated FAILED, and the result read must refuse a named code.
        ensure!(
            final_state == fence_won_state,
            "g4 {fence} race iteration {index}: disposition 0 (fence won) but the terminal \
             state is {} ({}) — expected {} ({})",
            final_state,
            state_code_name(&final_state.to_string()),
            fence_won_state,
            state_code_name(&fence_won_state.to_string()),
        );
        let refusal = expect_named_read_refusal(
            session,
            artifacts,
            &work,
            &format!("iteration-{index}-read-refusal.txt"),
        )?;
        events.append("", None, Some(&work), Some(1), None, None)?;
        ("fence", format!("result read {refusal}"))
    };
    Ok(RaceIteration {
        index,
        input_len,
        disposition,
        winning_order,
        final_state,
        state_at_commit,
        read_outcome,
    })
}

fn g4_publication_vs_cancel(context: &ScenarioContext) -> Result<()> {
    run_g4_row(
        context,
        "g4-publication-vs-cancel",
        g4_publication_vs_cancel_direction,
    )
}

/// g4-publication-vs-cancel: race cancel against the in-flight publication.
/// Hook-free statistical variant per the G4 matrix evidence rule: neither
/// subject can pause inside the application callback, so seeded input sizes
/// create the order asymmetry and every iteration records its winning order.
fn g4_publication_vs_cancel_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g4-publication-vs-cancel";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    let session = setup_session(context, scenario_dir, server, client)?;
    // Descending sizes: large inputs keep the publication in flight so the
    // cancel fence usually wins; small inputs publish before the fence
    // process can start and usually lose. Both orders are legal per row.
    let sizes = [
        12 * 1024 * 1024,
        8 * 1024 * 1024,
        1024 * 1024,
        256 * 1024,
        64 * 1024,
        64 * 1024,
    ];
    let entities: Vec<u64> = (1..=sizes.len() as u64).collect();
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("iterations", sizes.len().to_string()),
            (
                "iteration_input_lens",
                sizes
                    .iter()
                    .map(|size| size.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "winning_order_rule",
                "per iteration: disposition=1 → publication won (terminal SUCCEEDED(5), \
                 result read byte-exact); disposition=0 → fence won (terminal CANCELLED(7), \
                 result read refuses a named Section 12.2 code); exactly one terminal state"
                    .into(),
            ),
            (
                "evidence_rule",
                "hook-free statistical variant (scenario-matrix-g4.md): no subject hook can \
                 pause inside the application callback, so seeded input sizes create the \
                 order asymmetry; winning_order is recorded per iteration, never faked"
                    .into(),
            ),
            (
                "never",
                "both winners, neither winner, terminal FAILED(6), silent read failure".into(),
            ),
        ],
    )?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &entities)?;
    let mut iterations = Vec::new();
    for (position, size) in sizes.iter().enumerate() {
        iterations.push(g4_fence_race_iteration(
            &session,
            &mut events,
            context,
            &artifacts,
            &declare,
            (position + 1) as u32,
            *size,
            "cancel",
            7,
        )?);
    }
    detach(&session)?;
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
    ];
    let mut publication_wins = 0usize;
    let mut fence_wins = 0usize;
    for iteration in &iterations {
        publication_wins += usize::from(iteration.winning_order == "publication");
        fence_wins += usize::from(iteration.winning_order == "fence");
        observed.push((
            Box::leak(format!("iteration_{}_input_len", iteration.index).into_boxed_str()),
            iteration.input_len.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_winning_order", iteration.index).into_boxed_str()),
            iteration.winning_order.into(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_disposition", iteration.index).into_boxed_str()),
            iteration.disposition.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_final_state", iteration.index).into_boxed_str()),
            format!(
                "{} ({})",
                iteration.final_state,
                state_code_name(&iteration.final_state.to_string())
            ),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_state_at_commit", iteration.index).into_boxed_str()),
            iteration.state_at_commit.clone(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_read", iteration.index).into_boxed_str()),
            iteration.read_outcome.clone(),
        ));
    }
    observed.push((
        "orders_observed",
        format!("publication:{publication_wins},fence:{fence_wins}"),
    ));
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

fn g4_publication_vs_skip(context: &ScenarioContext) -> Result<()> {
    run_g4_row(
        context,
        "g4-publication-vs-skip",
        g4_publication_vs_skip_direction,
    )
}

/// g4-publication-vs-skip: race skip (skip-authorized via `--allow-skip` on
/// both subjects) against the in-flight publication, plus the skip-fence
/// aftermath: a conflicting later cancel refuses CANCELLED (12) and replaying
/// the skip operation returns its original receipt unchanged.
fn g4_publication_vs_skip_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g4-publication-vs-skip";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    // `--allow-skip` is a rust Storage open flag and a java serve flag; both
    // spellings ride the serve invocation.
    let session =
        setup_session_serve_args(context, scenario_dir, server, client, &["--allow-skip"])?;
    // Sized one head-step below the cancel row: every admitted work drains
    // the authority's funded WAL completion budget cumulatively, and run
    // evidence showed the 12 MiB head size pushing session A's later admits
    // against that ceiling.
    let sizes = [
        8 * 1024 * 1024,
        4 * 1024 * 1024,
        1024 * 1024,
        256 * 1024,
        64 * 1024,
        64 * 1024,
    ];
    let entities: Vec<u64> = (1..=sizes.len() as u64).collect();
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("iterations", sizes.len().to_string()),
            (
                "iteration_input_lens",
                sizes
                    .iter()
                    .map(|size| size.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "skip_authorization",
                "--allow-skip on serve: rust Storage open flag (server/src/v2/ \
                 configuration.rs Access::permits), java serve flag (V2Main.java)"
                    .into(),
            ),
            (
                "winning_order_rule",
                "per iteration: disposition=1 → publication won (terminal SUCCEEDED(5), \
                 result read byte-exact); disposition=0 → fence won (terminal SKIPPED(8) — \
                 never SUCCEEDED, never FAILED — result read refuses a named code)"
                    .into(),
            ),
            (
                "aftermath_rule",
                "on a fence-won work: a later conflicting cancel refuses CANCELLED (12) \
                 \"first work fence has another outcome\"; replaying the skip operation \
                 (same immutable id) returns the identical receipt"
                    .into(),
            ),
            (
                "aftermath_hold_open",
                "statistical, hook-free: a mode-1 reassemble/v2 parent is \
                 nonterminal from admission with a sealed child scope of 33 \
                 children; child 1 is retry-copy/v2, which parks in \
                 AWAITING_RETRY with no authorized retry, so reconcile \
                 cannot close the scope until the fence cascade cancels it; \
                 the seal-hash and status member scans then cost several \
                 scan_batch passes, widening the fenced-but-nonterminal \
                 window to ~6 idle polls (20ms each); a cancel that instead \
                 observes the settled parent takes the disposition-1 \
                 terminal path (a legal race outcome), so the aftermath \
                 retries on a fresh authority, at most 4 attempts, and \
                 records every order observed"
                    .into(),
            ),
            (
                "evidence_rule",
                "hook-free statistical variant (scenario-matrix-g4.md); winning_order \
                 recorded per iteration, never faked"
                    .into(),
            ),
        ],
    )?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &entities)?;
    let mut iterations = Vec::new();
    for (position, size) in sizes.iter().enumerate() {
        iterations.push(g4_fence_race_iteration(
            &session,
            &mut events,
            context,
            &artifacts,
            &declare,
            (position + 1) as u32,
            *size,
            "skip",
            8,
        )?);
    }

    // The six statistical iterations share one session; the deterministic
    // aftermath runs in a FRESH authority: retained completion credits fund
    // every work ever admitted (records.rs audit), so a session that already
    // streamed its statistical admissions has no headroom left for a second
    // large expansion. Detach and stop the iteration session first.
    detach(&session)?;
    session.server.stop()?;

    // Statistical aftermath. The parent is a mode-1 reassemble/v2 work: mode
    // 1 allocates its child scope AT ADMISSION (admission.rs) and enters
    // WAITING_CHILDREN (3), so the parent is nonterminal with live
    // descendants the moment its admit returns. The child scope is declared
    // sealed over 33 small copy/v2 children, and closing that scope costs
    // reconcile several bounded passes (scan_batch is 32 members per pass,
    // walked twice: seal hash, then status), so after the skip fence commits
    // reconcile needs ~5 idle polls (idle_poll_ms = 20) to close the scope
    // and settle the parent SKIPPED. That window is what the conflicting
    // cancel needs: it must observe the parent nonterminal with the SKIPPED
    // fence committed and refuse CANCELLED (12) "first work fence has another
    // outcome" (settlement.rs work_fence mismatch path). If the cancel
    // instead observes an already terminal parent it takes the disposition-1
    // fast path and returns a receipt — a legal race outcome, not a spec
    // violation — so the aftermath runs up to four fresh-authority attempts,
    // records every order it saw, and requires at least one skip-won refusal.
    const AFTERMATH_CHILDREN: u64 = 33;
    const AFTERMATH_MAX_ATTEMPTS: u32 = 4;
    let aftermath_dir = scenario_dir.join("aftermath");
    let aftermath_seed = context.seed ^ 0x0514_b1d5_c0de_u64;
    let mut refusal_line: Option<String> = None;
    let mut replay_note = String::new();
    let mut attempt_notes: Vec<String> = Vec::new();
    let mut attempts_run = 0u32;
    for attempt in 1..=AFTERMATH_MAX_ATTEMPTS {
        if refusal_line.is_some() {
            break;
        }
        attempts_run = attempt;
        let attempt_seed = aftermath_seed ^ (u64::from(attempt).wrapping_mul(0x9e37_79b9));
        let attempt_dir = aftermath_dir.join(format!("attempt-{attempt}"));
        let session =
            setup_session_serve_args(context, &attempt_dir, server, client, &["--allow-skip"])?;
        let entities: Vec<u64> = (1..=AFTERMATH_CHILDREN + 1).collect();
        let declare = declare_sealed(&session, &mut events, attempt_seed, "declare", &entities)?;
        let work = "0:0:1";
        let input = oracle::dataset(attempt_seed, 1024 * 1024);
        let input_path = attempt_dir.join("aftermath-input.bin");
        fs::write(&input_path, &input)?;
        let child_input_path = attempt_dir.join("aftermath-child-input.bin");
        fs::write(
            &child_input_path,
            oracle::dataset(attempt_seed ^ 0x0c415d, 1024),
        )?;
        admit_modeled(
            &session,
            &mut events,
            attempt_seed,
            "admit",
            &declare,
            work,
            &input_path,
            "reassemble/v2",
            1,
            1,
        )?;
        // Gate on the parent holding in WAITING_CHILDREN with its child
        // scope open. Mode 1 allocates the child scope at admission, so the
        // first watch already shows this; the bounded loop only guards
        // against scheduler delay.
        let gate_deadline = Instant::now() + RECOVERY_TIMEOUT;
        let mut gated = false;
        loop {
            let stdout = session.watch(work)?;
            let state = parse_state(&stdout)?;
            if state == 3 && parse_child_scope(&stdout)?.is_some() {
                gated = true;
                break;
            }
            if (5..=8).contains(&state) {
                break;
            }
            ensure!(
                Instant::now() < gate_deadline,
                "g4-publication-vs-skip aftermath attempt {attempt}: mode-1 \
                 parent did not reach WAITING_CHILDREN within {RECOVERY_TIMEOUT:?}"
            );
            thread::sleep(Duration::from_millis(20));
        }
        ensure!(
            gated,
            "g4-publication-vs-skip aftermath attempt {attempt}: mode-1 \
             parent settled before the deterministic gate"
        );
        // Declare the child scope sealed over its members, then admit them.
        // Child 1 is retry-copy/v2: attempt 1 parks it in AWAITING_RETRY
        // ("never advances its own attempt") with no driver-authorized retry,
        // so the scope's member scan can never close the scope while the
        // retry child is nonterminal. After the skip fence commits,
        // reconcile cancels it in one pass and then still owes the seal-hash
        // and status member scans (32 members per pass) before the scope can
        // close and the parent can settle — the fenced-but-nonterminal
        // window the conflicting cancel races.
        let child_entities: Vec<u64> = (1..=AFTERMATH_CHILDREN).collect();
        let (child_declare, _child_receipt) = declare_scoped_batch(
            &session,
            &mut events,
            attempt_seed,
            "declare-child",
            0,
            1,
            &child_entities,
            true,
        )?;
        for child in 1..=AFTERMATH_CHILDREN {
            let application = if child == 1 {
                "retry-copy/v2"
            } else {
                "copy/v2"
            };
            admit_modeled(
                &session,
                &mut events,
                attempt_seed,
                &format!("admit-child-{child}"),
                &child_declare,
                &format!("1:0:{child}"),
                &child_input_path,
                application,
                0,
                1,
            )?;
        }
        let skip_op = oracle::operation_hex(oracle::operation_id(attempt_seed, "skip", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&skip_op)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        let skip_out = session.op(&["skip", "--operation", &skip_op, "--work", work])?;
        let skip_stdout = require(&skip_out, "RECEIPT", "skip operation")?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&skip_op)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        ensure!(
            receipt_disposition(&skip_stdout)? == 0,
            "aftermath attempt {attempt}: the skip fence must be accepted \
             (disposition 0) while descendants are open:\n{skip_stdout}"
        );
        let state_at_commit = receipt_state_at_commit(&skip_stdout);
        ensure!(
            state_at_commit == "CANCELLING",
            "aftermath attempt {attempt}: the accepted skip fence must \
             commit state_at_commit CANCELLING while descendants are open, \
             got {state_at_commit}:\n{skip_stdout}"
        );
        // Conflicting cancel after the accepted skip fence. Probed without
        // failing so a raced settle (disposition-1 receipt) can retry.
        let cancel_op = oracle::operation_hex(oracle::operation_id(attempt_seed, "cancel", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&cancel_op)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        let cancel_out = session.op(&["cancel", "--operation", &cancel_op, "--work", work])?;
        let cancel_text = format!(
            "exit={}\nstdout:\n{}\nstderr:\n{}",
            cancel_out.status,
            String::from_utf8_lossy(&cancel_out.stdout),
            String::from_utf8_lossy(&cancel_out.stderr),
        );
        fs::write(
            artifacts.join(format!("aftermath-attempt-{attempt}-cancel.txt")),
            &cancel_text,
        )?;
        if cancel_out.status.success() {
            // The cancel observed an already terminal parent and took the
            // disposition-1 fast path: a legal race outcome. Record it and
            // retry on a fresh authority.
            attempt_notes.push(format!(
                "attempt {attempt}: cancel raced the settle and observed the \
                 terminal parent (disposition 1)"
            ));
            events.append(
                "OBSERVATION_JOURNALED",
                Some(hex_to_id(&cancel_op)?),
                Some(work),
                Some(1),
                None,
                None,
            )?;
            detach(&session)?;
            session.server.stop()?;
            continue;
        }
        let named = refusal_named_line(&cancel_text, &["CANCELLED"]).with_context(|| {
            format!(
                "aftermath attempt {attempt}: cancel after the accepted skip \
                 fence must name CANCELLED (12)\n{cancel_text}"
            )
        })?;
        events.append(
            "",
            Some(hex_to_id(&cancel_op)?),
            Some(work),
            Some(1),
            Some(12),
            None,
        )?;
        refusal_line = Some(format!("attempt {attempt} work {work}: {named}"));
        // Replay stability: the retained skip receipt returns unchanged, and
        // after reconcile settles the fenced descendants the outcome stays
        // SKIPPED and the result read still refuses with a named code.
        let replay_out = session.op(&["skip", "--operation", &skip_op, "--work", work])?;
        let replay_stdout = require(&replay_out, "RECEIPT", "skip replay")?;
        ensure!(
            replay_stdout.trim() == skip_stdout.trim(),
            "aftermath attempt {attempt}: replayed skip returned a different \
             receipt:\n{}\noriginal:\n{}",
            replay_stdout.trim(),
            skip_stdout.trim()
        );
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&skip_op)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        let settle_deadline = Instant::now() + RECOVERY_TIMEOUT;
        let terminal = loop {
            let stdout = session.watch(work)?;
            let state = parse_state(&stdout)?;
            if (5..=8).contains(&state) {
                ensure!(
                    state == 8,
                    "aftermath attempt {attempt}: fenced parent must settle \
                     SKIPPED, got {state}\n{stdout}"
                );
                break stdout;
            }
            ensure!(
                Instant::now() < settle_deadline,
                "g4-publication-vs-skip aftermath attempt {attempt}: parent \
                 did not settle within {RECOVERY_TIMEOUT:?}"
            );
            thread::sleep(Duration::from_millis(50));
        };
        let replay_out = session.op(&["skip", "--operation", &skip_op, "--work", work])?;
        let replay_stdout = require(&replay_out, "RECEIPT", "skip replay")?;
        ensure!(
            replay_stdout.trim() == skip_stdout.trim(),
            "aftermath attempt {attempt}: post-settle replay returned a \
             different receipt:\n{}\noriginal:\n{}",
            replay_stdout.trim(),
            skip_stdout.trim()
        );
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&skip_op)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        let after = session.watch(work)?;
        ensure!(
            view_line(&after)? == view_line(&terminal)?,
            "aftermath attempt {attempt}: skip replay changed the settled \
             work view:\n{after}\noriginal:\n{terminal}"
        );
        let read_note = expect_named_read_refusal(
            &session,
            &artifacts,
            work,
            &format!("aftermath-attempt-{attempt}-read-refusal.txt"),
        )?;
        replay_note = format!(
            "attempt {attempt} work {work}: identical receipt; still SKIPPED; result read {read_note}"
        );
        detach(&session)?;
        session.server.stop()?;
    }
    let refusal_line = refusal_line.with_context(|| {
        format!(
            "no aftermath attempt observed the CANCELLED (12) refusal in \
             {AFTERMATH_MAX_ATTEMPTS} attempts: {}",
            attempt_notes.join(" | ")
        )
    })?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
    ];
    let mut publication_wins = 0usize;
    let mut fence_wins = 0usize;
    for iteration in &iterations {
        publication_wins += usize::from(iteration.winning_order == "publication");
        fence_wins += usize::from(iteration.winning_order == "fence");
        observed.push((
            Box::leak(format!("iteration_{}_input_len", iteration.index).into_boxed_str()),
            iteration.input_len.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_winning_order", iteration.index).into_boxed_str()),
            iteration.winning_order.into(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_final_state", iteration.index).into_boxed_str()),
            format!(
                "{} ({})",
                iteration.final_state,
                state_code_name(&iteration.final_state.to_string())
            ),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_read", iteration.index).into_boxed_str()),
            iteration.read_outcome.clone(),
        ));
    }
    observed.push((
        "orders_observed",
        format!("publication:{publication_wins},fence:{fence_wins}"),
    ));
    observed.push(("skip_then_cancel_refusal", refusal_line));
    observed.push(("skip_replay", replay_note));
    observed.push(("aftermath_attempts", attempts_run.to_string()));
    observed.push(("aftermath_race_orders", attempt_notes.join(" | ")));
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
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

fn g4_deadline_settlement(context: &ScenarioContext) -> Result<()> {
    run_g4_row(
        context,
        "g4-deadline-settlement",
        g4_deadline_settlement_direction,
    )
}

/// The outcome of one deadline-race iteration.
struct DeadlineIteration {
    index: u32,
    input_len: usize,
    winning_order: &'static str,
    final_state: u64,
    diagnostic: String,
    read_outcome: String,
}

/// g4-deadline-settlement (matrix: g4-publication-vs-deadline): admission
/// with a short `--execution-ms` deadline races the mode-2 chunk-copy
/// publication against the deadline. Per-iteration legal orders:
/// publication-wins (parent terminal SUCCEEDED(5), result byte-exact) or
/// deadline-wins (parent terminal FAILED(6) carrying the DEADLINE_EXCEEDED
/// diagnostic, retry afterwards refuses a named code, the declared
/// obligation persists in the view). Hook-free statistical variant: the
/// seeded input sizes create the asymmetry; winning_order is recorded, never
/// faked.
fn g4_deadline_settlement_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g4-deadline-settlement";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    const EXECUTION_MS: u64 = 1000;
    // Mode-2 (chunk-copy/v2) admissions; the parent only succeeds after the
    // whole producer-1 child scope closes, so large inputs outrun the
    // deadline while small inputs settle well inside it. 8 MiB (128 chunks)
    // is the largest input the authority record capacity admits — 12 MiB
    // (192 chunks) refuses LIMIT_EXCEEDED "authority record completion
    // capacity exhausted" (rust) / "retained input, output or executor
    // capacity" (java) once earlier iterations' children still hold funded
    // jobs. Every iteration therefore owns a fresh authority.
    let sizes = [8 * 1024 * 1024, 4 * 1024 * 1024, 1024 * 1024, 256 * 1024];
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("application", "chunk-copy/v2".into()),
            ("mode", "2".into()),
            ("execution_deadline_ms", EXECUTION_MS.to_string()),
            ("iterations", sizes.len().to_string()),
            (
                "iteration_input_lens",
                sizes
                    .iter()
                    .map(|size| size.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "session_model",
                "one fresh authority per iteration: a deadline-failed mode-2 parent leaves \
                 funded child jobs that count against the next admission's capacity checks on \
                 both subjects"
                    .into(),
            ),
            (
                "winning_order_rule",
                "per iteration: parent terminal SUCCEEDED(5) → publication won (result read \
                 byte-exact); parent terminal FAILED(6) with the DEADLINE_EXCEEDED diagnostic \
                 (code 11) → deadline won (fenced settlement, not silent eviction; result read \
                 refuses a named code)"
                    .into(),
            ),
            (
                "retry_after_deadline_refusal",
                "explicit retry naming the settled attempt refuses DEADLINE_EXCEEDED (11) per \
                 the matrix; both subjects check terminality first and refuse ALREADY_TERMINAL \
                 (18) instead — the row accepts either named code and records which \
                 (deviation note, never faked as 11)"
                    .into(),
            ),
            (
                "obligation_persistence",
                "deadline settlement never erases the declared obligation: the work view \
                 keeps the terminal state, attempt and deadline after the retry refusal"
                    .into(),
            ),
            (
                "named_gap",
                "no subject hook can hold the application callback past the deadline \
                 (scenario-matrix-g4.md hook-free statistical variant); deterministic \
                 settlement is exercised by the short-deadline leg below"
                    .into(),
            ),
        ],
    )?;
    let mut iterations = Vec::new();
    for (position, size) in sizes.iter().enumerate() {
        let index = (position + 1) as u32;
        let iteration_dir = scenario_dir.join(format!("iteration-{index}"));
        let session = setup_session(context, &iteration_dir, server, client)?;
        let work = "0:0:1";
        let seed = context.seed ^ (u64::from(index) * 0x9e37_79b9);
        let input = oracle::dataset(seed, *size);
        let input_sha256 = oracle::sha256_hex(&input);
        let input_path = artifacts.join(format!("input-{index}.bin"));
        fs::write(&input_path, &input)?;
        events.append(
            "",
            None,
            Some(work),
            Some(1),
            None,
            Some(ArtifactRef {
                path: format!("artifacts/input-{index}.bin"),
                len: input.len() as u64,
                sha256: input_sha256.clone(),
            }),
        )?;
        let declare = declare_sealed(&session, &mut events, seed, "declare", &[1])?;
        let admit = oracle::operation_hex(oracle::operation_id(seed, "admit", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&admit)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        let admit_child = session.fixture.spawn_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &session.connection,
            &[
                "admit",
                "--operation",
                &admit,
                "--declaration",
                &declare,
                "--work",
                work,
                "--input",
                &crate::path(&input_path),
                "--application",
                "chunk-copy/v2",
                "--mode",
                "2",
                "--output-count",
                "1",
                "--execution-ms",
                &EXECUTION_MS.to_string(),
            ],
        )?;
        let admitted = AuthorityFixture::wait_client_op(admit_child, OP_WAIT)?;
        let _receipt = require(&admitted, "RECEIPT", "admit operation")?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&admit)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        let (final_state, final_view) = loop {
            let stdout = session.watch(work)?;
            let state = parse_state(&stdout)?;
            if (5..=8).contains(&state) {
                break (state, stdout);
            }
            ensure!(
                Instant::now() < deadline,
                "g4-deadline-settlement iteration {index}: work {work} did not settle within \
                 {RECOVERY_TIMEOUT:?}\nlast view:\n{stdout}"
            );
            thread::sleep(Duration::from_millis(50));
        };
        fs::write(
            artifacts.join(format!("iteration-{index}-terminal-view.txt")),
            &final_view,
        )?;
        let diagnostic = view_diagnostic_code(&final_view)
            .map(|code| format!("{} ({code})", refusal_code_name(code)))
            .unwrap_or_else(|| "none".to_owned());
        let iteration = if final_state == 5 {
            let sha = read_output_verified(
                &session,
                &mut events,
                work,
                1,
                &input,
                &input_sha256,
                &artifacts,
                &format!("iteration-{index}-output.bin"),
            )?;
            DeadlineIteration {
                index,
                input_len: *size,
                winning_order: "publication",
                final_state,
                diagnostic,
                read_outcome: format!("result read byte-exact (sha256={sha})"),
            }
        } else if final_state == 6 {
            ensure!(
                view_diagnostic_code(&final_view) == Some(11),
                "g4-deadline-settlement iteration {index}: deadline-won work must carry the \
                 DEADLINE_EXCEEDED (11) diagnostic, got {diagnostic}\n{final_view}"
            );
            if client == Subject::Rust {
                let deadline_at = parse_field_u64(&final_view, "deadline")?
                    .context("deadline-won terminal view did not report a deadline")?;
                let terminal_at = parse_field_u64(&final_view, "terminal_at")?
                    .context("deadline-won terminal view did not report terminal_at")?;
                ensure!(
                    terminal_at >= deadline_at,
                    "deadline-won work settled before its deadline: terminal_at={terminal_at} \
                     deadline={deadline_at}"
                );
            }
            let read_outcome = expect_named_read_refusal(
                &session,
                &artifacts,
                work,
                &format!("iteration-{index}-read-refusal.txt"),
            )?;
            DeadlineIteration {
                index,
                input_len: *size,
                winning_order: "deadline",
                final_state,
                diagnostic,
                read_outcome,
            }
        } else {
            bail!(
                "g4-deadline-settlement iteration {index}: illegal terminal state \
                 {final_state} ({})\n{final_view}",
                state_code_name(&final_state.to_string())
            );
        };
        events.append(
            "OBSERVATION_JOURNALED",
            None,
            Some(work),
            Some(1),
            None,
            None,
        )?;
        iterations.push(iteration);
        session.server.stop()?;
    }

    // Deterministic leg in its own authority: a copy/v2 admission whose
    // deadline (150 ms) the 12 MiB copy cannot meet settles FAILED with the
    // DEADLINE_EXCEEDED diagnostic; the explicit retry then refuses a named
    // code and the declared obligation persists. Retry the leg on fresh
    // works if the scheduler let the copy finish inside the deadline.
    const LEG_EXECUTION_MS: u64 = 150;
    let leg_dir = scenario_dir.join("deterministic-leg");
    let session = setup_session(context, &leg_dir, server, client)?;
    let declare = declare_sealed(
        &session,
        &mut events,
        context.seed,
        "declare-leg",
        &[1, 2, 3],
    )?;
    let mut retry_refusal = None;
    for leg in 1u64..=3 {
        if retry_refusal.is_some() {
            break;
        }
        let work = format!("0:0:{leg}");
        let seed = context.seed ^ (leg * 0x9e37_79b9);
        let input = oracle::dataset(seed, CLIENT_KILL_INPUT_LEN);
        let input_path = artifacts.join(format!("leg-input-{leg}.bin"));
        fs::write(&input_path, &input)?;
        let admit = oracle::operation_hex(oracle::operation_id(seed, "admit", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&admit)?),
            Some(&work),
            Some(1),
            None,
            None,
        )?;
        let admit_child = session.fixture.spawn_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &session.connection,
            &[
                "admit",
                "--operation",
                &admit,
                "--declaration",
                &declare,
                "--work",
                &work,
                "--input",
                &crate::path(&input_path),
                "--application",
                "copy/v2",
                "--execution-ms",
                &LEG_EXECUTION_MS.to_string(),
            ],
        )?;
        let admitted = AuthorityFixture::wait_client_op(admit_child, OP_WAIT)?;
        let _receipt = require(&admitted, "RECEIPT", "admit operation")?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&admit)?),
            Some(&work),
            Some(1),
            None,
            None,
        )?;
        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        let (final_state, final_view) = loop {
            let stdout = session.watch(&work)?;
            let state = parse_state(&stdout)?;
            if (5..=8).contains(&state) {
                break (state, stdout);
            }
            ensure!(
                Instant::now() < deadline,
                "g4-deadline-settlement leg {leg}: work {work} did not settle within \
                 {RECOVERY_TIMEOUT:?}\nlast view:\n{stdout}"
            );
            thread::sleep(Duration::from_millis(25));
        };
        if final_state != 6 || view_diagnostic_code(&final_view) != Some(11) {
            // The copy finished inside the short deadline; retry the leg.
            continue;
        }
        fs::write(
            artifacts.join(format!("leg-{leg}-terminal-view.txt")),
            &final_view,
        )?;
        // Explicit retry naming the settled attempt: the matrix expects
        // DEADLINE_EXCEEDED (11); both subjects check terminality first and
        // refuse ALREADY_TERMINAL (18). Accept either, record which.
        let retry_op = oracle::operation_hex(oracle::operation_id(seed, "retry", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&retry_op)?),
            Some(&work),
            Some(1),
            None,
            None,
        )?;
        let retry_text = expect_failure(
            &session,
            &artifacts,
            &format!("leg-{leg}-retry-refusal.txt"),
            &[
                "retry",
                "--operation",
                &retry_op,
                "--work",
                &work,
                "--expected-attempt",
                "1",
            ],
        )?;
        let named = refusal_named_line(&retry_text, &["DEADLINE_EXCEEDED", "ALREADY_TERMINAL"])
            .with_context(|| {
                format!(
                    "retry after deadline settlement must name DEADLINE_EXCEEDED (11) or \
                     ALREADY_TERMINAL (18)\n{retry_text}"
                )
            })?;
        let code = transcript_named_code(&retry_text)
            .context("retry refusal transcript carries no named code")?;
        events.append(
            "",
            Some(hex_to_id(&retry_op)?),
            Some(&work),
            Some(1),
            Some(code),
            None,
        )?;
        // The declared obligation persists: the view still shows the work,
        // its terminal state, attempt and deadline after the refusal.
        let persisted = session.watch(&work)?;
        ensure!(
            parse_state(&persisted)? == 6,
            "retry refusal erased the settled work view:\n{persisted}"
        );
        ensure!(
            view_line(&persisted)? == view_line(&final_view)?,
            "retry refusal changed the settled work view:\n{persisted}\noriginal:\n{final_view}"
        );
        retry_refusal = Some(format!(
            "work {work}: retry refused {named}; settled view persists byte-identical"
        ));
    }
    let retry_refusal = retry_refusal.context(
        "no deterministic leg settled at its deadline in three 12 MiB / 150 ms attempts",
    )?;
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
    ];
    let mut publication_wins = 0usize;
    let mut deadline_wins = 0usize;
    for iteration in &iterations {
        publication_wins += usize::from(iteration.winning_order == "publication");
        deadline_wins += usize::from(iteration.winning_order == "deadline");
        observed.push((
            Box::leak(format!("iteration_{}_input_len", iteration.index).into_boxed_str()),
            iteration.input_len.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_winning_order", iteration.index).into_boxed_str()),
            iteration.winning_order.into(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_final_state", iteration.index).into_boxed_str()),
            format!(
                "{} ({})",
                iteration.final_state,
                state_code_name(&iteration.final_state.to_string())
            ),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_diagnostic", iteration.index).into_boxed_str()),
            iteration.diagnostic.clone(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_read", iteration.index).into_boxed_str()),
            iteration.read_outcome.clone(),
        ));
    }
    observed.push((
        "orders_observed",
        format!("publication:{publication_wins},deadline:{deadline_wins}"),
    ));
    observed.push(("retry_after_deadline_refusal", retry_refusal));
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

fn parse_replacement_attempt(receipt_stdout: &str) -> Result<u64> {
    for needle in ["replacement_attempt: Id(", "replacementAttempt="] {
        if let Some(rest) = receipt_stdout.split(needle).nth(1) {
            return rest
                .split([')', ',', ']'])
                .next()
                .context("replacement attempt unterminated")?
                .trim()
                .parse()
                .context("replacement attempt is not decimal");
        }
    }
    bail!("retry receipt does not render a replacement attempt:\n{receipt_stdout}")
}

fn g4_stale_attempt_retry(context: &ScenarioContext) -> Result<()> {
    run_g4_row(
        context,
        "g4-stale-attempt-retry",
        g4_stale_attempt_retry_direction,
    )
}

/// Hold bound for the attempt-2 pause of g4-stale-attempt-retry, well above
/// the stale-retry round trip it covers.
const STALE_HOLD_DEADLINE_MS: u64 = 60_000;

/// Input length of the hook-free attempt-2 window on the Rust server: the
/// negotiated object limit, so the copy under attempt 2 runs for as long as
/// the subject allows an input to be.
const STALE_RETRY_WINDOW_INPUT_LEN: usize = 16 * 1024 * 1024;

/// Why the Rust server direction has no deterministic attempt-2 hold: the
/// Rust fixture hooks accept `pause` only at the three reply-pair boundaries.
const STALE_RETRY_RUST_HOLD_GAP: &str = "named gap: the Rust subject's fixture hooks accept \
    pause only at SESSION_COMMITTED, DECLARATION_COMMITTED and ADMISSION_COMMITTED \
    (src/v2/fixture.rs REPLY_PAIRS; a pause at EXECUTION_CLAIMED is rejected at schedule \
    parse), so attempt 2 is kept live by a 16 MiB copy (the negotiated object limit) rather \
    than a hold; a deterministic hold needs a server-crate hook extension";

/// Schedule rows that hold attempt 2 live at EXECUTION_CLAIMED for the
/// stale-retry round trip. The Java FixtureMain pauses at ANY committed
/// boundary and consumes one `pause` row per reached boundary, so the first
/// row holds attempt 1's claim (released as soon as it is reached) and the
/// second holds attempt 2's; the `release` row between them is the
/// driver-side ordering rule of interface-v1. The Rust subject rejects a
/// pause outside its reply pairs, so its direction gets no rows and keeps
/// attempt 2 live with the object-limit input instead (a named gap).
fn stale_retry_hold_rows(
    context: &ScenarioContext,
    id: &str,
    server: Subject,
) -> Vec<schedule::ScheduleRow> {
    match server {
        Subject::Rust => Vec::new(),
        Subject::Java => {
            let row = |action: schedule::Action| schedule::ScheduleRow {
                run_id: context.run_id.clone(),
                scenario_id: id.to_owned(),
                target: "server".into(),
                boundary: "EXECUTION_CLAIMED".into(),
                action,
                seed: context.seed,
                deadline_ms: STALE_HOLD_DEADLINE_MS,
            };
            vec![
                row(schedule::Action::Pause),
                row(schedule::Action::Release),
                row(schedule::Action::Pause),
            ]
        }
    }
}

/// g4-stale-attempt-retry: retry retry-copy/v2 to attempt 2 (the attempt-1
/// application outcome is retryable), then attempt a SECOND retry naming
/// expected-attempt 1 with a new operation id while attempt 2 is live. The
/// stale expected-attempt refuses CONFLICT (7); replaying the FIRST retry
/// operation returns its original receipt without advancing the fence again;
/// the work ends at exactly attempt 2, never 3.
///
/// Attempt 2 is held live deterministically on the Java server: a schedule
/// row pauses the server at EXECUTION_CLAIMED for attempt 2 (see
/// [`stale_retry_hold_rows`]), the stale retry is sent into the hold, the
/// CONFLICT is observed, and only then is the hold released. Both authorities
/// check terminal state before the attempt mismatch, so without the hold a
/// fast copy answers ALREADY_TERMINAL instead (the milestone-17b
/// rust-client/java-server result). The Rust server has no such hook and
/// keeps attempt 2 live with a 16 MiB copy; the mechanism used is recorded in
/// expected.tsv and observed.tsv per direction.
fn g4_stale_attempt_retry_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g4-stale-attempt-retry";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let hold_rows = stale_retry_hold_rows(context, id, server);
    let (session, events_path, hold_mechanism) = if hold_rows.is_empty() {
        enforce_no_fault_schedule(context, id)?;
        let session = setup_session(context, scenario_dir, server, client)?;
        (
            session,
            scenario_dir.join("events.tsv"),
            format!(
                "hook-free {STALE_RETRY_WINDOW_INPUT_LEN}-byte copy window ({STALE_RETRY_RUST_HOLD_GAP})"
            ),
        )
    } else {
        let hooked = setup_hooked(
            context,
            scenario_dir,
            id,
            server,
            client,
            &hold_rows,
            "schedule.tsv",
            true,
        )?;
        let (session, events_path) = split_hooked(hooked);
        (
            session,
            events_path,
            "schedule pause at EXECUTION_CLAIMED for attempt 2 (second pause row), released \
             after the stale retry's CONFLICT"
                .to_owned(),
        )
    };
    let held = !hold_rows.is_empty();
    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let input_len = if held {
        INPUT_LEN
    } else {
        STALE_RETRY_WINDOW_INPUT_LEN
    };
    let input = oracle::dataset(context.seed, input_len);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("application", "retry-copy/v2".into()),
            ("mode", "0".into()),
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            ("attempt_2_hold", hold_mechanism.clone()),
            (
                "attempt_1_outcome",
                "retryable → state AWAITING_RETRY(2)".into(),
            ),
            (
                "fence_sequence",
                "retry R1 (expected 1) → attempt 2; stale retry R2 \
                 (expected 1, new op id, attempt 2 live) → CONFLICT (7) \"retry attempt \
                 changed\"; replay R1 → identical receipt, fence not advanced"
                    .into(),
            ),
            (
                "terminal",
                "SUCCEEDED(5) under exactly attempt 2, never 3".into(),
            ),
        ],
    )?;
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
    let admit_child = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &[
            "admit",
            "--operation",
            &admit,
            "--declaration",
            &declare,
            "--work",
            "0:0:1",
            "--input",
            &crate::path(&input_path),
            "--application",
            "retry-copy/v2",
        ],
    )?;
    let admitted = AuthorityFixture::wait_client_op(admit_child, OP_WAIT)?;
    let _receipt = require(&admitted, "RECEIPT", "admit operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    // Attempt 1's claim is the FIRST pause row on a held server: release it
    // as soon as the subject records it, then clear the release so the
    // second pause row (attempt 2) holds again.
    if held {
        wait_subject_record(&events_path, "EXECUTION_CLAIMED", KILL_TIMEOUT)
            .context("subject never reached EXECUTION_CLAIMED for attempt 1")?;
        write_release(&events_path, "EXECUTION_CLAIMED")?;
    }

    // Attempt 1 reports retryable: the work parks in AWAITING_RETRY (state 2).
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let awaiting = loop {
        let stdout = session.watch("0:0:1")?;
        let state = parse_state(&stdout)?;
        if state == 2 {
            break stdout;
        }
        ensure!(
            state != 6 && state != 5,
            "g4-stale-attempt-retry: attempt 1 must park in AWAITING_RETRY, got state {state}\n\
             {stdout}"
        );
        ensure!(
            Instant::now() < deadline,
            "g4-stale-attempt-retry: work did not reach AWAITING_RETRY within {RECOVERY_TIMEOUT:?}"
        );
        thread::sleep(Duration::from_millis(25));
    };
    fs::write(artifacts.join("awaiting-retry-view.txt"), &awaiting)?;
    if held {
        clear_release(&events_path, "EXECUTION_CLAIMED")?;
    }

    // R1: explicit retry naming expected attempt 1 fences attempt 1 and
    // admits attempt 2.
    let retry_one = oracle::operation_hex(oracle::operation_id(context.seed, "retry", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&retry_one)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let retry_out = session.op(&[
        "retry",
        "--operation",
        &retry_one,
        "--work",
        "0:0:1",
        "--expected-attempt",
        "1",
    ])?;
    let retry_stdout = require(&retry_out, "RECEIPT", "retry operation")?;
    ensure!(
        parse_replacement_attempt(&retry_stdout)? == 2,
        "retry receipt must name replacement attempt 2:\n{retry_stdout}"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&retry_one)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    // Attempt 2 live: on a held server the subject's second EXECUTION_CLAIMED
    // record is the evidence (the claim is committed and the worker is held
    // before the application runs); on the Rust server the watch must show
    // ACTIVE under attempt 2 and the stale retry races the copy.
    let attempt_2_live = if held {
        let deadline = Instant::now() + KILL_TIMEOUT;
        loop {
            if subject_record_count(&events_path, "EXECUTION_CLAIMED")? >= 2 {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "g4-stale-attempt-retry: attempt 2 was not claimed within {KILL_TIMEOUT:?}"
            );
            thread::sleep(Duration::from_millis(25));
        }
        "EXECUTION_CLAIMED recorded twice by the subject; attempt 2 held at its claim".to_owned()
    } else {
        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        loop {
            let stdout = session.watch("0:0:1")?;
            let state = parse_state(&stdout)?;
            let attempt = parse_attempt(&stdout)?;
            if state == 1 && attempt == 2 {
                break;
            }
            ensure!(
                state != 6,
                "g4-stale-attempt-retry: attempt 2 fabricated a failure:\n{stdout}"
            );
            ensure!(
                Instant::now() < deadline,
                "g4-stale-attempt-retry: attempt 2 did not start within {RECOVERY_TIMEOUT:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }
        "watch showed ACTIVE(1) under attempt 2; the stale retry raced the copy".to_owned()
    };
    let retry_two = oracle::operation_hex(oracle::operation_id(context.seed, "retry", 2));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&retry_two)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let stale_text = expect_failure(
        &session,
        &artifacts,
        "stale-retry-refusal.txt",
        &[
            "retry",
            "--operation",
            &retry_two,
            "--work",
            "0:0:1",
            "--expected-attempt",
            "1",
        ],
    )?;
    let named = refusal_named_line(&stale_text, &["CONFLICT"]).with_context(|| {
        format!(
            "stale retry (expected-attempt 1, attempt 2 live) must name CONFLICT (7)\n\
                 {stale_text}"
        )
    })?;
    events.append(
        "",
        Some(hex_to_id(&retry_two)?),
        Some("0:0:1"),
        Some(1),
        Some(7),
        None,
    )?;
    if held {
        // The CONFLICT was observed with attempt 2 held; only now may it run.
        write_release(&events_path, "EXECUTION_CLAIMED")?;
    }

    // Attempt 2 now settles successfully under exactly attempt 2.
    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit, RECOVERY_TIMEOUT)?;
    let final_attempt = parse_attempt(&terminal)?;
    ensure!(
        final_attempt == 2,
        "g4-stale-attempt-retry: terminal attempt must be exactly 2, got {final_attempt}\n\
         {terminal}"
    );

    // Replaying the FIRST retry operation returns its original receipt
    // unchanged and never advances the fence to a third attempt.
    let replay_out = session.op(&[
        "retry",
        "--operation",
        &retry_one,
        "--work",
        "0:0:1",
        "--expected-attempt",
        "1",
    ])?;
    let replay_stdout = require(&replay_out, "RECEIPT", "retry replay")?;
    ensure!(
        replay_stdout.trim() == retry_stdout.trim(),
        "replayed retry returned a different receipt:\n{}\noriginal:\n{}",
        replay_stdout.trim(),
        retry_stdout.trim()
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&retry_one)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let after = session.watch("0:0:1")?;
    ensure!(
        view_line(&after)? == view_line(&terminal)?,
        "retry replay changed the settled work view:\n{after}\noriginal:\n{terminal}"
    );
    let _sha = read_output_verified(
        &session,
        &mut events,
        "0:0:1",
        2,
        &input,
        &input_sha256,
        &artifacts,
        "output.bin",
    )?;
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("attempt_1_state", "AWAITING_RETRY (2)".into()),
        ("r1_replacement_attempt", "2".into()),
        ("attempt_2_hold", hold_mechanism),
        ("attempt_2_live_evidence", attempt_2_live),
        ("stale_retry_refusal", named),
        ("terminal_attempt", final_attempt.to_string()),
        ("terminal_state", "SUCCEEDED (5)".into()),
        (
            "r1_replay",
            "identical receipt; settled view byte-identical; fence not advanced".into(),
        ),
        (
            "result_read",
            "attempt 2 byte-exact against the independent oracle".into(),
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
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// The outcome of one ancestor-fence iteration.
struct AncestorFenceIteration {
    index: u32,
    delay_ms: u64,
    winning_order: &'static str,
    parent_saw_cancelling: bool,
    parent_final_state: u64,
    child_scope: String,
    child_succeeded: usize,
    child_cancelled: usize,
    child_other: usize,
}

fn g4_ancestor_fence_publication(context: &ScenarioContext) -> Result<()> {
    run_g4_row(
        context,
        "g4-ancestor-fence-publication",
        g4_ancestor_fence_publication_direction,
    )
}

/// g4-ancestor-fence-publication: admit a mode-2 chunk-copy work (a
/// producer-1 child scope full of in-flight child publications) and fence the
/// ancestor with cancel-scope on scope 0. Expected: child publications are
/// fenced by the ancestor; the parent stays CANCELLING until the descendant
/// scope closes and ends CANCELLED — never FAILED; committed successful
/// descendants survive. The seeded pre-fence delays create the order
/// asymmetry (hook-free statistical variant).
fn g4_ancestor_fence_publication_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g4-ancestor-fence-publication";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    const INPUT_LEN: usize = 8 * 1024 * 1024;
    // Seeded pre-fence delays stagger how many child publications commit
    // before the ancestor fence lands. Every iteration owns a fresh
    // authority: the root-scope cancel fence is permanent for the session,
    // so a second iteration's declare/admit into scope 0 would refuse
    // CANCELLED "scope cancellation fence accepted".
    let delays = [0u64, 2, 10];
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("application", "chunk-copy/v2".into()),
            ("mode", "2".into()),
            ("input_len", INPUT_LEN.to_string()),
            ("iterations", delays.len().to_string()),
            (
                "iteration_pre_fence_delay_ms",
                delays
                    .iter()
                    .map(|delay| delay.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "session_model",
                "one fresh authority per iteration: the root-scope fence accepted by \
                 cancel-scope is permanent, so iterations cannot share a session"
                    .into(),
            ),
            (
                "parent_rule",
                "the accepted ancestor fence is never overwritten: the parent ends \
                 CANCELLED(7) — never FAILED(6); it stays CANCELLING(4) while descendants \
                 settle, or settles immediately when the fence lands before any descendant \
                 exists"
                    .into(),
            ),
            (
                "child_rule",
                "every descendant reaches exactly one terminal state within the bounded \
                 deadline; committed pre-fence successes (SUCCEEDED) survive; the rest \
                 settle CANCELLED — never FAILED"
                    .into(),
            ),
            (
                "winning_order_rule",
                "per iteration: any child committed SUCCEEDED before the fence → \
                 \"publication\" (partial); otherwise \"fence\""
                    .into(),
            ),
        ],
    )?;
    let mut iterations = Vec::new();
    for (position, delay_ms) in delays.iter().enumerate() {
        let index = (position + 1) as u32;
        let iteration_dir = scenario_dir.join(format!("iteration-{index}"));
        let session = setup_session(context, &iteration_dir, server, client)?;
        let work = "0:0:1";
        let seed = context.seed ^ (u64::from(index) * 0x9e37_79b9);
        let input = oracle::dataset(seed, INPUT_LEN);
        let input_path = artifacts.join(format!("input-{index}.bin"));
        fs::write(&input_path, &input)?;
        let declare = declare_sealed(&session, &mut events, seed, "declare", &[1])?;
        let admit = oracle::operation_hex(oracle::operation_id(seed, "admit", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&admit)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        let admit_child = session.fixture.spawn_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &session.connection,
            &[
                "admit",
                "--operation",
                &admit,
                "--declaration",
                &declare,
                "--work",
                work,
                "--input",
                &crate::path(&input_path),
                "--application",
                "chunk-copy/v2",
                "--mode",
                "2",
                "--output-count",
                "1",
            ],
        )?;
        let admitted = AuthorityFixture::wait_client_op(admit_child, OP_WAIT)?;
        let _receipt = require(&admitted, "RECEIPT", "admit operation")?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&admit)?),
            Some(work),
            Some(1),
            None,
            None,
        )?;
        if *delay_ms > 0 {
            thread::sleep(Duration::from_millis(*delay_ms));
        }
        // Fence the ancestor: cancel every work in scope 0 (the parent); the
        // descendant scope is fenced through the ancestor check at each
        // child publication.
        let fence_op = oracle::operation_hex(oracle::operation_id(seed, "scope-cancel", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&fence_op)?),
            None,
            None,
            None,
            None,
        )?;
        let fence_out = session.op(&["cancel-scope", "--operation", &fence_op, "--scope", "0"])?;
        let _fence_receipt = require(&fence_out, "RECEIPT", "cancel-scope operation")?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&fence_op)?),
            None,
            None,
            None,
            None,
        )?;

        // Poll the parent to its single terminal state; a fabricated FAILED
        // at any point is a row failure.
        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        let mut parent_saw_cancelling = false;
        let (parent_final_state, parent_final_view) = loop {
            let stdout = session.watch(work)?;
            let state = parse_state(&stdout)?;
            ensure!(
                state != 6,
                "g4-ancestor-fence-publication iteration {index}: the accepted fence was \
                 overwritten with FAILED:\n{stdout}"
            );
            parent_saw_cancelling |= state == 4;
            if state == 7 {
                break (state, stdout);
            }
            ensure!(
                state == 1 || state == 3 || state == 4,
                "g4-ancestor-fence-publication iteration {index}: unexpected parent state \
                 {state}\n{stdout}"
            );
            ensure!(
                Instant::now() < deadline,
                "g4-ancestor-fence-publication iteration {index}: parent {work} did not \
                 settle within {RECOVERY_TIMEOUT:?}\nlast view:\n{stdout}"
            );
            thread::sleep(Duration::from_millis(50));
        };
        // CANCELLING is recorded as evidence, not asserted: when the fence
        // lands before the expansion declares any descendant, the parent
        // settles terminal CANCELLED without passing through CANCELLING —
        // with no open descendants that is the spec-correct path.
        fs::write(
            artifacts.join(format!("iteration-{index}-parent-terminal-view.txt")),
            &parent_final_view,
        )?;
        events.append(
            "OBSERVATION_JOURNALED",
            None,
            Some(work),
            Some(1),
            None,
            None,
        )?;

        // Converge the descendant scope: every member terminal, membership
        // growth stopped by the fence (two consecutive identical pages).
        let child = parse_child_scope(&parent_final_view)?
            .map(|(scope, producer)| format!("{scope}:{producer}"))
            .unwrap_or_else(|| "none".to_owned());
        let mut child_counts = (0usize, 0usize, 0usize); // succeeded, cancelled, other
        if let Some((scope, _producer)) = parse_child_scope(&parent_final_view)? {
            let page_deadline = Instant::now() + RECOVERY_TIMEOUT;
            let mut previous: Option<(u64, Vec<(u64, String)>)> = None;
            loop {
                let page = observe_scope_page(&session, scope, 0, 256)?;
                let observation = &page.1;
                let snapshot = (observation.declared, observation.members.clone());
                let all_terminal = observation
                    .members
                    .iter()
                    .all(|(_, state)| state != "DECLARED");
                if all_terminal && previous.as_ref() == Some(&snapshot) {
                    child_counts = observation.members.iter().fold(
                        (0usize, 0usize, 0usize),
                        |(mut ok, mut cancelled, mut other), (_, state)| {
                            match state.as_str() {
                                "SUCCEEDED" => ok += 1,
                                "CANCELLED" => cancelled += 1,
                                _ => other += 1,
                            }
                            (ok, cancelled, other)
                        },
                    );
                    ensure!(
                        child_counts.2 == 0,
                        "g4-ancestor-fence-publication iteration {index}: descendants settled \
                         into unexpected terminal states: {:?}",
                        observation.members
                    );
                    break;
                }
                previous = Some(snapshot);
                ensure!(
                    Instant::now() < page_deadline,
                    "g4-ancestor-fence-publication iteration {index}: child scope {scope} did \
                     not converge within {RECOVERY_TIMEOUT:?}"
                );
                thread::sleep(Duration::from_millis(50));
            }
        }
        let winning_order = if child_counts.0 > 0 {
            "publication"
        } else {
            "fence"
        };
        iterations.push(AncestorFenceIteration {
            index,
            delay_ms: *delay_ms,
            winning_order,
            parent_saw_cancelling,
            parent_final_state,
            child_scope: child,
            child_succeeded: child_counts.0,
            child_cancelled: child_counts.1,
            child_other: child_counts.2,
        });
        session.server.stop()?;
    }

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
    ];
    let mut publication_wins = 0usize;
    let mut fence_wins = 0usize;
    for iteration in &iterations {
        publication_wins += usize::from(iteration.winning_order == "publication");
        fence_wins += usize::from(iteration.winning_order == "fence");
        observed.push((
            Box::leak(format!("iteration_{}_pre_fence_delay_ms", iteration.index).into_boxed_str()),
            iteration.delay_ms.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_winning_order", iteration.index).into_boxed_str()),
            iteration.winning_order.into(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_parent_final_state", iteration.index).into_boxed_str()),
            format!(
                "{} ({})",
                iteration.parent_final_state,
                state_code_name(&iteration.parent_final_state.to_string())
            ),
        ));
        observed.push((
            Box::leak(
                format!("iteration_{}_parent_cancelling_observed", iteration.index)
                    .into_boxed_str(),
            ),
            iteration.parent_saw_cancelling.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_child_scope", iteration.index).into_boxed_str()),
            iteration.child_scope.clone(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_children_succeeded", iteration.index).into_boxed_str()),
            iteration.child_succeeded.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_children_cancelled", iteration.index).into_boxed_str()),
            iteration.child_cancelled.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_children_other", iteration.index).into_boxed_str()),
            iteration.child_other.to_string(),
        ));
    }
    observed.push((
        "orders_observed",
        format!("publication:{publication_wins},fence:{fence_wins}"),
    ));
    observed.push((
        "never_observed",
        "parent FAILED(6); descendant FAILED; unsettled descendant at the deadline".into(),
    ));
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
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

fn g4_revocation_vs_publication(context: &ScenarioContext) -> Result<()> {
    g4_revocation_vs_publication_direction(
        context,
        &context.scenario_dir("g4-revocation-vs-publication"),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(
        context,
        "g4-revocation-vs-publication",
        "rust-client-java-server",
        |context, dir| {
            g4_revocation_vs_publication_direction(context, dir, Subject::Java, Subject::Rust)
        },
    )?;
    Ok(())
}

/// One revocation iteration's recorded outcome.
struct RevocationIteration {
    index: u32,
    input_len: usize,
    pre_revoke_state: u64,
    winning_order: &'static str,
    post_revoke_refusal: String,
    bytes_intact: bool,
    objects: u64,
}

/// g4-revocation-vs-publication: offline operator revocation
/// (`v2 revoke --owner alice --generation 1`, local action against existing
/// history, run while the server is stopped — the published CLI holds the
/// payload root exclusively) with a publication in flight or freshly
/// committed. Expected: the publication is fenced or completed-before-revoke
/// (recorded which), every session op on the revoked generation refuses
/// UNAUTHORIZED (3) after the restart, and transmitted bytes are not
/// retracted. Rust/rust only: neither Java CLI exposes an operator revoke
/// command (DurableHost.revoke is host-internal), a named gap the hooked
/// direction records as INCOMPLETE.
fn g4_revocation_vs_publication_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    if server == Subject::Java {
        bail!(
            "rust-client/java-server is a named gap for this row: neither Java CLI \
             (V2Main/ClientCommands) exposes an operator revoke command; \
             DurableHost.revoke(long) is host-internal only. The row evidence is the \
             rust-client/rust-server direction."
        );
    }
    let id = "g4-revocation-vs-publication";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    // Descending sizes: the large input is still publishing when the
    // revocation lands (fenced); the small input usually completed before it
    // (publication won). Both orders are legal and recorded.
    let sizes = [8 * 1024 * 1024, 256 * 1024, 64 * 1024];
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("iterations", sizes.len().to_string()),
            (
                "iteration_input_lens",
                sizes
                    .iter()
                    .map(|size| size.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "revocation_mechanism",
                "offline operator revocation per the matrix: the server is stopped, the \
                 operator runs rust `v2 revoke --owner alice --generation 1` (server/src/v2.rs \
                 Command::Revoke, prints SESSION_REVOKED), the server restarts on the same \
                 roots. The published CLI opens the payload store with an exclusive \
                 non-blocking flock (payload.rs RootLock) and refuses LIMIT_EXCEEDED \
                 \"payload root already owned\" while a server holds the root — the revoke \
                 cannot run against a live server through the published surface (recorded, \
                 not routed around). java = named gap (no operator revoke CLI; \
                 DurableHost.revoke host-internal)"
                    .into(),
            ),
            (
                "winning_order_rule",
                "per iteration, sampled by the watch immediately preceding the offline \
                 stop: terminal SUCCEEDED(5) → publication completed before the revocation; \
                 nonterminal → the revocation fenced the publication"
                    .into(),
            ),
            (
                "post_revoke_rule",
                "after the restart every session op on the revoked generation refuses \
                 UNAUTHORIZED (3) \"authority access denied\" — existing and new connections \
                 alike — and fresh session creation still answers (next creation sequence 2)"
                    .into(),
            ),
            (
                "bytes_not_retracted",
                "the transmitted input artifact and its independent oracle hash are \
                 re-verified after the revocation and restart, and remain intact"
                    .into(),
            ),
            (
                "observation_note",
                "post-revoke work state is not observable through the published CLIs \
                 (every session op refuses UNAUTHORIZED); fenced-vs-completed rests on the \
                 immediately-pre-offline watch sample plus object-dir metrics — recorded, \
                 not faked"
                    .into(),
            ),
        ],
    )?;
    let mut iterations = Vec::new();
    for (position, size) in sizes.iter().enumerate() {
        let index = (position + 1) as u32;
        // Each revocation terminates its session, so every iteration owns a
        // fresh authority under an iteration subdirectory.
        let iteration_dir = scenario_dir.join(format!("iteration-{index}"));
        let session = setup_session(context, &iteration_dir, server, client)?;
        let seed = context.seed ^ (u64::from(index) * 0x9e37_79b9);
        let input = oracle::dataset(seed, *size);
        let input_sha256 = oracle::sha256_hex(&input);
        let input_path = artifacts.join(format!("input-{index}.bin"));
        fs::write(&input_path, &input)?;
        events.append(
            "",
            None,
            Some("0:0:1"),
            Some(1),
            None,
            Some(ArtifactRef {
                path: format!("artifacts/input-{index}.bin"),
                len: input.len() as u64,
                sha256: input_sha256.clone(),
            }),
        )?;
        let declare = declare_sealed(&session, &mut events, seed, "declare", &[1])?;
        let admit = oracle::operation_hex(oracle::operation_id(seed, "admit", 1));
        events.append(
            "REQUEST_SENT",
            Some(hex_to_id(&admit)?),
            Some("0:0:1"),
            Some(1),
            None,
            None,
        )?;
        let admit_child = session.fixture.spawn_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &session.connection,
            &[
                "admit",
                "--operation",
                &admit,
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
        let admitted = AuthorityFixture::wait_client_op(admit_child, OP_WAIT)?;
        let admit_receipt = require(&admitted, "RECEIPT", "admit operation")?;
        events.append(
            "RECEIPT_VALIDATED",
            Some(hex_to_id(&admit)?),
            Some("0:0:1"),
            Some(1),
            None,
            None,
        )?;
        fs::write(
            artifacts.join(format!("iteration-{index}-admit-receipt.txt")),
            &admit_receipt,
        )?;

        // Sample the work state immediately before taking the server offline
        // (the race classification).
        let pre_revoke = session.watch("0:0:1")?;
        let pre_revoke_state = parse_state(&pre_revoke)?;
        fs::write(
            artifacts.join(format!("iteration-{index}-pre-offline-view.txt")),
            &pre_revoke,
        )?;

        // Offline operator revocation: the published `v2 revoke` opens the
        // payload store with an exclusive non-blocking flock (payload.rs
        // RootLock), so it refuses LIMIT_EXCEEDED "payload root already
        // owned" while a server holds the root. The operator action
        // therefore runs while the server is stopped — the matrix's
        // "offline operator revocation".
        session.server.kill()?;
        let mut command = session.fixture.base()?;
        command.push("revoke".into());
        command.extend(session.fixture.storage_args());
        command.push("--owner".into());
        command.push("alice".into());
        command.push("--generation".into());
        command.push(session.sequence.to_string());
        let revoked = crate::run_output_owned(&session.fixture.root, &command, OP_WAIT)?;
        let revoked_stdout = require(&revoked, "SESSION_REVOKED", "v2 revoke")?;
        events.append("REVOCATION_COMMITTED", None, None, None, None, None)?;
        fs::write(
            artifacts.join(format!("iteration-{index}-revoke-stdout.txt")),
            revoked_stdout,
        )?;
        let winning_order = if pre_revoke_state == 5 {
            "publication"
        } else {
            "fence"
        };

        // Restart on the same roots: every session op on the revoked
        // generation refuses UNAUTHORIZED (3) — existing and new connections
        // alike — while fresh session creation is not bricked.
        let restarted = session.fixture.start_server()?;
        let connection = session.fixture.connection_args(&restarted, "alice")?;
        let probe = session.fixture.run_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &connection,
            &["watch", "--work", "0:0:1"],
        )?;
        let probe_text = format!(
            "exit={}\nstdout:\n{}\nstderr:\n{}",
            probe.status,
            String::from_utf8_lossy(&probe.stdout),
            String::from_utf8_lossy(&probe.stderr),
        );
        fs::write(
            artifacts.join(format!("iteration-{index}-post-revoke-refusal.txt")),
            &probe_text,
        )?;
        ensure!(
            !probe.status.success(),
            "post-revoke session op was expected to refuse but exited zero\n{probe_text}"
        );
        let named = refusal_named_line(&probe_text, &["UNAUTHORIZED"]).with_context(|| {
            format!("post-revoke session op must name UNAUTHORIZED (3)\n{probe_text}")
        })?;
        events.append("", None, Some("0:0:1"), Some(1), Some(3), None)?;
        let next = session.fixture.next_sequence(&restarted, "alice")?;
        ensure!(
            next == 2,
            "post-revoke authority must offer the next creation sequence 2, got {next}"
        );
        let bytes_intact = oracle::sha256_hex(&fs::read(&input_path)?) == input_sha256;
        ensure!(
            bytes_intact,
            "g4-revocation-vs-publication iteration {index}: transmitted bytes were \
             retracted by the revocation"
        );
        let metrics = storage_metrics(&session.fixture.object_dir)?;
        iterations.push(RevocationIteration {
            index,
            input_len: *size,
            pre_revoke_state,
            winning_order,
            post_revoke_refusal: named,
            bytes_intact,
            objects: metrics.0,
        });
        restarted.stop()?;
    }

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
    ];
    let mut publication_wins = 0usize;
    let mut fence_wins = 0usize;
    for iteration in &iterations {
        publication_wins += usize::from(iteration.winning_order == "publication");
        fence_wins += usize::from(iteration.winning_order == "fence");
        observed.push((
            Box::leak(format!("iteration_{}_input_len", iteration.index).into_boxed_str()),
            iteration.input_len.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_pre_revoke_state", iteration.index).into_boxed_str()),
            format!(
                "{} ({})",
                iteration.pre_revoke_state,
                state_code_name(&iteration.pre_revoke_state.to_string())
            ),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_winning_order", iteration.index).into_boxed_str()),
            iteration.winning_order.into(),
        ));
        observed.push((
            Box::leak(
                format!("iteration_{}_post_revoke_refusal", iteration.index).into_boxed_str(),
            ),
            iteration.post_revoke_refusal.clone(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_bytes_intact", iteration.index).into_boxed_str()),
            iteration.bytes_intact.to_string(),
        ));
        observed.push((
            Box::leak(format!("iteration_{}_objects", iteration.index).into_boxed_str()),
            iteration.objects.to_string(),
        ));
    }
    observed.push((
        "orders_observed",
        format!("publication:{publication_wins},fence:{fence_wins}"),
    ));
    observed.push((
        "java_direction",
        "named gap: no operator revoke command on either Java CLI (V2Main/ \
         ClientCommands); DurableHost.revoke(long) host-internal only"
            .into(),
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;
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

fn g4_eventual_settlement(context: &ScenarioContext) -> Result<()> {
    run_g4_row(
        context,
        "g4-eventual-settlement",
        g4_eventual_settlement_direction,
    )
}

/// g4-eventual-settlement: admit a mode-2 chunk-copy work, kill the server
/// mid-expansion (SIGKILL), restart on the same roots, fence the ancestor,
/// and require bounded eventual settlement: every nonterminal descendant
/// reaches CANCELLED within the stated deadline, the status tree converges
/// to exactly one terminal state per work, and no cleanup/retirement runs
/// before root closure.
fn g4_eventual_settlement_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g4-eventual-settlement";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    let mut session = setup_session(context, scenario_dir, server, client)?;
    const INPUT_LEN: usize = 8 * 1024 * 1024;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("application", "chunk-copy/v2".into()),
            ("mode", "2".into()),
            ("input_len", INPUT_LEN.to_string()),
            (
                "settle_deadline_ms",
                format!("{}", RECOVERY_TIMEOUT.as_millis()),
            ),
            (
                "recovery",
                "SIGKILL mid-expansion, restart on the same roots, then the ancestor \
                 cancel-scope fence"
                    .into(),
            ),
            (
                "settlement_rule",
                "parent ends CANCELLED(7) — never FAILED(6); every descendant reaches \
                 exactly one terminal state (CANCELLED, or SUCCEEDED when committed before \
                 the fence) within the deadline"
                    .into(),
            ),
            (
                "no_cleanup_before_root_closure",
                "watch keeps answering and the object dir stays populated until the root \
                 closes; no retirement/EXPIRED refusals during settlement"
                    .into(),
            ),
        ],
    )?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
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
    let admit_child = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &[
            "admit",
            "--operation",
            &admit,
            "--declaration",
            &declare,
            "--work",
            "0:0:1",
            "--input",
            &crate::path(&input_path),
            "--application",
            "chunk-copy/v2",
            "--mode",
            "2",
            "--output-count",
            "1",
        ],
    )?;
    let admitted = AuthorityFixture::wait_client_op(admit_child, OP_WAIT)?;
    let _receipt = require(&admitted, "RECEIPT", "admit operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let metrics_before = storage_metrics(&session.fixture.object_dir)?;

    // Kill mid-expansion, restart on the same roots, rebind the client
    // connection to the restarted server (fresh port).
    session.server.kill()?;
    let restarted = session.fixture.start_server()?;
    session.connection = session.fixture.connection_args(&restarted, "alice")?;
    session.server = restarted;

    let fence_op = oracle::operation_hex(oracle::operation_id(context.seed, "scope-cancel", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&fence_op)?),
        None,
        None,
        None,
        None,
    )?;
    let fence_out = session.op(&["cancel-scope", "--operation", &fence_op, "--scope", "0"])?;
    let _fence_receipt = require(&fence_out, "RECEIPT", "cancel-scope operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&fence_op)?),
        None,
        None,
        None,
        None,
    )?;
    let fence_at = Instant::now();

    // Bounded eventual settlement of the parent.
    let deadline = fence_at + RECOVERY_TIMEOUT;
    let mut parent_saw_cancelling = false;
    let (parent_final_state, parent_final_view) = loop {
        let stdout = session.watch("0:0:1")?;
        let state = parse_state(&stdout)?;
        ensure!(
            state != 6,
            "g4-eventual-settlement: the accepted fence was overwritten with FAILED:\n{stdout}"
        );
        parent_saw_cancelling |= state == 4;
        if state == 7 {
            break (state, stdout);
        }
        ensure!(
            state == 1 || state == 3 || state == 4,
            "g4-eventual-settlement: unexpected parent state {state}\n{stdout}"
        );
        ensure!(
            Instant::now() < deadline,
            "g4-eventual-settlement: parent did not settle within {RECOVERY_TIMEOUT:?}\n\
             last view:\n{stdout}"
        );
        thread::sleep(Duration::from_millis(50));
    };
    let settlement_ms = fence_at.elapsed().as_millis() as u64;
    fs::write(
        artifacts.join("parent-terminal-view.txt"),
        &parent_final_view,
    )?;
    events.append(
        "OBSERVATION_JOURNALED",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    // Converge the descendant scope to exactly one terminal state per work.
    let child = parse_child_scope(&parent_final_view)?;
    let mut child_counts = (0usize, 0usize, 0usize);
    if let Some((scope, _producer)) = child {
        let page_deadline = Instant::now() + RECOVERY_TIMEOUT;
        let mut previous: Option<(u64, Vec<(u64, String)>)> = None;
        loop {
            let page = observe_scope_page(&session, scope, 0, 256)?;
            let observation = &page.1;
            let snapshot = (observation.declared, observation.members.clone());
            let all_terminal = observation
                .members
                .iter()
                .all(|(_, state)| state != "DECLARED");
            if all_terminal && previous.as_ref() == Some(&snapshot) {
                child_counts = observation.members.iter().fold(
                    (0usize, 0usize, 0usize),
                    |(mut ok, mut cancelled, mut other), (_, state)| {
                        match state.as_str() {
                            "SUCCEEDED" => ok += 1,
                            "CANCELLED" => cancelled += 1,
                            _ => other += 1,
                        }
                        (ok, cancelled, other)
                    },
                );
                ensure!(
                    child_counts.2 == 0,
                    "g4-eventual-settlement: descendants settled into unexpected terminal \
                     states: {:?}",
                    observation.members
                );
                break;
            }
            previous = Some(snapshot);
            ensure!(
                Instant::now() < page_deadline,
                "g4-eventual-settlement: child scope {scope} did not converge within \
                 {RECOVERY_TIMEOUT:?}"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    // No cleanup/retirement before root closure: watch still answers (the
    // polls above) and the object dir stays populated.
    let metrics_after = storage_metrics(&session.fixture.object_dir)?;
    ensure!(
        metrics_after.0 > 0,
        "g4-eventual-settlement: object dir emptied before root closure"
    );
    detach(&session)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("settlement_wall_ms", settlement_ms.to_string()),
        (
            "parent_final_state",
            format!(
                "{} ({})",
                parent_final_state,
                state_code_name(&parent_final_state.to_string())
            ),
        ),
        (
            "parent_cancelling_observed",
            parent_saw_cancelling.to_string(),
        ),
        (
            "child_scope",
            child
                .map(|(scope, producer)| format!("{scope}:{producer}"))
                .unwrap_or_else(|| "none".to_owned()),
        ),
        ("children_succeeded", child_counts.0.to_string()),
        ("children_cancelled", child_counts.1.to_string()),
        ("children_other", child_counts.2.to_string()),
        ("objects_before_restart", metrics_before.0.to_string()),
        ("objects_after_settlement", metrics_after.0.to_string()),
    ];
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
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
        policy_args: Vec::new(),
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

// ---------------------------------------------------------------------------
// G8: completion/detach rows (milestone 13)
// ---------------------------------------------------------------------------

/// The exact `ScopeSummary` a COVERAGE/COMPLETED observation commits to,
/// decoded subject-agnostically (rust Debug `ScopeSummary { .. }`, Java
/// record `ScopeSummary[..]`).
#[derive(Debug, PartialEq, Eq)]
struct G8SummaryView {
    scope: u64,
    producer: u64,
    parent: Option<[u64; 3]>,
    seal: [u8; 32],
    declared: u64,
    counts: [u64; 4],
    status_root: [u8; 32],
    closed_at: u64,
}

/// Remainder of `text` after `marker`, for strictly sequential decoding.
fn g8_after<'a>(text: &'a str, marker: &str) -> Result<&'a str> {
    text.split_once(marker)
        .map(|(_, rest)| rest)
        .with_context(|| format!("observation lacks {marker:?}:\n{text}"))
}

fn g8_take_u64(rest: &str) -> Result<(u64, &str)> {
    let len = rest.bytes().take_while(u8::is_ascii_digit).count();
    ensure!(len > 0, "expected a decimal in {rest:?}");
    let value = rest[..len].parse().context("decimal overflows u64")?;
    Ok((value, &rest[len..]))
}

/// Decode a rust Debug `Digest([b, b, ..])` decimal byte array.
fn g8_take_decimal_digest(rest: &str) -> Result<([u8; 32], &str)> {
    let rest = rest
        .strip_prefix("Digest([")
        .context("expected a rust Debug Digest([..])")?;
    let mut digest = [0u8; 32];
    let mut cursor = rest;
    for (index, byte) in digest.iter_mut().enumerate() {
        let (value, next) = g8_take_u64(cursor)?;
        ensure!(value <= u64::from(u8::MAX), "digest byte {index} overflows");
        *byte = value as u8;
        cursor = next
            .strip_prefix(if index == 31 { "])" } else { ", " })
            .with_context(|| format!("digest byte {index} lacks its separator"))?;
    }
    Ok((digest, cursor))
}

/// Decode a bare 64-digit hex digest (the Java rendering of `Digest`).
fn g8_take_hex_digest(rest: &str) -> Result<([u8; 32], &str)> {
    let len = rest.bytes().take_while(u8::is_ascii_hexdigit).count();
    ensure!(
        len == 64,
        "expected 64 hex digest digits, got {len} in {rest:?}"
    );
    let mut digest = [0u8; 32];
    for (index, pair) in rest.as_bytes()[..64].chunks_exact(2).enumerate() {
        digest[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok((digest, &rest[64..]))
}

fn g8_take_until<'a>(rest: &'a str, end: &str) -> Result<(&'a str, &'a str)> {
    let index = rest
        .find(end)
        .with_context(|| format!("observation lacks {end:?} in {rest:?}"))?;
    Ok((&rest[..index], &rest[index + end.len()..]))
}

/// Split a rendered record list into per-entry windows: each window runs
/// from one `marker` to the next (or to the end of the rendering), which is
/// safe because no field value can contain the marker.
fn g8_split_blocks<'a>(text: &'a str, marker: &str) -> Vec<&'a str> {
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(index) = rest.find(marker) {
        let after = &rest[index + marker.len()..];
        let end = after.find(marker).unwrap_or(after.len());
        blocks.push(&after[..end]);
        rest = &after[end..];
    }
    blocks
}

fn g8_parse_summary(text: &str) -> Result<G8SummaryView> {
    if text.contains("ScopeSummary {") {
        g8_parse_summary_rust(text)
    } else if text.contains("ScopeSummary[") {
        g8_parse_summary_java(text)
    } else {
        bail!("no ScopeSummary rendering in:\n{text}")
    }
}

fn g8_parse_summary_rust(text: &str) -> Result<G8SummaryView> {
    let rest = g8_after(text, "ScopeSummary {")?;
    let rest = g8_after(rest, "scope: Number(")?;
    let (scope, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), producer: Producer(")?;
    let (producer, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), parent: ")?;
    let (parent, rest) = if let Some(rest) = rest.strip_prefix("None") {
        (None, rest)
    } else {
        let rest = g8_after(rest, "Some(WorkKey { scope: Number(")?;
        let (a, rest) = g8_take_u64(rest)?;
        let rest = g8_after(rest, "), producer: Producer(")?;
        let (b, rest) = g8_take_u64(rest)?;
        let rest = g8_after(rest, "), entity: Id(")?;
        let (c, rest) = g8_take_u64(rest)?;
        let rest = g8_after(rest, ") })")?;
        (Some([a, b, c]), rest)
    };
    let rest = g8_after(rest, ", seal: ")?;
    let (seal, rest) = g8_take_decimal_digest(rest)?;
    let rest = g8_after(rest, ", declared: Number(")?;
    let (declared, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), counts: Counts { success: Number(")?;
    let (success, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), failure: Number(")?;
    let (failure, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), cancelled: Number(")?;
    let (cancelled, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), skipped: Number(")?;
    let (skipped, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ") }, status_root: ")?;
    let (status_root, rest) = g8_take_decimal_digest(rest)?;
    let rest = g8_after(rest, ", closed_at: Number(")?;
    let (closed_at, _rest) = g8_take_u64(rest)?;
    Ok(G8SummaryView {
        scope,
        producer,
        parent,
        seal,
        declared,
        counts: [success, failure, cancelled, skipped],
        status_root,
        closed_at,
    })
}

fn g8_parse_summary_java(text: &str) -> Result<G8SummaryView> {
    let rest = g8_after(text, "ScopeSummary[")?;
    let rest = g8_after(rest, "scope=")?;
    let (scope, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", producer=")?;
    let (producer, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", parent=")?;
    let (parent, rest) = if let Some(rest) = rest.strip_prefix("null") {
        (None, rest)
    } else {
        let rest = g8_after(rest, "WorkKey[scope=")?;
        let (a, rest) = g8_take_u64(rest)?;
        let rest = g8_after(rest, ", producer=")?;
        let (b, rest) = g8_take_u64(rest)?;
        let rest = g8_after(rest, ", entity=")?;
        let (c, rest) = g8_take_u64(rest)?;
        let rest = g8_after(rest, "]")?;
        (Some([a, b, c]), rest)
    };
    let rest = g8_after(rest, ", seal=")?;
    let (seal, rest) = g8_take_hex_digest(rest)?;
    let rest = g8_after(rest, ", declared=")?;
    let (declared, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", counts=Counts[success=")?;
    let (success, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", failure=")?;
    let (failure, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", cancelled=")?;
    let (cancelled, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", skipped=")?;
    let (skipped, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "], statusRoot=")?;
    let (status_root, rest) = g8_take_hex_digest(rest)?;
    let rest = g8_after(rest, ", closedAt=")?;
    let (closed_at, _rest) = g8_take_u64(rest)?;
    Ok(G8SummaryView {
        scope,
        producer,
        parent,
        seal,
        declared,
        counts: [success, failure, cancelled, skipped],
        status_root,
        closed_at,
    })
}

/// Decode one MANIFEST observation into the fields the driver's independent
/// hand-encoding needs. Rust renders `Manifest { .. }` Debug, Java the
/// `Manifest[..]` record form; both are decoded here.
fn g8_parse_manifest(text: &str) -> Result<oracle::ManifestView<'_>> {
    if text.contains("Manifest {") {
        g8_manifest_rust(text)
    } else if text.contains("Manifest[") {
        g8_manifest_java(text)
    } else {
        bail!("no Manifest rendering in:\n{text}")
    }
}

fn g8_manifest_rust(text: &str) -> Result<oracle::ManifestView<'_>> {
    let (authority, _) = g8_take_until(g8_after(text, "authority: IdentityLabel(\"")?, "\")")?;
    let (owner, _) = g8_take_until(g8_after(text, "owner: IdentityLabel(\"")?, "\")")?;
    let (generation, _) = g8_take_u64(g8_after(text, "generation: Id(")?)?;
    let rest = g8_after(text, "work: WorkKey {")?;
    let rest = g8_after(rest, "scope: Number(")?;
    let (scope, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), producer: Producer(")?;
    let (producer, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, "), entity: Id(")?;
    let (entity, _) = g8_take_u64(rest)?;
    let (attempt, _) = g8_take_u64(g8_after(text, "attempt: Id(")?)?;
    let (input_sha256, _) = g8_take_decimal_digest(g8_after(text, "input_sha256: ")?)?;
    let (committed_at, _) = g8_take_u64(g8_after(text, "committed_at: Number(")?)?;
    let (available_until, _) = g8_take_u64(g8_after(text, "available_until: Number(")?)?;
    let mut outputs = Vec::new();
    for block in g8_split_blocks(text, "Output {") {
        let (index, _) = g8_take_u64(g8_after(block, "index: OutputIndex(")?)?;
        let (length, _) = g8_take_u64(g8_after(block, "length: Number(")?)?;
        let (sha256, _) = g8_take_decimal_digest(g8_after(block, "sha256: ")?)?;
        let content = g8_after(block, "content_type: ")?;
        let content = content.strip_prefix("ApplicationLabel(").unwrap_or(content);
        let (content_type, _) = g8_take_until(content, "\")")?;
        let content_type = content_type
            .strip_prefix('"')
            .context("rust content_type must be quoted")?;
        let (locator, _) = g8_take_until(g8_after(block, "locator: ResultLocator(\"")?, "\")")?;
        outputs.push(oracle::OutputView {
            index,
            length,
            sha256,
            content_type,
            locator,
        });
    }
    ensure!(!outputs.is_empty(), "manifest renders no outputs:\n{text}");
    Ok(oracle::ManifestView {
        authority,
        owner,
        generation,
        work: [scope, producer, entity],
        attempt,
        input_sha256,
        committed_at,
        available_until,
        outputs,
    })
}

fn g8_manifest_java(text: &str) -> Result<oracle::ManifestView<'_>> {
    let (authority, _) = g8_take_until(g8_after(text, "authority=")?, ",")?;
    let (owner, _) = g8_take_until(g8_after(text, "owner=")?, ",")?;
    let (generation, _) = g8_take_u64(g8_after(text, "generation=")?)?;
    let rest = g8_after(text, "work=WorkKey[scope=")?;
    let (scope, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", producer=")?;
    let (producer, rest) = g8_take_u64(rest)?;
    let rest = g8_after(rest, ", entity=")?;
    let (entity, _) = g8_take_u64(rest)?;
    let (attempt, _) = g8_take_u64(g8_after(text, "attempt=")?)?;
    let (input_sha256, _) = g8_take_hex_digest(g8_after(text, "inputSha256=")?)?;
    let (committed_at, _) = g8_take_u64(g8_after(text, "committedAt=")?)?;
    let (available_until, _) = g8_take_u64(g8_after(text, "availableUntil=")?)?;
    let mut outputs = Vec::new();
    for block in g8_split_blocks(text, "Output[") {
        let (index, rest) = g8_take_u64(g8_after(block, "index=")?)?;
        let (length, rest) = g8_take_u64(g8_after(rest, ", length=")?)?;
        let (sha256, rest) = g8_take_hex_digest(g8_after(rest, ", sha256=")?)?;
        let (content_type, rest) = g8_take_until(g8_after(rest, ", contentType=")?, ", locator=")?;
        let (locator, _) = g8_take_until(g8_after(rest, "Locator[value=")?, "]")?;
        outputs.push(oracle::OutputView {
            index,
            length,
            sha256,
            content_type,
            locator,
        });
    }
    ensure!(!outputs.is_empty(), "manifest renders no outputs:\n{text}");
    Ok(oracle::ManifestView {
        authority,
        owner,
        generation,
        work: [scope, producer, entity],
        attempt,
        input_sha256,
        committed_at,
        available_until,
        outputs,
    })
}

/// Assert the committed summary fields the driver can independently recompute
/// (`closed_at` is the subject's own clock and is never asserted here).
#[allow(clippy::too_many_arguments)]
fn g8_summary_matches(
    view: &G8SummaryView,
    scope: u64,
    producer: u64,
    parent: Option<[u64; 3]>,
    declared: u64,
    counts: [u64; 4],
    seal: [u8; 32],
    status_root: [u8; 32],
) -> Result<()> {
    ensure!(
        view.scope == scope
            && view.producer == producer
            && view.parent == parent
            && view.declared == declared
            && view.counts == counts
            && view.seal == seal
            && view.status_root == status_root,
        "committed summary does not match the driver's independent recomputation:\n\
         observed: {view:?}\n\
         expected: scope={scope} producer={producer} parent={parent:?} declared={declared} \
         counts={counts:?} seal={} status_root={}",
        hex(&seal),
        hex(&status_root)
    );
    Ok(())
}

/// Observe one settled work's MANIFEST print and recompute the digest the
/// subject committed, verifying every driver-known field first.
fn g8_manifest_digest(
    session: &Session,
    artifacts: &Path,
    work: &str,
    expected_input_sha256: &str,
    expected_len: usize,
) -> Result<[u8; 32]> {
    let output = session.op(&["manifest", "--work", work, "--attempt", "1"])?;
    let stdout = require(&output, "MANIFEST", "manifest operation")?;
    fs::write(artifacts.join(format!("manifest-{work}.txt")), &stdout)?;
    let view = g8_parse_manifest(&stdout)?;
    let mut key = [0u64; 3];
    let parts: Vec<&str> = work.split(':').collect();
    ensure!(
        parts.len() == 3,
        "work key {work} does not render as scope:producer:entity"
    );
    for (index, part) in parts.iter().enumerate() {
        key[index] = part.parse().context("work key decimal")?;
    }
    ensure!(
        view.work == key,
        "manifest work {:?} != requested {work}",
        view.work
    );
    ensure!(
        view.authority == "issuer-a" && view.owner == "alice",
        "manifest identity {} / {} != issuer-a / alice",
        view.authority,
        view.owner
    );
    ensure!(
        view.generation == 1 && view.attempt == 1,
        "manifest generation/attempt {}/{:?} != 1/1",
        view.generation,
        view.attempt
    );
    ensure!(
        hex(&view.input_sha256) == expected_input_sha256,
        "manifest input sha256 {} != driver {}",
        hex(&view.input_sha256),
        expected_input_sha256
    );
    ensure!(
        view.outputs.len() == 1,
        "settled copy-family work must commit a one-object manifest: {stdout}"
    );
    let object = &view.outputs[0];
    ensure!(
        object.index == 0 && object.length == expected_len as u64,
        "manifest output index/length {}/{} != 0/{}",
        object.index,
        object.length,
        expected_len
    );
    ensure!(
        hex(&object.sha256) == expected_input_sha256,
        "manifest output sha256 {} != driver {}",
        hex(&object.sha256),
        expected_input_sha256
    );
    ensure!(
        !object.content_type.is_empty() && object.locator.starts_with("pipestream://"),
        "manifest output descriptor is not printable: {stdout}"
    );
    Ok(oracle::manifest_digest(&view))
}

/// Checkpoint one scope and verify the committed summary against the driver's
/// independent recomputation. Returns the decoded summary.
fn g8_checkpoint_verified(
    session: &Session,
    artifacts: &Path,
    name: &str,
    scope: u64,
    seal_hex: &str,
    expected: &G8SummaryView,
) -> Result<G8SummaryView> {
    // The client journal only accepts a coverage validation for a scope it has
    // observed (membership must be verified client-side before the returned
    // summary is trusted); page the scope first, like g1-mode1-branch.
    let (_page_stdout, page) = observe_scope_page(session, scope, 0, 256)?;
    ensure!(
        page.seal.as_deref() == Some(seal_hex),
        "checkpoint scope {scope}: committed seal {:?} != oracle {seal_hex}",
        page.seal
    );
    let output = session.op(&[
        "checkpoint",
        "--scope",
        &scope.to_string(),
        "--seal",
        seal_hex,
    ])?;
    let stdout = require(&output, "COVERAGE", "checkpoint operation")?;
    fs::write(artifacts.join(name), &stdout)?;
    let view = g8_parse_summary(&stdout)?;
    g8_summary_matches(
        &view,
        expected.scope,
        expected.producer,
        expected.parent,
        expected.declared,
        expected.counts,
        expected.seal,
        expected.status_root,
    )?;
    Ok(view)
}

/// Hex-decode a 64-digit oracle digest string into bytes for summary
/// comparison.
fn g8_hex_digest(digest: &str) -> Result<[u8; 32]> {
    let (bytes, rest) = g8_take_hex_digest(digest)?;
    ensure!(rest.is_empty(), "trailing data after digest {digest}");
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Row 1: g8-exact-root-complete
// ---------------------------------------------------------------------------

/// g8-exact-root-complete: a session holding a leaf copy, a reassemble/v2
/// mode-1 branch and a chunk-copy/v2 mode-2 branch settles bottom-up; the
/// driver checkpoints the child scopes then the root and completes. Every
/// committed field of every COVERAGE/COMPLETED summary — counters, seal,
/// status root — is recomputed independently from the MANIFEST prints
/// (hand-encoded CBOR, oracle.rs) and must match byte for byte.
fn g8_exact_root_complete(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g8-exact-root-complete",
        g8_exact_root_complete_direction,
    )
}

fn g8_exact_root_complete_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g8-exact-root-complete";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let session = setup_session(context, scenario_dir, server, client)?;

    let leaf_bytes = oracle::dataset(context.seed ^ 0x1eaf, G8_LEAF_INPUT_LEN);
    let mode1_bytes = oracle::dataset(context.seed, MODE1_PART_ONE_LEN + MODE1_PART_TWO_LEN);
    let part_one = &mode1_bytes[..MODE1_PART_ONE_LEN];
    let part_two = &mode1_bytes[MODE1_PART_ONE_LEN..];
    // 256 KiB divides into exactly four 64 KiB mode-2 chunks.
    let mode2_bytes = oracle::dataset(context.seed ^ 0x2bad, G8_MODE2_INPUT_LEN);
    let leaf_sha256 = oracle::sha256_hex(&leaf_bytes);
    let mode1_sha256 = oracle::sha256_hex(&mode1_bytes);
    let mode2_sha256 = oracle::sha256_hex(&mode2_bytes);
    let mode2_ids: Vec<u64> = (1..=4).collect();
    let scope1_seal =
        oracle::scope_seal_hex("issuer-a", "alice", 1, 1, 0, Some([0, 0, 2]), &[1, 2]);
    let scope2_seal =
        oracle::scope_seal_hex("issuer-a", "alice", 1, 2, 1, Some([0, 0, 3]), &mode2_ids);
    let root_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1, 2, 3]);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "root_members",
                "0:0:1 copy/v2 mode 0, 0:0:2 reassemble/v2 mode 1, 0:0:3 chunk-copy/v2 mode 2"
                    .into(),
            ),
            (
                "child_scope_1",
                "1:0 over [1,2] (mode-1, parent 0:0:2)".into(),
            ),
            (
                "child_scope_2",
                "2:1 over [1,2,3,4] (mode-2, parent 0:0:3)".into(),
            ),
            ("expected_child_1_seal_sha256", scope1_seal.clone()),
            ("expected_child_2_seal_sha256", scope2_seal.clone()),
            ("expected_root_seal_sha256", root_seal.clone()),
            (
                "expected_root_counts",
                "declared=3 success=3 failure=0 cancelled=0 skipped=0".into(),
            ),
            (
                "expected_child_1_counts",
                "declared=2 success=2 failure=0 cancelled=0 skipped=0".into(),
            ),
            (
                "expected_child_2_counts",
                "declared=4 success=4 failure=0 cancelled=0 skipped=0".into(),
            ),
            (
                "status_root",
                "driver recomputes every manifest digest from the MANIFEST prints and folds \
                 status leaves entity-ascending (oracle::status_root); committed COVERAGE and \
                 COMPLETED status roots must match"
                    .into(),
            ),
        ],
    )?;
    let leaf_path = artifacts.join("leaf-input.bin");
    fs::write(&leaf_path, &leaf_bytes)?;
    let mode1_path = artifacts.join("mode1-input.bin");
    fs::write(&mode1_path, &mode1_bytes)?;
    let part_one_path = artifacts.join("child-part-1.bin");
    fs::write(&part_one_path, part_one)?;
    let part_two_path = artifacts.join("child-part-2.bin");
    fs::write(&part_two_path, part_two)?;
    let mode2_path = artifacts.join("mode2-input.bin");
    fs::write(&mode2_path, &mode2_bytes)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
    ];

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    let root_declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1, 2, 3])?;
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit-leaf",
        &root_declare,
        "0:0:1",
        &leaf_path,
        "copy/v2",
        0,
        1,
    )?;
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit-mode1",
        &root_declare,
        "0:0:2",
        &mode1_path,
        "reassemble/v2",
        1,
        1,
    )?;
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit-mode2",
        &root_declare,
        "0:0:3",
        &mode2_path,
        "chunk-copy/v2",
        2,
        1,
    )?;
    observed.push((
        "root_admissions",
        "0:0:1 copy/v2 mode 0, 0:0:2 reassemble/v2 mode 1, 0:0:3 chunk-copy/v2 mode 2".into(),
    ));

    // Leaf settles on its own.
    watch_terminal(
        &session,
        &mut events,
        "0:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-leaf", 1)),
        WATCH_TIMEOUT,
    )?;

    // Mode-1 branch: the caller declares and admits the child parts.
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
    watch_terminal(
        &session,
        &mut events,
        "0:0:2",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-mode1", 1)),
        WATCH_TIMEOUT,
    )?;
    observed.push((
        "mode1_branch",
        "1:0:1, 1:0:2 settled; 0:0:2 settled after both".into(),
    ));

    // Mode-2 branch: the authority declares and executes the four chunks.
    let mut expansion = None;
    for _ in 0..300 {
        let (text, page) = observe_scope_page(&session, 2, 0, 256)?;
        if page.declared == 4 && page.seal.as_deref() == Some(scope2_seal.as_str()) {
            expansion = Some((text, page));
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let (expansion_text, expansion_page) = expansion
        .context("authority expansion did not declare the four mode-2 children within 15s")?;
    fs::write(artifacts.join("child-scope-2-page.txt"), &expansion_text)?;
    ensure!(
        expansion_page.producer == 1,
        "mode-2 child scope producer must be 1, got {}",
        expansion_page.producer
    );
    ensure!(
        expansion_page.membership_verified,
        "sealed mode-2 child scope page must verify membership"
    );
    observed.push((
        "mode2_expansion",
        format!(
            "scope 2:1 declared={} seal_matches_oracle=true",
            expansion_page.declared
        ),
    ));
    let mode2_children = expansion_page.declared;
    let mut samples = String::new();
    let mut rounds = 0;
    loop {
        rounds += 1;
        let mut child_states = Vec::new();
        let mut children_terminal = true;
        for entity in 1..=mode2_children {
            let stdout = session.watch(&format!("2:1:{entity}"))?;
            let state = parse_state(&stdout)?;
            if state != 5 {
                children_terminal = false;
            }
            child_states.push(state);
        }
        let parent_state = parse_state(&session.watch("0:0:3")?)?;
        samples.push_str(&format!(
            "round={rounds} children={child_states:?} parent={parent_state}\n"
        ));
        if parent_state == 5 {
            ensure!(
                children_terminal,
                "mode-2 parent settled while children were nonterminal: {child_states:?}"
            );
            break;
        }
        ensure!(
            rounds < 600,
            "mode-2 parent did not settle within the polling window\n{samples}"
        );
        thread::sleep(Duration::from_millis(100));
    }
    fs::write(artifacts.join("mode2-settle-samples.txt"), &samples)?;
    watch_terminal(
        &session,
        &mut events,
        "0:0:3",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-mode2", 1)),
        WATCH_TIMEOUT,
    )?;
    observed.push((
        "mode2_settle_order",
        format!("0:0:3 terminal after all 2:1 children terminal; rounds={rounds}"),
    ));

    // Independent recomputation: decode every committed manifest and fold the
    // status leaves entity-ascending exactly like the subject's StatusRoot.
    let leaf_digest = g8_manifest_digest(
        &session,
        &artifacts,
        "0:0:1",
        &leaf_sha256,
        G8_LEAF_INPUT_LEN,
    )?;
    let mode1_digest = g8_manifest_digest(
        &session,
        &artifacts,
        "0:0:2",
        &mode1_sha256,
        mode1_bytes.len(),
    )?;
    let mode2_digest = g8_manifest_digest(
        &session,
        &artifacts,
        "0:0:3",
        &mode2_sha256,
        G8_MODE2_INPUT_LEN,
    )?;
    let child1_digest = g8_manifest_digest(
        &session,
        &artifacts,
        "1:0:1",
        &oracle::sha256_hex(part_one),
        part_one.len(),
    )?;
    let child2_digest = g8_manifest_digest(
        &session,
        &artifacts,
        "1:0:2",
        &oracle::sha256_hex(part_two),
        part_two.len(),
    )?;
    let scope1_root = oracle::status_root(&[
        oracle::status_leaf([1, 0, 1], 5, 1, Some(child1_digest), None),
        oracle::status_leaf([1, 0, 2], 5, 1, Some(child2_digest), None),
    ]);
    let mut scope2_leaves = Vec::new();
    for (index, entity) in mode2_ids.iter().enumerate() {
        let start = index * MODE2_CHUNK_LEN;
        let end = ((index + 1) * MODE2_CHUNK_LEN).min(G8_MODE2_INPUT_LEN);
        let digest = g8_manifest_digest(
            &session,
            &artifacts,
            &format!("2:1:{entity}"),
            &oracle::sha256_hex(&mode2_bytes[start..end]),
            end - start,
        )?;
        scope2_leaves.push(oracle::status_leaf(
            [2, 1, *entity],
            5,
            1,
            Some(digest),
            None,
        ));
    }
    let scope2_root = oracle::status_root(&scope2_leaves);
    let scope0_root = oracle::status_root(&[
        oracle::status_leaf([0, 0, 1], 5, 1, Some(leaf_digest), None),
        oracle::status_leaf([0, 0, 2], 5, 1, Some(mode1_digest), Some(scope1_root)),
        oracle::status_leaf([0, 0, 3], 5, 1, Some(mode2_digest), Some(scope2_root)),
    ]);
    observed.push((
        "status_tree",
        "manifest digests recomputed from MANIFEST prints; scope 1 (2 leaves), scope 2 (4 \
         leaves), scope 0 (3 leaves with child_status_root folds) folded driver-side"
            .into(),
    ));

    // Settlement bottom-up: child checkpoints, root checkpoint, complete.
    // Every committed field is verified against the recomputation above.
    let scope1_expectation = G8SummaryView {
        scope: 1,
        producer: 0,
        parent: Some([0, 0, 2]),
        seal: g8_hex_digest(&scope1_seal)?,
        declared: 2,
        counts: [2, 0, 0, 0],
        status_root: scope1_root,
        closed_at: 0,
    };
    g8_checkpoint_verified(
        &session,
        &artifacts,
        "coverage-scope-1.txt",
        1,
        &scope1_seal,
        &scope1_expectation,
    )?;
    observed.push((
        "coverage_scope_1",
        "declared=2 counts=[2,0,0,0] seal+status_root match driver recomputation".into(),
    ));
    let scope2_expectation = G8SummaryView {
        scope: 2,
        producer: 1,
        parent: Some([0, 0, 3]),
        seal: g8_hex_digest(&scope2_seal)?,
        declared: mode2_children,
        counts: [mode2_children, 0, 0, 0],
        status_root: scope2_root,
        closed_at: 0,
    };
    g8_checkpoint_verified(
        &session,
        &artifacts,
        "coverage-scope-2.txt",
        2,
        &scope2_seal,
        &scope2_expectation,
    )?;
    observed.push((
        "coverage_scope_2",
        "declared=4 counts=[4,0,0,0] seal+status_root match driver recomputation".into(),
    ));
    let root_expectation = G8SummaryView {
        scope: 0,
        producer: 0,
        parent: None,
        seal: g8_hex_digest(&root_seal)?,
        declared: 3,
        counts: [3, 0, 0, 0],
        status_root: scope0_root,
        closed_at: 0,
    };
    let root_coverage = g8_checkpoint_verified(
        &session,
        &artifacts,
        "coverage-root.txt",
        0,
        &root_seal,
        &root_expectation,
    )?;
    observed.push((
        "coverage_root",
        "declared=3 counts=[3,0,0,0] seal+status_root match driver recomputation".into(),
    ));

    let completed = session.op(&["complete"])?;
    let completed_stdout = require(&completed, "COMPLETED", "complete operation")?;
    fs::write(artifacts.join("complete.txt"), &completed_stdout)?;
    let completed_view = g8_parse_summary(&completed_stdout)?;
    g8_summary_matches(
        &completed_view,
        0,
        0,
        None,
        3,
        [3, 0, 0, 0],
        g8_hex_digest(&root_seal)?,
        scope0_root,
    )?;
    ensure!(
        completed_view == root_coverage,
        "COMPLETED summary must replay the exact durable root coverage:\n\
         completed: {completed_view:?}\n coverage: {root_coverage:?}"
    );
    observed.push((
        "completed",
        "COMPLETED replays the exact root coverage (all fields equal)".into(),
    ));

    // The settled aggregate outputs stay byte-exact.
    let mode1_out = read_output_verified(
        &session,
        &mut events,
        "0:0:2",
        1,
        &mode1_bytes,
        &mode1_sha256,
        &artifacts,
        "mode1-output.bin",
    )?;
    let mode2_out = read_output_verified(
        &session,
        &mut events,
        "0:0:3",
        1,
        &mode2_bytes,
        &mode2_sha256,
        &artifacts,
        "mode2-output.bin",
    )?;
    ensure!(mode1_out == mode1_sha256 && mode2_out == mode2_sha256);

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
// Row 2: g8-child-cut-conflict
// ---------------------------------------------------------------------------

/// g8-child-cut-conflict: completion must cut the exact committed root. The
/// published CLIs submit only the journaled root coverage — neither can send
/// a child, altered or arbitrary summary (named CLI-surface gap; the server's
/// Conflict arms are documented from src/v2/authority/scopes.rs). The row
/// exercises every refusal layer the surface can reach: a checkpoint under a
/// wrong seal refuses INTEGRITY_ERROR at the authority; a complete with only
/// child coverage saved refuses in the client journal; the correct root
/// coverage completes exactly once and a second complete refuses.
fn g8_child_cut_conflict(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g8-child-cut-conflict",
        g8_child_cut_conflict_direction,
    )
}

fn g8_child_cut_conflict_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g8-child-cut-conflict";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let session = setup_session(context, scenario_dir, server, client)?;

    let parent_bytes = oracle::dataset(context.seed, MODE1_PART_ONE_LEN + MODE1_PART_TWO_LEN);
    let part_one = &parent_bytes[..MODE1_PART_ONE_LEN];
    let part_two = &parent_bytes[MODE1_PART_ONE_LEN..];
    let parent_sha256 = oracle::sha256_hex(&parent_bytes);
    let child_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 1, 0, Some([0, 0, 1]), &[1, 2]);
    let root_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1]);
    let mut altered_seal = g8_hex_digest(&root_seal)?;
    altered_seal[0] ^= 0xff;

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("parent_work", "0:0:1 reassemble/v2 mode 1".into()),
            ("child_scope", "1:0 over [1,2]".into()),
            ("expected_child_seal_sha256", child_seal.clone()),
            ("expected_root_seal_sha256", root_seal.clone()),
            (
                "submitted_summary_surface",
                "named gap: neither published CLI accepts a submitted ScopeSummary; the rust \
                 client replays the journaled root coverage (operations.rs cut()) and the java \
                 client its saved coverage; the authority Conflict arms (drain does not name \
                 attached root / root summary changed, scopes.rs complete_session) are not \
                 reachable through the CLIs — documented from code inspection"
                    .into(),
            ),
            (
                "wrong_seal_checkpoint",
                "named refusal of the wrong-seal checkpoint: INTEGRITY_ERROR (8) at the \
                 authority (rust client submits the cut), or NOT_READY at the java client \
                 journal (pre-submit seal-vs-membership check) — the request must never \
                 validate"
                    .into(),
            ),
            (
                "child_only_complete",
                "client-journal refusal: root coverage is the only submittable cut".into(),
            ),
            (
                "correct_complete",
                "COMPLETED once; a second complete replays the identical durable completion or \
                 refuses named — both prove no second completion claim"
                    .into(),
            ),
        ],
    )?;
    let parent_path = artifacts.join("parent-input.bin");
    fs::write(&parent_path, &parent_bytes)?;
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
    let root_declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit",
        &root_declare,
        "0:0:1",
        &parent_path,
        "reassemble/v2",
        1,
        1,
    )?;
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
    watch_terminal(
        &session,
        &mut events,
        "0:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1)),
        WATCH_TIMEOUT,
    )?;
    observed.push(("settled", "0:0:1 over 1:0:1 + 1:0:2".into()));

    // Record the sealed root membership in the journal first: the java client
    // refuses a checkpoint for a scope its journal has not observed, and the
    // wrong-seal probe below must reach the authority (named INTEGRITY_ERROR)
    // in every direction.
    let (_root_page, root_page) = observe_page(&session, 0, 256)?;
    ensure!(
        root_page.seal.as_deref() == Some(root_seal.as_str()),
        "committed root seal {:?} != oracle {root_seal}",
        root_page.seal
    );

    // Probe (a): checkpoint under an altered seal refuses named. The rust
    // client submits the cut and the authority answers INTEGRITY_ERROR; the
    // java journal pre-validates the submitted seal against its observed
    // membership and refuses NOT_READY before anything reaches the wire.
    // Both are named refusals of the wrong-seal checkpoint.
    let wrong = expect_failure(
        &session,
        &artifacts,
        "checkpoint-wrong-seal-refusal.txt",
        &["checkpoint", "--scope", "0", "--seal", &hex(&altered_seal)],
    )?;
    let named = refusal_named_line(&wrong, &["INTEGRITY_ERROR"])
        .map(|line| format!("authority: {line}"))
        .or_else(|| {
            refusal_named_line(&wrong, &["NOT_READY"])
                .map(|line| format!("client journal pre-submit: {line}"))
        })
        .with_context(|| {
            format!(
                "checkpoint under an altered seal must refuse named (authority \
                 INTEGRITY_ERROR or client-journal NOT_READY):\n{wrong}"
            )
        })?;
    observed.push(("checkpoint_wrong_seal", named));

    // Probe (b): saving only the child coverage, then complete, must refuse —
    // the client can only submit the journaled ROOT cut. The journal accepts
    // the child coverage only after observing the sealed child scope page
    // (membership verification, like g3-restart-same-roots).
    let (_child_page, child_page) = observe_scope_page(&session, 1, 0, 256)?;
    ensure!(
        child_page.seal.as_deref() == Some(child_seal.as_str()),
        "committed child seal {:?} != oracle {child_seal}",
        child_page.seal
    );
    let child_coverage = session.op(&["checkpoint", "--scope", "1", "--seal", &child_seal])?;
    require(&child_coverage, "COVERAGE", "child checkpoint")?;
    fs::write(
        artifacts.join("coverage-child-only.txt"),
        String::from_utf8_lossy(&child_coverage.stdout).into_owned(),
    )?;
    let child_only = expect_failure(
        &session,
        &artifacts,
        "complete-child-only-refusal.txt",
        &["complete"],
    )?;
    observed.push((
        "complete_child_only_coverage",
        format!(
            "refused before any completion claim (client journal layer): {}",
            child_only
                .lines()
                .find(|line| !line.is_empty())
                .unwrap_or("no output")
        ),
    ));
    // The refusals must not have disturbed the session.
    let view = session.watch("0:0:1")?;
    ensure!(
        parse_state(&view)? == 5,
        "session must still answer with the settled parent after the refusals"
    );
    observed.push(("session_open_after_refusals", "true".into()));

    // Correct settlement: root coverage, complete exactly once.
    let root_coverage = session.op(&["checkpoint", "--scope", "0", "--seal", &root_seal])?;
    let root_stdout = require(&root_coverage, "COVERAGE", "root checkpoint")?;
    fs::write(artifacts.join("coverage-root.txt"), &root_stdout)?;
    let root_view = g8_parse_summary(&root_stdout)?;
    g8_summary_matches(
        &root_view,
        0,
        0,
        None,
        1,
        [1, 0, 0, 0],
        g8_hex_digest(&root_seal)?,
        root_view.status_root,
    )?;
    let completed = session.op(&["complete"])?;
    let completed_stdout = require(&completed, "COMPLETED", "complete operation")?;
    fs::write(artifacts.join("complete.txt"), &completed_stdout)?;
    let completed_view = g8_parse_summary(&completed_stdout)?;
    ensure!(
        completed_view == root_view,
        "COMPLETED must replay the exact durable root coverage:\n\
         completed: {completed_view:?}\n coverage: {root_view:?}"
    );
    observed.push((
        "complete",
        "COMPLETED equals the saved root coverage".into(),
    ));

    // A second complete: the session is durably complete. The published
    // surface answers with the durable completion itself (the client replays
    // its journaled cut; the authority's idempotent replay echoes it) or
    // refuses named — either proves no second completion is claimed.
    let second = session.op(&["complete"])?;
    let second_text = transcript(&second);
    fs::write(artifacts.join("complete-second.txt"), &second_text)?;
    if second.status.success() {
        let stdout = require(&second, "COMPLETED", "second complete replay")?;
        let second_view = g8_parse_summary(&stdout)?;
        ensure!(
            second_view == completed_view,
            "a replayed second complete must equal the first completion:\n\
             second: {second_view:?}\n first: {completed_view:?}"
        );
        observed.push((
            "complete_second",
            "replayed the durable completion identical to the first COMPLETED — no second \
             claim"
                .into(),
        ));
    } else {
        let probe = probe_outcome(&second);
        ensure!(
            !probe.stdout.contains("COMPLETED"),
            "a refused second complete must never print a completion claim:\n{second_text}"
        );
        observed.push((
            "complete_second",
            format!(
                "refused without a completion claim: exit={} refusal={:?}",
                probe.exit, probe.refusal
            ),
        ));
    }

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
    ensure!(output_sha256 == parent_sha256);

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

/// Wrap one captured `expect_failure` transcript (the `exit=/stdout:/stderr:`
/// text) into a ProbeOutcome for classification.
fn probe_outcome_from_transcript(text: &str) -> ProbeOutcome {
    let exit = text
        .lines()
        .find_map(|line| line.strip_prefix("exit="))
        .unwrap_or("unknown")
        .to_owned();
    let stdout = text
        .split_once("stdout:\n")
        .map(|(_, rest)| rest.split_once("stderr:\n").map_or(rest, |(out, _)| out))
        .unwrap_or_default()
        .to_owned();
    let stderr = text
        .split_once("stderr:\n")
        .map(|(_, rest)| rest)
        .unwrap_or_default()
        .to_owned();
    let refusal = stderr.lines().find_map(|line| {
        if let Some(rest) = line.strip_prefix("connection lost: closed by peer: ") {
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
        success: exit == "exit status: 0",
        exit,
        stdout,
        stderr,
        refusal,
    }
}

// ---------------------------------------------------------------------------
// Row 3: g8-complete-with-pending
// ---------------------------------------------------------------------------

/// g8-complete-with-pending: completion while obligations are open must
/// refuse without claiming completion. (a) A mode-1 parent held in
/// WAITING_CHILDREN refuses `complete` (client journal layer — no root
/// coverage saved) and refuses a root checkpoint at the authority (named
/// wire code); nothing settles early. After bottom-up settlement the same
/// checkpoint + complete succeed. (b) A live transfer on the completing
/// connection is not expressible through the single-shot CLIs (named gap:
/// each op is its own connection and the client journal lease serializes a
/// journal to one process); the row approximates with a large result
/// download in flight on a second journal while `complete` runs.
fn g8_complete_with_pending(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g8-complete-with-pending",
        g8_complete_with_pending_direction,
    )
}

fn g8_complete_with_pending_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g8-complete-with-pending";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let session = setup_session(context, scenario_dir, server, client)?;

    let leaf_bytes = oracle::dataset(context.seed ^ 0x5eed, G8_LEAF_INPUT_LEN);
    let leaf_sha256 = oracle::sha256_hex(&leaf_bytes);
    let parent_bytes = oracle::dataset(context.seed, MODE1_PART_ONE_LEN + MODE1_PART_TWO_LEN);
    let part_one = &parent_bytes[..MODE1_PART_ONE_LEN];
    let part_two = &parent_bytes[MODE1_PART_ONE_LEN..];
    let root_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1, 2]);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "root_members",
                "0:0:1 copy/v2 (settled first), 0:0:2 reassemble/v2 mode 1 (held)".into(),
            ),
            ("expected_root_seal_sha256", root_seal.clone()),
            (
                "complete_pending_parent",
                "refusal, never a COMPLETED claim; parent must stay nonterminal".into(),
            ),
            (
                "checkpoint_open_root",
                "named wire refusal while obligations are open (actual code recorded)".into(),
            ),
            (
                "live_transfer_same_connection",
                "named gap: single-shot CLI ops each own a connection; approximated by a \
                 concurrent download on a second journal"
                    .into(),
            ),
            (
                "child_checkpoint",
                "descendant COVERAGE before root COVERAGE (bottom-up settlement; the authority \
                 refuses a root checkpoint while descendant coverage is missing)"
                    .into(),
            ),
            ("settled_complete", "root COVERAGE then COMPLETED".into()),
        ],
    )?;
    let leaf_path = artifacts.join("leaf-input.bin");
    fs::write(&leaf_path, &leaf_bytes)?;
    let parent_path = artifacts.join("parent-input.bin");
    fs::write(&parent_path, &parent_bytes)?;
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
    let root_declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1, 2])?;
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit-leaf",
        &root_declare,
        "0:0:1",
        &leaf_path,
        "copy/v2",
        0,
        1,
    )?;
    watch_terminal(
        &session,
        &mut events,
        "0:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-leaf", 1)),
        WATCH_TIMEOUT,
    )?;
    admit_modeled(
        &session,
        &mut events,
        context.seed,
        "admit-parent",
        &root_declare,
        "0:0:2",
        &parent_path,
        "reassemble/v2",
        1,
        1,
    )?;
    let held = session.watch("0:0:2")?;
    let held_state = parse_state(&held)?;
    ensure!(
        held_state == 3,
        "mode-1 parent with an un-admitted child scope must sit in WAITING_CHILDREN (3), got {held_state}"
    );
    observed.push(("parent_held_state", held_state.to_string()));

    // Record the sealed root membership in the journal up front: the open-root
    // checkpoint refusal below must come from the authority (named wire code),
    // not from the client-side membership check, and the settled checkpoint at
    // the end validates the returned summary against this observation.
    let (_root_page, root_page) = observe_page(&session, 0, 256)?;
    ensure!(
        root_page.seal.as_deref() == Some(root_seal.as_str()),
        "committed root seal {:?} != oracle {root_seal}",
        root_page.seal
    );

    // (a) complete against pending obligations: refusal, no completion claim.
    let complete_pending = expect_failure(
        &session,
        &artifacts,
        "complete-pending-refusal.txt",
        &["complete"],
    )?;
    let complete_probe = probe_outcome_from_transcript(&complete_pending);
    ensure!(
        !complete_probe.stdout.contains("COMPLETED"),
        "complete against pending obligations must never print a completion claim:\n{complete_pending}"
    );
    observed.push((
        "complete_pending",
        format!(
            "refused without a completion claim: refusal={:?}",
            complete_probe.refusal
        ),
    ));
    // The pending parent must not have moved.
    let after = parse_state(&session.watch("0:0:2")?)?;
    ensure!(
        after == 3,
        "complete refusal must not settle the pending parent (state {after})"
    );
    observed.push(("parent_state_after_refusal", after.to_string()));

    // A root checkpoint against the same open obligations refuses at the wire.
    let checkpoint_open = expect_failure(
        &session,
        &artifacts,
        "checkpoint-open-root-refusal.txt",
        &[
            "checkpoint",
            "--scope",
            "0",
            "--seal",
            &root_seal,
            "--wait-ms",
            "0",
        ],
    )?;
    let named = refusal_named_line(&checkpoint_open, &["NOT_READY", "WAIT_TIMEOUT", "CONFLICT"])
        .context("checkpoint of the open root must refuse with a named code\n{checkpoint_open}")?;
    observed.push(("checkpoint_open_root", named));
    ensure!(
        parse_state(&session.watch("0:0:2")?)? == 3,
        "checkpoint refusal must not settle the pending parent"
    );

    // Settle bottom-up; the same settlement path now succeeds.
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
    watch_terminal(
        &session,
        &mut events,
        "0:0:2",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-parent", 1)),
        WATCH_TIMEOUT,
    )?;
    // Settlement is bottom-up: descendant coverage before root coverage (the
    // authority refuses a root checkpoint while descendant coverage is
    // missing), then the same checkpoint + complete that refused while
    // obligations were open now succeed.
    let child_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 1, 0, Some([0, 0, 2]), &[1, 2]);
    let (_child_page, child_page) = observe_scope_page(&session, 1, 0, 256)?;
    ensure!(
        child_page.seal.as_deref() == Some(child_seal.as_str()),
        "committed child seal {:?} != oracle {child_seal}",
        child_page.seal
    );
    let child_coverage = session.op(&["checkpoint", "--scope", "1", "--seal", &child_seal])?;
    require(&child_coverage, "COVERAGE", "child checkpoint after settle")?;
    fs::write(
        artifacts.join("coverage-child.txt"),
        String::from_utf8_lossy(&child_coverage.stdout).into_owned(),
    )?;
    observed.push((
        "child_checkpoint_after_settle",
        "scope 1 COVERAGE (descendant coverage committed bottom-up)".into(),
    ));
    let root_coverage = session.op(&["checkpoint", "--scope", "0", "--seal", &root_seal])?;
    let root_stdout = require(&root_coverage, "COVERAGE", "root checkpoint")?;
    fs::write(artifacts.join("coverage-root.txt"), &root_stdout)?;
    let root_view = g8_parse_summary(&root_stdout)?;
    g8_summary_matches(
        &root_view,
        0,
        0,
        None,
        2,
        [2, 0, 0, 0],
        g8_hex_digest(&root_seal)?,
        root_view.status_root,
    )?;
    observed.push((
        "settled_coverage",
        "root COVERAGE after bottom-up settlement".into(),
    ));

    // (b) A large download in flight on a second journal while complete runs
    // on the main journal: both must succeed (the named same-connection gap
    // is recorded in expected.tsv).
    let select = session.op(&[
        "select",
        "--work",
        "0:0:1",
        "--attempt",
        "1",
        "--index",
        "0",
    ])?;
    require(&select, "REFERENCE", "select operation")?;
    let second_journal = scenario_dir.join("client").join("session-b.sqlite");
    {
        let mut command = session.fixture.client_base()?;
        command.push("init-client".into());
        command.extend(session.fixture.journal_args(&second_journal, "alice", 1));
        let init = crate::run_output_owned(&session.fixture.root, &command, OP_WAIT)?;
        require(
            &init,
            client.client_initialized_marker(),
            "v2 init-client (second journal)",
        )?;
    }
    // A retained reference is per-journal: journal-b must select the output
    // itself before its read, or the read refuses NOT_FOUND.
    let second_select = session.fixture.run_client_op_with(
        &second_journal,
        "alice",
        1,
        &[],
        &session.connection,
        &[
            "select",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
        ],
    )?;
    require(
        &second_select,
        "REFERENCE",
        "select operation (second journal)",
    )?;
    let read_args: Vec<String> = [
        "read",
        "--work",
        "0:0:1",
        "--attempt",
        "1",
        "--index",
        "0",
        "--output",
        &crate::path(&artifacts.join("concurrent-read.bin")),
    ]
    .iter()
    .map(|value| (*value).to_owned())
    .collect();
    let mut download = session.fixture.spawn_client_op(
        &second_journal,
        "alice",
        1,
        &session.connection,
        &op_refs(&read_args),
    )?;
    thread::sleep(Duration::from_millis(50));
    let in_flight = download
        .try_wait()
        .context("poll concurrent download")?
        .is_none();
    let completed = session.op(&["complete"])?;
    let completed_stdout = require(&completed, "COMPLETED", "complete with download in flight")?;
    fs::write(artifacts.join("complete.txt"), &completed_stdout)?;
    let completed_view = g8_parse_summary(&completed_stdout)?;
    ensure!(
        completed_view == root_view,
        "COMPLETED must replay the exact durable root coverage:\n\
         completed: {completed_view:?}\n coverage: {root_view:?}"
    );
    let download_output = AuthorityFixture::wait_client_op(download, RECOVERY_TIMEOUT)?;
    let download_text = transcript(&download_output);
    fs::write(artifacts.join("concurrent-read.txt"), &download_text)?;
    ensure!(
        download_output.status.success()
            && String::from_utf8_lossy(&download_output.stdout).contains("VERIFIED"),
        "concurrent download must finish byte-exact across the complete\n{download_text}"
    );
    let received = fs::read(artifacts.join("concurrent-read.bin"))?;
    ensure!(
        received == leaf_bytes,
        "concurrent download is not byte-exact: {} != {leaf_sha256}",
        hex(&Sha256::digest(&received))
    );
    observed.push((
        "concurrent_download_during_complete",
        format!("download_in_flight_at_complete={in_flight}; download VERIFIED byte-exact; complete COMPLETED"),
    ));

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
// Row 4: g8-detach-drains
// ---------------------------------------------------------------------------

const G8_LEAF_INPUT_LEN: usize = 4 * 1024;
const G8_MODE2_INPUT_LEN: usize = 256 * 1024;
const G8_DETACH_INPUT_LEN: usize = 12 * 1024 * 1024;

/// g8-detach-drains: detach is a barrier over the connection's accepted
/// facade ops. A 12 MiB download in flight on a second journal completes
/// byte-exact while the main journal detaches (detach wall time recorded);
/// the same-journal variant is refused client-side by the journal lease
/// (recorded as the surface evidence). Durable work keeps settling after
/// detach: a work admitted and detached unsettled keeps settling and is
/// watched to terminal success by the same journal reattached on a fresh
/// connection (a new session creation binds its own scope tree, so
/// cross-session observation is not the published surface).
fn g8_detach_drains(context: &ScenarioContext) -> Result<()> {
    run_three_directions(context, "g8-detach-drains", g8_detach_drains_direction)
}

fn g8_detach_drains_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g8-detach-drains";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let session = setup_session(context, scenario_dir, server, client)?;

    let big_bytes = oracle::dataset(context.seed, G8_DETACH_INPUT_LEN);
    let big_sha256 = oracle::sha256_hex(&big_bytes);
    let cont_bytes = oracle::dataset(context.seed ^ 0xc0ffee, INPUT_LEN);
    let cont_sha256 = oracle::sha256_hex(&cont_bytes);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("big_result", "0:0:1 copy/v2 12 MiB, settled".into()),
            (
                "detach_barrier",
                "detach prints DETACHED only after the connection's accepted ops drain; a \
                 concurrent download on a second journal completes byte-exact"
                    .into(),
            ),
            (
                "same_journal_concurrency",
                "client journal lease refuses a concurrent same-journal op (CONFLICT, \
                 ownership.rs) — recorded as the published-surface evidence"
                    .into(),
            ),
            (
                "durable_continuation",
                "0:0:2 admitted then detached unsettled keeps settling; the same journal \
                 reattaches on a fresh connection and watches it to terminal success (a new \
                 session creation would bind its own scope tree — scopes are per session \
                 generation — so cross-session observation is not the published surface)"
                    .into(),
            ),
        ],
    )?;
    let big_path = artifacts.join("big-input.bin");
    fs::write(&big_path, &big_bytes)?;
    let cont_path = artifacts.join("continuation-input.bin");
    fs::write(&cont_path, &cont_bytes)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
    ];

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let root_declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1, 2])?;
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit-big",
        &root_declare,
        "0:0:1",
        &big_path,
    )?;
    watch_terminal(
        &session,
        &mut events,
        "0:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-big", 1)),
        RECOVERY_TIMEOUT,
    )?;
    let select = session.op(&[
        "select",
        "--work",
        "0:0:1",
        "--attempt",
        "1",
        "--index",
        "0",
    ])?;
    require(&select, "REFERENCE", "select operation")?;

    // Concurrent download on a second journal, then detach on the main one.
    let second_journal = scenario_dir.join("client").join("session-b.sqlite");
    {
        let mut command = session.fixture.client_base()?;
        command.push("init-client".into());
        command.extend(session.fixture.journal_args(&second_journal, "alice", 1));
        let init = crate::run_output_owned(&session.fixture.root, &command, OP_WAIT)?;
        require(
            &init,
            client.client_initialized_marker(),
            "v2 init-client (second journal)",
        )?;
    }
    // A retained reference is per-journal: journal-b must select the output
    // itself before its read, or the read refuses NOT_FOUND.
    let second_select = session.fixture.run_client_op_with(
        &second_journal,
        "alice",
        1,
        &[],
        &session.connection,
        &[
            "select",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
        ],
    )?;
    require(
        &second_select,
        "REFERENCE",
        "select operation (second journal)",
    )?;
    let read_args: Vec<String> = [
        "read",
        "--work",
        "0:0:1",
        "--attempt",
        "1",
        "--index",
        "0",
        "--output",
        &crate::path(&artifacts.join("detach-read.bin")),
    ]
    .iter()
    .map(|value| (*value).to_owned())
    .collect();
    let mut download = session.fixture.spawn_client_op(
        &second_journal,
        "alice",
        1,
        &session.connection,
        &op_refs(&read_args),
    )?;
    thread::sleep(Duration::from_millis(50));
    let in_flight = download
        .try_wait()
        .context("poll concurrent download")?
        .is_none();
    let detach_start = Instant::now();
    let detached = session.op(&["detach"])?;
    let detach_ms = detach_start.elapsed();
    require(&detached, "DETACHED", "detach operation")?;
    let download_output = AuthorityFixture::wait_client_op(download, RECOVERY_TIMEOUT)?;
    let download_text = transcript(&download_output);
    fs::write(artifacts.join("detach-read.txt"), &download_text)?;
    ensure!(
        download_output.status.success()
            && String::from_utf8_lossy(&download_output.stdout).contains("VERIFIED"),
        "download concurrent with detach must complete byte-exact\n{download_text}"
    );
    let received = fs::read(artifacts.join("detach-read.bin"))?;
    ensure!(
        hex(&Sha256::digest(&received)) == big_sha256,
        "download concurrent with detach is not byte-exact"
    );
    observed.push((
        "detach_barrier",
        format!(
            "detach_wall_ms={} download_in_flight_at_detach={} download VERIFIED byte-exact",
            detach_ms.as_millis(),
            in_flight
        ),
    ));

    // Same-journal concurrency: while the read holds the journal lease (an
    // exclusive flock sidecar, ownership.rs), a second same-journal op
    // refuses CONFLICT client-side — the published surface serializes a
    // journal to one process at a time. The read needs a moment to open the
    // journal, so poll a watch probe until it names the lease refusal while
    // the read is still in flight.
    // A probe can occasionally win the flock before the read opens the
    // journal (the read then dies CONFLICT at startup), and the read refuses
    // to overwrite an existing output file, so each round gets its own file.
    let mut lease_evidence: Option<String> = None;
    let mut verified_round: Option<usize> = None;
    let mut verified_any: Option<usize> = None;
    let mut probe_count = 0u32;
    for round in 1..=4 {
        let read_name = format!("lease-read-round{round}.bin");
        let lease_read_args: Vec<String> = [
            "read",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
            "--output",
            &crate::path(&artifacts.join(&read_name)),
        ]
        .iter()
        .map(|value| (*value).to_owned())
        .collect();
        let mut lease_read = session.fixture.spawn_client_op(
            &session.journal,
            "alice",
            session.sequence,
            &session.connection,
            &op_refs(&lease_read_args),
        )?;
        // Grace for the read to open the journal and take the lease before
        // any probe contends with it (the read opens its journal within
        // ~100ms; the transfer itself may finish in under a second on a fast
        // direction, so probes start early and poll tightly).
        thread::sleep(Duration::from_millis(150));
        let probe_deadline = Instant::now() + Duration::from_secs(10);
        while lease_read
            .try_wait()
            .context("poll concurrent read for the lease probe")?
            .is_none()
            && Instant::now() < probe_deadline
        {
            let probe_output = session.op(&["watch", "--work", "0:0:1"])?;
            let probe = probe_outcome(&probe_output);
            probe_count += 1;
            fs::write(
                artifacts.join(format!("lease-probe-round{round}.txt")),
                probe.transcript(),
            )?;
            if !probe.success {
                let transcript = probe.transcript();
                let named = refusal_named_line(&transcript, &["CONFLICT"]).with_context(|| {
                    format!(
                        "a same-journal op while the lease is held must refuse named \
                             CONFLICT:\n{transcript}"
                    )
                })?;
                lease_evidence = Some(format!("{named} (watch probe refused while the read ran)"));
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let lease_read_output = AuthorityFixture::wait_client_op(lease_read, RECOVERY_TIMEOUT)?;
        fs::write(
            artifacts.join(format!("lease-read-round{round}.txt")),
            transcript(&lease_read_output),
        )?;
        let read_verified = lease_read_output.status.success()
            && String::from_utf8_lossy(&lease_read_output.stdout).contains("VERIFIED");
        if lease_evidence.is_some() && read_verified {
            verified_round = Some(round);
            break;
        }
        if read_verified && verified_any.is_none() {
            verified_any = Some(round);
        }
        lease_evidence = None;
    }
    // Direction-honest outcome: the rust client takes an exclusive flock
    // lease on its journal and refuses a concurrent same-journal op named
    // CONFLICT; the java client exposes no client-side lease, so probes run
    // alongside the read and all answer. Both are recorded; the read itself
    // must always verify byte-exact.
    let verified_round = verified_round
        .or(verified_any)
        .context("no lease-probe round produced a verified read")?;
    let lease_received = fs::read(artifacts.join(format!("lease-read-round{verified_round}.bin")))?;
    ensure!(
        hex(&Sha256::digest(&lease_received)) == big_sha256,
        "same-journal reattach read is not byte-exact"
    );
    let lease_note = match lease_evidence {
        Some(evidence) => format!("{evidence}; the read itself VERIFIED byte-exact"),
        None => format!(
            "no client-side journal lease on this client: {probe_count} same-journal watch \
             probes ran concurrently with the read and all answered; the read itself VERIFIED \
             byte-exact (the rust client refuses a concurrent same-journal op named CONFLICT — \
             exclusive flock sidecar, ownership.rs)"
        ),
    };
    observed.push(("same_journal_concurrent_op", lease_note));

    // Durable continuation: admit 0:0:2 unsettled, detach, then reattach the
    // same journal on a fresh connection: the work keeps settling after
    // detach and the reattached session watches it to terminal success and
    // reads it byte-exact. (A new session creation would bind its own scope
    // tree — scopes are per session generation — so it cannot name this work;
    // the reattach is the published continuation surface.)
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit-continuation",
        &root_declare,
        "0:0:2",
        &cont_path,
    )?;
    let detached_again = session.op(&["detach"])?;
    require(&detached_again, "DETACHED", "second detach operation")?;
    let sequence = session.fixture.next_sequence(&session.server, "alice")?;
    ensure!(
        sequence == 2,
        "one durable creation must bind sequence 1; the next session binds 2, got {sequence}"
    );
    watch_terminal(
        &session,
        &mut events,
        "0:0:2",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit-continuation", 1)),
        RECOVERY_TIMEOUT,
    )?;
    let continuation_sha256 = read_output_verified(
        &session,
        &mut events,
        "0:0:2",
        1,
        &cont_bytes,
        &cont_sha256,
        &artifacts,
        "continuation-read.bin",
    )?;
    ensure!(continuation_sha256 == cont_sha256);
    observed.push((
        "durable_continuation",
        "0:0:2 settled after detach; same journal reattached, watched state 5, read byte-exact; \
         the authority offers sequence 2 (detach consumed nothing)"
            .into(),
    ));

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
// Row 5: g8-half-close-preserves-responses
// ---------------------------------------------------------------------------

/// g8-half-close-preserves-responses: FIN on the request direction with
/// responses still pending must not strand committed responses (matrix row,
/// normative-clarifications item 5: FIN AFTER requesting detach; pre-FIN
/// responses including post-detach correlated refusals are delivered in
/// full). NAMED CLI-SURFACE/RAW-PROBE GAP: no raw v2 session client exists
/// in this tree — `conformance/src/extensions.rs` speaks only the
/// CAPABILITIES exchange over frozen bytes (no session binding, no CBOR
/// control frames, no detach) — and the single-shot CLIs model detach as a
/// single blocking op, so the FIN direction is not expressible. The row
/// records what the published surface CAN observe (the detach ack delivered
/// in full, post-detach op behavior) and defers the wire-level FIN probe to
/// the G6 raw-probe milestone.
fn g8_half_close_preserves_responses(context: &ScenarioContext) -> Result<()> {
    run_three_directions(
        context,
        "g8-half-close-preserves-responses",
        g8_half_close_preserves_responses_direction,
    )?;
    // The wire-level FIN probe deferred from milestone 13: the raw peer
    // requests session create + detach, half-closes the control send
    // direction immediately, and verifies every pre-FIN response (including
    // the post-detach ack) arrives in full.
    let scenario_dir = context.scenario_dir("g8-half-close-preserves-responses");
    g8_half_close_raw_direction(
        context,
        &scenario_dir.join("raw-rust-server"),
        Subject::Rust,
    )?;
    let java_raw = scenario_dir.join("raw-java-server");
    if context.java_jar.is_none() {
        fs::create_dir_all(&java_raw)?;
        fs::write(
            java_raw.join("INCOMPLETE"),
            b"no --java-jar provided; this direction was not run\n",
        )?;
        return Ok(());
    }
    g8_half_close_raw_direction(context, &java_raw, Subject::Java)
}

/// Raw half-close arm (Section 12.8 MUST): requests (session create, detach),
/// then FIN on the control send direction; the session receipt and the
/// DETACHED response must both arrive in full before the control receive
/// direction ends.
fn g8_half_close_raw_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "g8-half-close-preserves-responses";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let (fixture, owned_server) = setup_raw_probe(context, scenario_dir, server)?;
    let peer = Peer::new()?;

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[("half_close_must",
           "after requesting detach the client may finish its control send direction; every             response committed before the FIN (the session receipt and the DETACHED response,             including any post-detach correlated refusals) MUST be delivered in full — the             control receive direction ends only after them".into()),
          ("close", "clean close after the FIN is acknowledged (application code 0)".into())],
    )?;

    let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
    // Pipeline the whole sequence: create (request 1), detach (request 2),
    // then the half-close. No response is read until the FIN is sent.
    conn.send_control(FRAME_SESSION, &rawclient::session_create(1, 1))?;
    conn.send_control(FRAME_DRAIN, &rawclient::drain_detach(2))?;
    conn.finish_control()?;
    events.append("CONTROL_FIN_SENT", None, None, None, None, None)?;

    let receipt = conn.expect_control(FRAME_SESSION)?;
    let binding = rawclient::parse_binding(&receipt)?;
    events.append("BINDING_RECEIVED", None, None, None, None, None)?;
    let detached = conn.expect_control(FRAME_DRAIN)?;
    let detached_request = rawclient::parse_detached(&detached)?;
    ensure!(
        detached_request == 2,
        "detached response echoes request {detached_request}, expected 2"
    );
    events.append_as(
        server.name(),
        "server",
        "DETACH_ACKNOWLEDGED",
        None,
        None,
        None,
        None,
        None,
    )?;
    // The spec MUST: only after every pre-FIN response is the direction over.
    match conn.read_control()? {
        Frame::Fin => {}
        Frame::Control(found, _) => bail!(
            "control direction continued after the DETACHED response (frame {found});              the FIN discarded in-flight responses"
        ),
    }
    // Section 12.8: in response to the control FIN the server MAY initiate
    // graceful close (then it MUST close cleanly, code 0) or MAY leave
    // connection close to the client. Both are conforming; only a *server*
    // close that is not clean is a violation.
    let close = match conn.try_wait_closed(Duration::from_secs(3))? {
        Some(close) => {
            raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
            ensure!(
                close_code(&close) == Some(0),
                "half-close against {} server: expected clean close code 0 after \
                 the FIN, got {}",
                server.name(),
                close_text(&close)
            );
            format!("server-initiated {}", close_text(&close))
        }
        None => {
            conn.close_application(b"probe complete")?;
            "server left connection close to the client (MAY, Section 12.8); \
             probe closed the connection with application code 0"
                .to_string()
        }
    };
    fs::write(
        artifacts.join("pre-fin-responses.hex"),
        format!(
            "session-receipt: {}\ndetached: {}\n",
            hex(&receipt),
            hex(&detached)
        ),
    )?;

    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("server_subject", server.name().into()),
            ("client_subject", "rust (raw probe)".into()),
            ("alpn", "pipestream/2".into()),
            (
                "pre_fin_responses_in_full",
                format!(
                    "session receipt (generation {}) and DETACHED response received in                      full BEFORE the control receive direction ended; the post-detach                      ack survived the client's immediate FIN",
                    binding.generation
                ),
            ),
            (
                "post_fin_direction_end",
                "ordered FIN after the DETACHED response".into(),
            ),
            ("close", close.clone()),
        ],
    )?;
    stop_and_seal(context, scenario_dir, scenario_id, owned_server, events)
}

fn g8_half_close_preserves_responses_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let scenario_id = "g8-half-close-preserves-responses";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, client)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let session = setup_session(context, scenario_dir, server, client)?;

    let input = oracle::dataset(context.seed, INPUT_LEN);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("work", "0:0:1 copy/v2 settled".into()),
            (
                "half_close_surface",
                "the single-shot CLIs cannot send FIN on the request direction while \
                 responses are pending; the wire-level FIN probe is implemented by the \
                 raw peer in this row's raw-rust-server/ and raw-java-server/ \
                 directions (milestone 14)"
                    .into(),
            ),
            (
                "detach_ack_full_delivery",
                "DETACHED marker and exit 0: the ack (and everything committed before it) is \
                 delivered in full"
                    .into(),
            ),
            (
                "post_detach_op",
                "a post-detach op on the same journal is a fresh attach attempt; the actual \
                 outcome is recorded"
                    .into(),
            ),
        ],
    )?;
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("alpn", "pipestream/2".into()),
        (
            "cli_surface",
            "FIN-on-request-direction with pending responses is not expressible through the \
             published CLIs; the wire-level MUST is asserted by the raw peer directions \
             raw-rust-server/ and raw-java-server/"
                .into(),
        ),
    ];

    let binding = session.op(&["binding"])?;
    require(&binding, "BINDING", "client binding")?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    admit_input(
        &session,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
    )?;
    watch_terminal(
        &session,
        &mut events,
        "0:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1)),
        WATCH_TIMEOUT,
    )?;
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
    ensure!(output_sha256 == input_sha256);

    // The detach ack is delivered in full: the op exits zero and prints
    // DETACHED only after the server acknowledged the drain.
    let detached = session.op(&["detach"])?;
    let detached_text = transcript(&detached);
    fs::write(artifacts.join("detach.txt"), &detached_text)?;
    require(&detached, "DETACHED", "detach operation")?;
    observed.push(("detach_ack", "DETACHED delivered in full (exit 0)".into()));

    // Post-detach op on the same journal: a fresh attach attempt — the
    // closest the published surface gets to "responses after FIN".
    let post = session.op(&["watch", "--work", "0:0:1"])?;
    let post_probe = probe_outcome(&post);
    fs::write(
        artifacts.join("post-detach-watch.txt"),
        post_probe.transcript(),
    )?;
    observed.push((
        "post_detach_op",
        format!(
            "exit={} answered={} refusal={:?}",
            post_probe.exit, post_probe.success, post_probe.refusal
        ),
    ));

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
// Row 6: g8-timeout-no-completion-claim
// ---------------------------------------------------------------------------

/// g8-timeout-no-completion-claim: a lost complete reply must never produce
/// a client-side completion claim. Shape A (rust server, lost-reply variant):
/// the subject fixture gates drop-reply/pause/disconnect to the three
/// committed reply pairs, so a completion reply cannot be withheld without
/// process death (named surface gap, recorded in expected.tsv); the lost
/// reply is armed as a kill at COMPLETE_RESPONSE_SENT, the withheld op's
/// outcome is recorded honestly (a raced reply is possible; only the
/// no-false-claim invariant is asserted), and after a restart the re-driven
/// complete either replays the durable completion or refuses with a named
/// code — both prove no second completion is claimed. Shape B (kill variant,
/// rust and java servers): the same scheduled kill; the restart binds the
/// next sequence; the re-drive is classified the same dual way.
fn g8_timeout_no_completion_claim(context: &ScenarioContext) -> Result<()> {
    let id = "g8-timeout-no-completion-claim";
    g8_timeout_lost_reply_direction(
        context,
        &context.scenario_dir(id),
        Subject::Rust,
        Subject::Rust,
    )?;
    g8_timeout_kill_direction(
        context,
        &context.scenario_dir(id).join("kill-variant"),
        Subject::Rust,
        Subject::Rust,
    )?;
    run_hooked_direction(
        context,
        id,
        "kill-variant-rust-client-java-server",
        |context, direction_dir| {
            g8_timeout_kill_direction(context, direction_dir, Subject::Java, Subject::Rust)
        },
    )?;
    Ok(())
}

/// Settle one copy work and save the exact root coverage; shared prologue of
/// both timeout shapes.
fn g8_timeout_settle(
    context: &ScenarioContext,
    session: &Session,
    events: &mut EventWriter,
    artifacts: &Path,
) -> Result<(String, String)> {
    let declare = declare_sealed(session, events, context.seed, "declare", &[1])?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    admit_input(
        session,
        events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
    )?;
    watch_terminal(
        session,
        events,
        "0:0:1",
        &oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1)),
        WATCH_TIMEOUT,
    )?;
    let root_seal = oracle::scope_seal_hex("issuer-a", "alice", 1, 0, 0, None, &[1]);
    // The journal accepts the saved coverage only for an observed scope
    // (membership verification); page the sealed root before checkpointing.
    let (_root_page, root_page) = observe_page(session, 0, 256)?;
    ensure!(
        root_page.seal.as_deref() == Some(root_seal.as_str()),
        "committed root seal {:?} != oracle {root_seal}",
        root_page.seal
    );
    let coverage = session.op(&["checkpoint", "--scope", "0", "--seal", &root_seal])?;
    let coverage_stdout = require(&coverage, "COVERAGE", "root checkpoint")?;
    fs::write(artifacts.join("coverage-root.txt"), &coverage_stdout)?;
    Ok((root_seal, coverage_stdout))
}

/// Classify the re-driven complete after a lost reply: either the durable
/// completion replays (success, must equal the saved coverage) or the
/// authority refuses with a named code. Both prove the session was not
/// double-completed.
fn g8_classify_redrive(
    output: &Output,
    coverage_stdout: &str,
    artifacts: &Path,
    name: &str,
) -> Result<String> {
    let text = transcript(output);
    fs::write(artifacts.join(name), &text)?;
    let probe = probe_outcome(output);
    if probe.success {
        let stdout = require(output, "COMPLETED", "re-driven complete after a lost reply")?;
        let replayed = g8_parse_summary(&stdout)?;
        let saved = g8_parse_summary(coverage_stdout)?;
        ensure!(
            replayed == saved,
            "replayed COMPLETED must equal the saved root coverage:\n\
             replayed: {replayed:?}\n saved: {saved:?}"
        );
        Ok("idempotent replay of the durable completion (equals saved coverage)".to_owned())
    } else {
        ensure!(
            !text.contains("COMPLETED"),
            "a refused re-drive must never print a completion claim:\n{text}"
        );
        let (code, _line) = probe
            .refusal
            .with_context(|| format!("refused re-drive must name a Section 12.2 code:\n{text}"))?;
        Ok(format!(
            "session durably completed; re-drive refuses named {} ({code})",
            refusal_code_name(u64::from(code))
        ))
    }
}

/// Run one client op with a bounded wait that tolerates a hang: a lost
/// complete reply leaves the client drain-waiting with no connection death
/// to observe (a hard kill sends no CONNECTION_CLOSE), which can outlive the
/// driver op bound. On timeout the child is terminated and `hung` is
/// reported — a hung op prints nothing, so it never claims completion.
fn g8_bounded_op(
    session: &Session,
    operation: &[&str],
    wait: Duration,
) -> Result<(ProbeOutcome, bool)> {
    use std::io::Read as _;
    let args: Vec<String> = operation.iter().map(|value| (*value).to_owned()).collect();
    let mut child = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &op_refs(&args),
    )?;
    let deadline = Instant::now() + wait;
    let mut hung = false;
    let status = loop {
        if let Some(status) = child.try_wait().context("poll bounded op")? {
            break status;
        }
        if Instant::now() >= deadline {
            hung = true;
            child.kill().context("terminate the hung op")?;
            break child.wait().context("reap the hung op")?;
        }
        thread::sleep(Duration::from_millis(25));
    };
    let mut stdout = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout)
            .context("drain bounded op stdout")?;
    }
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr)
            .context("drain bounded op stderr")?;
    }
    let output = Output {
        status,
        stdout,
        stderr,
    };
    Ok((probe_outcome(&output), hung))
}

/// Lost-reply leg (rust server, row directory). The fixture only arms
/// drop-reply/pause/disconnect at the three committed reply pairs, so the
/// lost completion reply is modelled with the scheduled kill at
/// COMPLETE_RESPONSE_SENT: the completion is committed before the reply
/// boundary, the process dies right after the reply write is accepted by the
/// transport, and the client's op outcome (lost reply, or a raced-through
/// reply) is recorded honestly — only the no-false-claim invariant is
/// asserted. After a restart the re-driven complete must replay the durable
/// completion or refuse named.
fn g8_timeout_lost_reply_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g8-timeout-no-completion-claim";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "COMPLETE_RESPONSE_SENT",
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
    let (_root_seal, coverage_stdout) =
        g8_timeout_settle(context, &session, &mut events, &artifacts)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "lost_reply_surface",
                "named gap: the subject fixture gates drop-reply/pause/disconnect to the three \
                 committed reply pairs (SESSION/DECLARATION/ADMISSION_COMMITTED; fixture.rs \
                 REPLY_PAIRS) and completion replies never gate, so a complete reply cannot be \
                 withheld without process death; the lost reply is armed as a kill at \
                 COMPLETE_RESPONSE_SENT, committed before the reply boundary"
                    .into(),
            ),
            (
                "withheld_op",
                "outcome recorded honestly: a failed or hung op must not print COMPLETED; the \
                 reply write was accepted by the transport before the kill, so a raced-through \
                 reply printing COMPLETED is recorded, never asserted away; a lost reply can \
                 leave the client drain-waiting past the op bound — the driver then terminates \
                 the still-waiting client"
                    .into(),
            ),
            (
                "redrive",
                "post-restart watch answers; re-driven complete replays the durable completion \
                 or refuses named — either proves no second completion claim"
                    .into(),
            ),
        ],
    )?;

    let (withheld_probe, withheld_hung) = g8_bounded_op(&session, &["complete"], KILL_TIMEOUT)?;
    fs::write(
        artifacts.join("complete-withheld.txt"),
        withheld_probe.transcript(),
    )?;
    ensure!(
        !withheld_probe.success || withheld_probe.stdout.contains("COMPLETED"),
        "a complete that exited zero must print its completion claim:\n{}",
        withheld_probe.transcript()
    );
    ensure!(
        withheld_probe.success || !withheld_probe.stdout.contains("COMPLETED"),
        "a failed or hung complete must never print a completion claim:\n{}",
        withheld_probe.transcript()
    );
    wait_subject_record(&events_path, "COMPLETE_RESPONSE_SENT", KILL_TIMEOUT)
        .context("subject never reached the armed COMPLETE_RESPONSE_SENT boundary")?;
    let exit = session.server.wait_exit(KILL_TIMEOUT)?;
    let restarted = restart_hooked(
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
    let watch = restarted.op(&["watch", "--work", "0:0:1"])?;
    let watch_probe = probe_outcome(&watch);
    fs::write(
        artifacts.join("post-restart-watch.txt"),
        watch_probe.transcript(),
    )?;
    let redrive = restarted.op(&["complete"])?;
    let redrive_outcome = g8_classify_redrive(
        &redrive,
        &coverage_stdout,
        &artifacts,
        "complete-redrive.txt",
    )?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("server_subject", server.name().into()),
            ("client_subject", client.name().into()),
            ("alpn", "pipestream/2".into()),
            (
                "subject_exit_code",
                exit.status
                    .code()
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "terminated by signal".into()),
            ),
            (
                "subject_boundary_record",
                "COMPLETE_RESPONSE_SENT recorded before the exit".into(),
            ),
            (
                "withheld_op",
                if withheld_hung {
                    format!(
                        "hung awaiting the lost reply (terminated by the driver after the {}s \
                         bound); no completion claim printed; exit={}",
                        KILL_TIMEOUT.as_secs(),
                        withheld_probe.exit
                    )
                } else {
                    format!(
                        "exit={} answered={} refusal={:?}",
                        withheld_probe.exit, withheld_probe.success, withheld_probe.refusal
                    )
                },
            ),
            (
                "post_restart_watch",
                format!(
                    "exit={} answered={} refusal={:?}",
                    watch_probe.exit, watch_probe.success, watch_probe.refusal
                ),
            ),
            ("redrive_complete", redrive_outcome),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, restarted.server, events)
}

fn g8_timeout_kill_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g8-timeout-no-completion-claim";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let rows = [g2_schedule_row(
        context,
        id,
        "COMPLETE_RESPONSE_SENT",
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
    let (_root_seal, coverage_stdout) =
        g8_timeout_settle(context, &session, &mut events, &artifacts)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "kill_boundary",
                format!(
                    "COMPLETE_RESPONSE_SENT: the subject exits {} right after the reply \
                     boundary",
                    kill_exit_code(server)
                ),
            ),
            (
                "withheld_op",
                "outcome recorded honestly: a failed or hung op must not print COMPLETED; the \
                 reply write was accepted by the transport before the kill, so a raced-through \
                 reply is recorded, never asserted away; a lost reply can leave the client \
                 drain-waiting past the op bound — the driver then terminates the still-waiting \
                 client"
                    .into(),
            ),
            (
                "restart",
                "NEXT_SEQUENCE 2 (the creation is durable, no double alloc); re-driven \
                 complete replays or refuses named"
                    .into(),
            ),
        ],
    )?;

    let (lost_probe, lost_hung) = g8_bounded_op(&session, &["complete"], KILL_TIMEOUT)?;
    fs::write(artifacts.join("complete-lost.txt"), lost_probe.transcript())?;
    ensure!(
        !lost_probe.success || lost_probe.stdout.contains("COMPLETED"),
        "a complete that exited zero must print its completion claim:\n{}",
        lost_probe.transcript()
    );
    ensure!(
        lost_probe.success || !lost_probe.stdout.contains("COMPLETED"),
        "a failed or hung complete must never print a completion claim:\n{}",
        lost_probe.transcript()
    );
    wait_subject_record(&events_path, "COMPLETE_RESPONSE_SENT", KILL_TIMEOUT)
        .context("subject never reached the armed COMPLETE_RESPONSE_SENT boundary")?;
    let exit = session.server.wait_exit(KILL_TIMEOUT)?;
    ensure!(
        exit.status.code() == Some(kill_exit_code(server)),
        "the scheduled kill must exit with the subject kill code after the boundary record, \
         got {}",
        exit.status
    );
    let restarted = restart_hooked(
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
    let next = restarted
        .fixture
        .next_sequence(&restarted.server, "alice")?;
    ensure!(
        next == 2,
        "the durable creation must bind sequence 1 and offer 2 after the restart, got {next}"
    );
    let redrive = restarted.op(&["complete"])?;
    let redrive_outcome = g8_classify_redrive(
        &redrive,
        &coverage_stdout,
        &artifacts,
        "complete-redrive.txt",
    )?;
    write_kv(
        scenario_dir,
        "observed.tsv",
        &[
            ("server_subject", server.name().into()),
            ("client_subject", client.name().into()),
            ("alpn", "pipestream/2".into()),
            ("subject_exit_code", kill_exit_code(server).to_string()),
            (
                "subject_boundary_record",
                "COMPLETE_RESPONSE_SENT recorded before the exit".into(),
            ),
            (
                "withheld_op",
                if lost_hung {
                    format!(
                        "hung awaiting the lost reply (terminated by the driver after the {}s \
                         bound); no completion claim printed; exit={}",
                        KILL_TIMEOUT.as_secs(),
                        lost_probe.exit
                    )
                } else {
                    format!(
                        "exit={} answered={} refusal={:?}",
                        lost_probe.exit, lost_probe.success, lost_probe.refusal
                    )
                },
            ),
            ("next_sequence_after_restart", next.to_string()),
            ("redrive_complete", redrive_outcome),
        ],
    )?;
    stop_and_seal(context, scenario_dir, id, restarted.server, events)
}

// ---------------------------------------------------------------------------
// G6 raw wire-abuse rows (milestone 14). The driver itself acts as a raw QUIC
// peer: mTLS handshake, hand-framed deterministic CBOR, frozen malformed bytes
// replayed verbatim from test-vectors/v2/wire.tsv. Directions name the SERVER
// probed; the probe is always the rust conformance driver.
// ---------------------------------------------------------------------------

/// G6 rows probe one server per direction dir: the rust server in the row
/// directory, the java server in `java-server/` (INCOMPLETE marker without a
/// jar).
fn run_raw_directions(
    context: &ScenarioContext,
    row_id: &str,
    direction: fn(&ScenarioContext, &Path, Subject) -> Result<()>,
) -> Result<()> {
    let scenario_dir = context.scenario_dir(row_id);
    direction(context, &scenario_dir, Subject::Rust)?;
    let java_dir = scenario_dir.join("java-server");
    if context.java_jar.is_none() {
        fs::create_dir_all(&java_dir)?;
        fs::write(
            java_dir.join("INCOMPLETE"),
            b"no --java-jar provided; this direction was not run\n",
        )?;
        return Ok(());
    }
    direction(context, &java_dir, Subject::Java)
}

/// Fixture + server for a raw-probe direction. The rust CLI client is used
/// only for the authenticated readiness/next-sequence probes; the row traffic
/// itself is the raw peer.
fn setup_raw_probe(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<(AuthorityFixture, OwnedServer)> {
    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
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
    let sequence = fixture.next_sequence(&server, "alice")?;
    ensure!(
        sequence == 1,
        "fresh authority must report NEXT_SEQUENCE 1, got {sequence}"
    );
    Ok((fixture, server))
}

/// Connect as `principal`, offer the frozen capabilities vector, and read
/// the selection.
fn raw_negotiate_as(
    peer: &Peer,
    fixture: &AuthorityFixture,
    server: &OwnedServer,
    events: &mut EventWriter,
    scenario_artifacts: &Path,
    principal: &str,
) -> Result<RawConn> {
    let mut conn = peer.connect(&fixture.certs, principal, &server.address)?;
    let offer = rawclient::frozen("capabilities-offer")?;
    let offer_hex: String = offer.frame.iter().map(|b| format!("{b:02x}")).collect();
    let artifact = scenario_artifacts.join("capabilities-offer.hex");
    if !artifact.exists() {
        fs::write(&artifact, &offer_hex)?;
    }
    events.append(
        "REQUEST_SENT",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/capabilities-offer.hex".into(),
            len: offer_hex.len() as u64,
            sha256: oracle::sha256_hex(offer_hex.as_bytes()),
        }),
    )?;
    conn.send_frozen(&offer.frame)?;
    let body = conn
        .expect_control(FRAME_CAPABILITIES)
        .context("negotiate stage: capabilities selection read")?;
    let caps = rawclient::parse_capabilities(&body).context("capabilities selection parse")?;
    conn.set_caps(caps);
    Ok(conn)
}

/// Connect as the canonical alice principal.
fn raw_negotiate(
    peer: &Peer,
    fixture: &AuthorityFixture,
    server: &OwnedServer,
    events: &mut EventWriter,
    scenario_artifacts: &Path,
) -> Result<RawConn> {
    raw_negotiate_as(peer, fixture, server, events, scenario_artifacts, "alice")
}

/// Session create as request 1 on an open connection; returns the binding.
fn raw_create_session(conn: &mut RawConn) -> Result<rawclient::Binding> {
    conn.send_control(FRAME_SESSION, &rawclient::session_create(1, 1))?;
    rawclient::parse_binding(&conn.expect_control(FRAME_SESSION)?)
}

fn raw_declare(
    conn: &mut RawConn,
    request: u64,
    operation: &[u8; 16],
    scope: u64,
    entities: &[u64],
    seal: bool,
) -> Result<()> {
    conn.send_control(
        FRAME_SCOPE,
        &rawclient::scope_declare(request, operation, scope, entities, seal),
    )?;
    let receipt_request = rawclient::parse_declared(&conn.expect_control(FRAME_SCOPE)?)?;
    ensure!(
        receipt_request == request,
        "declaration receipt echoes request {receipt_request}, expected {request}"
    );
    Ok(())
}

fn raw_page(conn: &mut RawConn, request: u64, scope: u64) -> Result<(u64, Vec<(u64, u64)>)> {
    conn.send_control(FRAME_SCOPE, &rawclient::scope_page(request, scope))?;
    rawclient::parse_page(&conn.expect_control(FRAME_SCOPE)?)
}

fn raw_detach(conn: &mut RawConn, request: u64) -> Result<()> {
    conn.send_control(FRAME_DRAIN, &rawclient::drain_detach(request))?;
    let response = rawclient::parse_detached(&conn.expect_control(FRAME_DRAIN)?)?;
    ensure!(
        response == request,
        "detach response echoes request {response}, expected {request}"
    );
    Ok(())
}

/// Classify a peer close for observed.tsv.
fn close_text(close: &Close) -> String {
    match close {
        Close::Application(code, reason) => format!(
            "APPLICATION_CLOSE code=0x{code:03x} reason={:?}",
            String::from_utf8_lossy(reason)
        ),
        Close::Transport(reason) => format!("transport close ({reason})"),
    }
}

fn close_code(close: &Close) -> Option<u64> {
    match close {
        Close::Application(code, _) => Some(*code),
        Close::Transport(_) => None,
    }
}

fn expect_named_close(
    row: &str,
    probe: &str,
    server: Subject,
    close: &Close,
    expected: u64,
) -> Result<()> {
    ensure!(
        close_code(close) == Some(expected),
        "{row} {probe} against {} server: expected QUIC app close 0x{expected:03x}, \
         got {}\nfull close: {close:?}",
        server.name(),
        close_text(close)
    );
    Ok(())
}

fn raw_event_server_close(
    events: &mut EventWriter,
    server: Subject,
    boundary: &str,
    close: &Close,
) -> Result<()> {
    events.append_as(
        server.name(),
        "server",
        boundary,
        None,
        None,
        None,
        close_code(close).map(|code| code as u32),
        None,
    )
}

/// The frozen control refuse-rows and their wire position: 0 = the
/// capabilities position (first frame), 1 = session position (right after a
/// valid capabilities exchange), 2 = scope/work position (after a session
/// create receipt). Input-header and server-record roots are not control
/// frames; they are exercised by g6-stream-identity-and-fin (named gaps there
/// for the server-record roots).
const G6_CONTROL_REFUSE_VECTORS: &[(&str, u8, u64)] = &[
    ("caps-extra-position", 0, QUIC_FRAME_ERROR),
    ("caps-missing-lifetime", 0, QUIC_FRAME_ERROR),
    ("caps-control-too-small", 0, QUIC_FRAME_ERROR),
    ("caps-duplicate-profile", 0, QUIC_FRAME_ERROR),
    ("caps-required-not-supported", 0, QUIC_FRAME_ERROR),
    (
        "result-profile-without-durable",
        0,
        QUIC_EXTENSION_UNSUPPORTED,
    ),
    ("caps-idle-exceeds-lifetime", 0, QUIC_FRAME_ERROR),
    ("zero-request", 1, QUIC_FRAME_ERROR),
    ("generation-overflow", 1, QUIC_FRAME_ERROR),
    ("session-policy-zero", 1, QUIC_FRAME_ERROR),
    ("session-nonascii-owner", 1, QUIC_FRAME_ERROR),
    ("noncanonical-session-request", 1, QUIC_FRAME_ERROR),
    ("trailing-cbor-item", 1, QUIC_FRAME_ERROR),
    ("zero-operation-id", 2, QUIC_FRAME_ERROR),
    ("unsorted-declaration", 2, QUIC_FRAME_ERROR),
    ("empty-unsealed-batch", 2, QUIC_FRAME_ERROR),
    ("extra-work-key-position", 2, QUIC_FRAME_ERROR),
    ("unknown-work-state", 2, QUIC_FRAME_ERROR),
    ("inconsistent-success-without-input", 2, QUIC_FRAME_ERROR),
];

fn g6_canonical_violations(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(
        context,
        "g6-canonical-violations",
        g6_canonical_violations_direction,
    )
}

fn g6_canonical_violations_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "g6-canonical-violations";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let (fixture, owned_server) = setup_raw_probe(context, scenario_dir, server)?;

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "corpus",
                "19 frozen control refuse-rows of test-vectors/v2/wire.tsv; the \
                 driver checks each row's sha256 and replays the bytes verbatim \
                 (input-header roots and server-record roots are not control \
                 frames: two go to g6-stream-identity-and-fin, three are named \
                 gaps there)"
                    .into(),
            ),
            (
                "expected_scope",
                "connection-fatal: the server closes the connection with QUIC \
                 application code 0x201 (FRAME_ERROR) or 0x202 (the one \
                 EXTENSION_UNSUPPORTED vector), never a correlated refusal frame"
                    .into(),
            ),
            (
                "position_rule",
                "capabilities roots replay as the first frame; session roots \
                 after a valid capabilities exchange; scope/work roots after a \
                 session create receipt"
                    .into(),
            ),
        ],
    )?;
    fs::write(
        artifacts.join("capabilities-offer.hex"),
        rawclient::frozen("capabilities-offer")?
            .frame
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    )?;

    let peer = Peer::new()?;
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", "rust (raw probe)".into()),
        ("alpn", "pipestream/2".into()),
    ];
    let mut expected_rows: Vec<(&str, String)> = Vec::new();

    for (name, position, expected_code) in G6_CONTROL_REFUSE_VECTORS {
        // The authority caps concurrent connections globally; pace iterations so
        // a freed per-principal slot has time to become visible to the acceptor.
        thread::sleep(Duration::from_millis(300));
        let vector = rawclient::frozen(name)?;
        ensure!(
            vector.expectation == "refuse",
            "{name} is not a refuse vector"
        );
        let expected_wire = match *expected_code {
            QUIC_FRAME_ERROR => "FRAME_ERROR",
            QUIC_EXTENSION_UNSUPPORTED => "EXTENSION_UNSUPPORTED",
            other => bail!("no named refusal for code {other:#x}"),
        };
        expected_rows.push((name, format!("{expected_wire} at 0x{expected_code:03x}")));

        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        match position {
            0 => {}
            1 => {
                // Session position: the vector itself must be the next frame;
                // no valid session create precedes it.
            }
            2 => {
                let binding = raw_create_session(&mut conn)?;
                ensure!(
                    binding.request == 1,
                    "session receipt echoes request {}",
                    binding.request
                );
            }
            other => bail!("unknown vector position {other}"),
        }
        let sent_hex: String = vector.frame.iter().map(|b| format!("{b:02x}")).collect();
        fs::write(artifacts.join(format!("{name}.sent-hex")), &sent_hex)?;
        events.append(
            "REQUEST_SENT",
            None,
            None,
            None,
            None,
            Some(ArtifactRef {
                path: format!("artifacts/{name}.sent-hex"),
                len: sent_hex.len() as u64,
                sha256: oracle::sha256_hex(sent_hex.as_bytes()),
            }),
        )?;
        conn.send_frozen(&vector.frame)?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(scenario_id, name, server, &close, *expected_code)?;
        observed.push((
            Box::leak(name.to_string().into_boxed_str()),
            format!(
                "vector {name} ({}, position {position}): expected {expected_wire} \
                 connection-fatal; observed {}",
                vector.root,
                close_text(&close)
            ),
        ));
    }

    write_kv(scenario_dir, "expected-vectors.tsv", &expected_rows)?;
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, scenario_id, owned_server, events)
}

fn g6_direction_and_correlation(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(
        context,
        "g6-direction-and-correlation",
        g6_direction_and_correlation_direction,
    )
}

fn g6_direction_and_correlation_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "g6-direction-and-correlation";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let (fixture, owned_server) = setup_raw_probe(context, scenario_dir, server)?;
    let peer = Peer::new()?;

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "wrong_direction_message",
                "a server-direction Session::Binding from the client is a framing \
                 violation: connection-fatal FRAME_ERROR (0x201)"
                    .into(),
            ),
            (
                "second_capabilities",
                "a second capabilities frame after negotiation: connection-fatal \
                 FRAME_ERROR (0x201)"
                    .into(),
            ),
            (
                "unsolicited_selection",
                "a capabilities frame with the response flag set as the first \
                 frame (server-direction before negotiation): connection-fatal \
                 FRAME_ERROR (0x201)"
                    .into(),
            ),
            (
                "repeated_and_decreasing_request_ids",
                "request IDs must increase from 1: repeated or decreasing IDs are \
                 connection-fatal FRAME_ERROR (0x201)"
                    .into(),
            ),
            (
                "bare_control_fin",
                "a control FIN before detach is outside the Section 12.8 MAY: \
                 connection-fatal FRAME_ERROR (0x201) \
                 (normative-clarifications-review item 5)"
                    .into(),
            ),
            (
                "second_bidirectional_stream",
                "stream-count ceilings are transport parameters \
                 (normative-clarifications-review item 4): the second client \
                 bidirectional stream is refused at QUIC transport level (the \
                 open blocks past the budget deadline); the row never waits for \
                 an application LIMIT_EXCEEDED frame"
                    .into(),
            ),
            (
                "unidirectional_ceiling",
                "more concurrent unidirectional streams than the negotiated data \
                 geometry are likewise refused at transport level; after the held \
                 streams are reset the credit is released and a replacement open \
                 succeeds"
                    .into(),
            ),
        ],
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", "rust (raw probe)".into()),
        ("alpn", "pipestream/2".into()),
    ];

    // 1. Server-direction message from the client.
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        conn.send_control(
            FRAME_SESSION,
            &rawclient::session_binding_response(1, "authority-1", "owner-1", 1, 1),
        )?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(
            scenario_id,
            "wrong_direction_message",
            server,
            &close,
            QUIC_FRAME_ERROR,
        )?;
        observed.push(("wrong_direction_message", close_text(&close)));
    }

    // 2. Second CAPABILITIES after negotiation.
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        let duplicate = rawclient::frozen("capabilities-offer")?;
        conn.send_frozen(&duplicate.frame)?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(
            scenario_id,
            "second_capabilities",
            server,
            &close,
            QUIC_FRAME_ERROR,
        )?;
        observed.push(("second_capabilities", close_text(&close)));
    }

    // 3. Unsolicited selection: response-flagged capabilities as the FIRST frame.
    {
        let mut conn = peer.connect(&fixture.certs, "alice", &owned_server.address)?;
        let response = rawclient::frozen("capabilities-response")?;
        fs::write(
            artifacts.join("unsolicited-selection.sent-hex"),
            response
                .frame
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        )?;
        conn.send_frozen(&response.frame)?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(
            scenario_id,
            "unsolicited_selection",
            server,
            &close,
            QUIC_FRAME_ERROR,
        )?;
        observed.push(("unsolicited_selection", close_text(&close)));
    }

    // 4. Repeated request IDs.
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        conn.send_control(FRAME_SESSION, &rawclient::next_sequence(1))?;
        rawclient::parse_next_sequence(&conn.expect_control(FRAME_SESSION)?)?;
        conn.send_control(FRAME_SESSION, &rawclient::next_sequence(1))?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(
            scenario_id,
            "repeated_request_id",
            server,
            &close,
            QUIC_FRAME_ERROR,
        )?;
        observed.push(("repeated_request_id", close_text(&close)));
    }

    // 5. Decreasing request IDs (1, 3, then 2).
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        for request in [1u64, 3] {
            conn.send_control(FRAME_SESSION, &rawclient::next_sequence(request))?;
            rawclient::parse_next_sequence(&conn.expect_control(FRAME_SESSION)?)?;
        }
        conn.send_control(FRAME_SESSION, &rawclient::next_sequence(2))?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(
            scenario_id,
            "decreasing_request_id",
            server,
            &close,
            QUIC_FRAME_ERROR,
        )?;
        observed.push(("decreasing_request_id", close_text(&close)));
    }

    // 6. Bare control FIN before any detach.
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        conn.finish_control()?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(
            scenario_id,
            "bare_control_fin",
            server,
            &close,
            QUIC_FRAME_ERROR,
        )?;
        observed.push(("bare_control_fin", close_text(&close)));
    }

    // 7. Second client bidirectional stream: transport ceiling, not an
    //    application refusal (normative-clarifications-review item 4).
    {
        let conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        match conn.open_bi_ceiling()? {
            Some(_) => {
                bail!(
                    "{scenario_id} second_bi_stream against {} server: a second \
                       bidirectional stream opened inside the budget window; the \
                       transport ceiling was not enforced",
                    server.name()
                )
            }
            None => observed.push((
                "second_bidirectional_stream",
                "second client bidirectional stream blocked past the 4s budget \
                 window while stream 0 stayed open: transport-level enforcement \
                 (peer max_concurrent_bidi_streams); no application refusal frame \
                 is expected or waited for"
                    .into(),
            )),
        }
        conn.vanish();
    }

    // 8. Unidirectional ceiling and credit release.
    {
        let conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        let mut held = Vec::new();
        let mut ceiling = 0u64;
        for _ in 0..8 {
            match conn.open_uni_ceiling()? {
                Some(stream) => {
                    held.push(stream);
                    ceiling += 1;
                }
                None => break,
            }
        }
        ensure!(
            !held.is_empty(),
            "{scenario_id} uni ceiling against {} server: no unidirectional stream \
             could be opened at all",
            server.name()
        );
        let refused_at_ceiling = held.len() < 8;
        for stream in held.iter_mut() {
            conn.reset_stream(stream, 0)?;
        }
        // After the resets the peer must release the stream credit: a
        // replacement open succeeds well inside the budget window.
        let replacement = conn.open_uni_ceiling()?;
        observed.push((
            "unidirectional_ceiling",
            format!(
                "concurrent unidirectional opens accepted: {ceiling}; blocked at \
                 the transport ceiling: {refused_at_ceiling}; after resetting the \
                 held streams a replacement open {} — credit released",
                if replacement.is_some() {
                    "succeeded"
                } else {
                    "STILL BLOCKED"
                }
            ),
        ));
        if let Some(mut stream) = replacement {
            conn.reset_stream(&mut stream, 0)?;
        }
        conn.vanish();
    }

    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, scenario_id, owned_server, events)
}

/// One malformed-input probe: sends header+payload on a fresh unidirectional
/// stream, finishes or aborts it, and expects the correlated refusal on the
/// control stream tagged with that stream's ID.
fn raw_input_probe(
    conn: &mut RawConn,
    header: &[u8],
    payload: &[u8],
    finish: bool,
) -> Result<(u64, rawclient::Refusal, Vec<u8>)> {
    let mut stream = conn.open_uni()?;
    let stream_id = u64::from(stream.id());
    conn.write_stream(&mut stream, header)?;
    conn.write_stream(&mut stream, payload)?;
    if finish {
        conn.finish_stream(&mut stream)?;
    } else {
        conn.reset_stream(&mut stream, 0)?;
    }
    let mut transcript = header.to_vec();
    transcript.extend_from_slice(payload);
    let refusal = rawclient::parse_refusal(&conn.expect_control(FRAME_REFUSAL)?)?;
    ensure!(
        refusal.tag_kind == 1 && refusal.tag_id == stream_id,
        "input refusal tag [{}, {}] does not name the probed stream {stream_id}",
        refusal.tag_kind,
        refusal.tag_id
    );
    Ok((stream_id, refusal, transcript))
}

fn g6_stream_identity_and_fin(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(
        context,
        "g6-stream-identity-and-fin",
        g6_stream_identity_and_fin_direction,
    )
}

fn g6_stream_identity_and_fin_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "g6-stream-identity-and-fin";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let (fixture, owned_server) = setup_raw_probe(context, scenario_dir, server)?;
    let peer = Peer::new()?;

    let payload = oracle::dataset(context.seed, 32);
    let payload_sha = oracle::sha256_hex(&payload);
    let wrong_sha = [0u8; 32];
    let declare_op = oracle::operation_id(context.seed, "declare", 0);
    let admit_op = oracle::operation_id(context.seed, "admit", 1);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("declared_length_over", "declared 48 bytes, 32 sent, FIN: correlated INTEGRITY_ERROR (8) on the [1, streamId] tag; the partial reception is discarded".into()),
            ("declared_length_under", "declared 16 bytes, 32 sent (trailing bytes), FIN: correlated INTEGRITY_ERROR (8)".into()),
            ("wrong_digest", "declared 32, 32 sent, digest of zeros: correlated INTEGRITY_ERROR (8)".into()),
            ("missing_fin", "client resets instead of FIN: correlated INTEGRITY_ERROR (8) (input stream interrupted, normative-clarifications item 3)".into()),
            ("frozen_invalid_input_mode", "wire.tsv invalid-input-mode row replayed verbatim: header decode refusal FRAME_ERROR (1) correlated on [1, streamId]".into()),
            ("frozen_short_input_digest", "wire.tsv short-input-digest row replayed verbatim: FRAME_ERROR (1) correlated".into()),
            ("declaration_survives", "after every refusal a scope page still shows entity 1 declared (state 0); a subsequent valid input on a replacement stream is ADMITTED — the partial receptions were discarded, never the declaration".into()),
            (
                "server_record_roots",
                "named gap: result-manifest/scope-summary refuse-rows are server-direction records; the raw client cannot make either server emit a malformed record"
                    .into(),
            ),
            (
                "result_header_unknown_request",
                "named gap: a result header names a server-chosen result request; the raw client cannot make the server emit one against an unknown request"
                    .into(),
            ),
        ],
    )?;

    let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
    let binding = raw_create_session(&mut conn)?;
    events.append("BINDING_RECEIVED", None, None, None, None, None)?;
    raw_declare(&mut conn, 2, &declare_op, 0, &[1], true)?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(declare_op),
        None,
        None,
        None,
        None,
    )?;

    let header_with = |operation: &[u8; 16], length: u64, sha: &[u8; 32], mode: u64| {
        rawclient::input_header_framed(
            binding.generation,
            operation,
            (0, 0, 1),
            length,
            sha,
            "text/plain",
            "copy/v2",
            mode,
            60_000,
            1,
            length,
        )
    };
    let sha_bytes = |text: &str| {
        let mut sha = [0u8; 32];
        sha.copy_from_slice(&crate::decode_hex(text)?);
        Ok::<[u8; 32], anyhow::Error>(sha)
    };
    let real_sha = sha_bytes(&payload_sha)?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", "rust (raw probe)".into()),
        ("alpn", "pipestream/2".into()),
    ];

    type InputProbe<'a> = (&'a str, Vec<u8>, Vec<u8>, bool, u64);
    let probes: Vec<InputProbe> = vec![
        (
            "declared_length_over",
            header_with(
                &oracle::operation_id(context.seed, "g6input", 2),
                48,
                &real_sha,
                0,
            ),
            payload.clone(),
            true,
            CODE_INTEGRITY_ERROR,
        ),
        (
            "declared_length_under",
            header_with(
                &oracle::operation_id(context.seed, "g6input", 3),
                16,
                &real_sha,
                0,
            ),
            payload.clone(),
            true,
            CODE_INTEGRITY_ERROR,
        ),
        (
            "wrong_digest",
            header_with(
                &oracle::operation_id(context.seed, "g6input", 4),
                32,
                &wrong_sha,
                0,
            ),
            payload.clone(),
            true,
            CODE_INTEGRITY_ERROR,
        ),
        (
            "missing_fin",
            header_with(
                &oracle::operation_id(context.seed, "g6input", 5),
                32,
                &real_sha,
                0,
            ),
            payload.clone(),
            false,
            CODE_INTEGRITY_ERROR,
        ),
        {
            let frozen = rawclient::frozen("invalid-input-mode")?;
            (
                "frozen_invalid_input_mode",
                frozen.frame.clone(),
                b"abc".to_vec(),
                true,
                rawclient::CODE_FRAME_ERROR,
            )
        },
        {
            let frozen = rawclient::frozen("short-input-digest")?;
            (
                "frozen_short_input_digest",
                frozen.frame.clone(),
                b"abc".to_vec(),
                true,
                rawclient::CODE_FRAME_ERROR,
            )
        },
    ];

    for (name, header, body, finish, expected_code) in probes {
        let (stream_id, refusal, transcript) = raw_input_probe(&mut conn, &header, &body, finish)?;
        events.append_as(
            server.name(),
            "server",
            "REFUSAL",
            None,
            None,
            None,
            Some(refusal.code as u32),
            None,
        )?;
        fs::write(
            artifacts.join(format!("{name}.sent-hex")),
            transcript
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        )?;
        ensure!(
            refusal.code == expected_code,
            "{scenario_id} {name} against {} server: expected refusal code \
             {expected_code}, got {} ({}) on stream {stream_id}\nsent bytes: {}",
            server.name(),
            refusal.code,
            refusal.detail,
            hex(&transcript)
        );
        observed.push((
            Box::leak(name.to_string().into_boxed_str()),
            format!(
                "refusal code={} ({}) tagged [1, {stream_id}] — expected code \
                 {expected_code}; sent bytes archived at artifacts/{name}.sent-hex",
                refusal.code,
                named_code(refusal.code)
            ),
        ));
    }

    // Declaration survives: the page still shows entity 1, still declared.
    let (declared, entries) = raw_page(&mut conn, 3, 0)?;
    ensure!(
        declared == 1 && entries == vec![(1, 0)],
        "{scenario_id} against {} server: declaration did not survive the refused \
         inputs (declared={declared}, entries={entries:?})",
        server.name()
    );
    observed.push((
        "declaration_survives",
        "scope page after all refusals: declared=1, entries=[(entity 1, state 0 \
         DECLARED)] — the declaration was never discarded"
            .into(),
    ));

    // A valid input on a replacement stream is admitted: the refused partial
    // receptions committed nothing.
    let valid_header = header_with(&admit_op, 32, &real_sha, 0);
    let mut stream = conn.open_uni()?;
    let stream_id = u64::from(stream.id());
    conn.write_stream(&mut stream, &valid_header)?;
    conn.write_stream(&mut stream, &payload)?;
    conn.finish_stream(&mut stream)?;
    let admitted = rawclient::parse_admitted_stream(&conn.expect_control(FRAME_WORK)?)?;
    ensure!(
        admitted == stream_id,
        "admission receipt names stream {admitted}, expected {stream_id}"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(admit_op),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    observed.push((
        "replacement_input_admitted",
        format!(
            "valid input on replacement stream {stream_id} ADMITTED — refused \
             partial receptions never became receipts"
        ),
    ));

    // Named gaps for the server-direction refuse roots.
    observed.push((
        "server_record_roots",
        "named gap: result-noncontiguous-index, result-locator-userinfo and \
         summary-count-mismatch target server-record roots; a raw client cannot \
         make either server emit malformed records"
            .into(),
    ));
    observed.push((
        "result_header_unknown_request",
        "named gap: result headers are server-chosen; a raw client cannot make \
         the server emit one naming an unknown request"
            .into(),
    ));

    raw_detach(&mut conn, 4)?;
    conn.finish_control()?;
    // Section 12.8: after the control FIN the server MAY close gracefully
    // (cleanly, code 0) or MAY leave connection close to the client.
    let close = match conn.try_wait_closed(Duration::from_secs(3))? {
        Some(close) => {
            raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
            ensure!(
                close_code(&close) == Some(0),
                "{scenario_id}: expected clean post-detach close (code 0), got {}",
                close_text(&close)
            );
            format!("server-initiated {}", close_text(&close))
        }
        None => {
            conn.close_application(b"probe complete")?;
            "server left connection close to the client (MAY, Section 12.8); \
             probe closed the connection with application code 0"
                .to_string()
        }
    };
    observed.push(("post_detach_close", close));
    let _ = wrong_sha;

    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, scenario_id, owned_server, events)
}

fn named_code(code: u64) -> &'static str {
    match code {
        rawclient::CODE_FRAME_ERROR => "FRAME_ERROR",
        rawclient::CODE_EXTENSION_UNSUPPORTED => "EXTENSION_UNSUPPORTED",
        CODE_INTEGRITY_ERROR => "INTEGRITY_ERROR",
        rawclient::CODE_NOT_READY => "NOT_READY",
        12 => "CANCELLED",
        14 => "CONTROL_RESET",
        _ => "UNNAMED",
    }
}

fn g6_stopped_control_and_transfers(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(
        context,
        "g6-stopped-control-and-transfers",
        g6_stopped_control_and_transfers_direction,
    )
}

fn g6_stopped_control_and_transfers_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "g6-stopped-control-and-transfers";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let (fixture, owned_server) = setup_raw_probe(context, scenario_dir, server)?;
    let peer = Peer::new()?;

    let payload = oracle::dataset(context.seed, 32);
    let payload_sha = oracle::sha256_hex(&payload);
    let declare_op = oracle::operation_id(context.seed, "declare", 0);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "reset_stream_on_control",
                "RESET_STREAM on control stream 0: CONTROL_RESET (14) terminates \
                 the connection — QUIC application close 0x20e"
                    .into(),
            ),
            (
                "stop_sending_on_control_responses",
                "STOP_SENDING on the control response direction: CONTROL_RESET \
                 class, connection terminates (0x20e)"
                    .into(),
            ),
            (
                "abort_mid_input",
                "a client abort (RESET_STREAM — the client-side stop primitive for \
                 a send-only stream) mid-input is never an admission receipt: \
                 correlated INTEGRITY_ERROR (8), no admission committed, the \
                 declaration survives (scope page lookup)"
                    .into(),
            ),
            (
                "connection_loss_mid_session",
                "abrupt connection loss changes no obligations: a replacement \
                 connection attaches the same session, next-sequence still \
                 advances by exactly the one creation, the declaration is intact"
                    .into(),
            ),
            (
                "stream_credit",
                "concurrent unidirectional opens are bounded by the peer's \
                 transport MAX_STREAMS; a refused input releases its credit (the \
                 pipestream.4 regression surface): a replacement open succeeds \
                 while the other held streams stay held"
                    .into(),
            ),
        ],
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", "rust (raw probe)".into()),
        ("alpn", "pipestream/2".into()),
        (
            "transport_pin",
            match server {
                Subject::Rust => "rust server: quinn 0.11.11 (workspace Cargo.lock pin); \
                     application data geometry from the negotiated capabilities"
                    .into(),
                Subject::Java => "java server: QUIC stack ships inside the jar (netty-based); \
                     the exact transport pin is not black-box introspectable — \
                     named gap, behavior recorded instead"
                    .into(),
            },
        ),
    ];

    // 1. RESET_STREAM on control stream 0.
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        conn.reset_control(0)?;
        let close = conn.wait_closed(Duration::from_secs(10))?;
        raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
        expect_named_close(
            scenario_id,
            "reset_stream_on_control",
            server,
            &close,
            QUIC_CONTROL_RESET,
        )?;
        observed.push(("reset_stream_on_control", close_text(&close)));
    }

    // 2. STOP_SENDING on the control response direction.
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        conn.stop_control_responses(0)?;
        match conn.try_wait_closed(Duration::from_secs(10))? {
            Some(close) => {
                raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
                expect_named_close(
                    scenario_id,
                    "stop_sending_on_control_responses",
                    server,
                    &close,
                    QUIC_CONTROL_RESET,
                )?;
                observed.push(("stop_sending_on_control_responses", close_text(&close)));
            }
            None => {
                // The server left the connection open. Classify what it did
                // with the stopped control direction for the defect record.
                let fate = match conn.read_control_bounded(Duration::from_secs(3))? {
                    Some(Frame::Fin) => "the server finished its control send direction but left \
                         the connection open"
                        .to_string(),
                    Some(Frame::Control(found, _)) => format!(
                        "the server kept sending control frames (type {found}) \
                         after its response direction was stopped"
                    ),
                    None => "the control direction stayed silent".to_string(),
                };
                conn.close_application(b"probe complete")?;
                observed.push((
                    "stop_sending_on_control_responses",
                    format!(
                        "DEVIATION (potential defect): the connection stayed open \
                         for the full window after STOP_SENDING on the control \
                         response direction; an unusable Control Stream terminates \
                         the connection (CONTROL_RESET class) — observed: {fate}"
                    ),
                ));
            }
        }
    }

    // 3. Abort mid-input: never an admission receipt; declaration survives.
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        let binding = raw_create_session(&mut conn)?;
        raw_declare(&mut conn, 2, &declare_op, 0, &[1], true)?;
        let mut sha = [0u8; 32];
        sha.copy_from_slice(&crate::decode_hex(&payload_sha)?);
        let mut partial = oracle::dataset(context.seed ^ 0x5a, 8);
        let admit_op = oracle::operation_id(context.seed, "g6input", 6);
        let header = rawclient::input_header_framed(
            binding.generation,
            &admit_op,
            (0, 0, 1),
            32,
            &sha,
            "text/plain",
            "copy/v2",
            0,
            60_000,
            1,
            32,
        );
        partial.extend_from_slice(&payload[..24]);
        let (stream_id, refusal, _) = raw_input_probe(&mut conn, &header, &partial, false)?;
        events.append_as(
            server.name(),
            "server",
            "REFUSAL",
            None,
            None,
            None,
            Some(refusal.code as u32),
            None,
        )?;
        ensure!(
            refusal.code == CODE_INTEGRITY_ERROR,
            "{scenario_id} abort_mid_input against {} server: expected \
             INTEGRITY_ERROR (8), got {} ({}) on stream {stream_id}",
            server.name(),
            refusal.code,
            refusal.detail
        );
        // Lookup: no admission was committed; the work is still declared.
        let (declared, entries) = raw_page(&mut conn, 3, 0)?;
        ensure!(
            declared == 1 && entries == vec![(1, 0)],
            "{scenario_id} abort_mid_input against {} server: an admission was \
             committed or the declaration lost (declared={declared}, \
             entries={entries:?})",
            server.name()
        );
        observed.push((
            "abort_mid_input",
            format!(
                "refusal code={} ({}) tagged [1, {stream_id}]; scope page lookup: \
                 declared=1, entries=[(entity 1, state 0 DECLARED)] — no \
                 admission receipt, no obligation change",
                refusal.code,
                named_code(refusal.code)
            ),
        ));
        // Detach cleanly so the server can drain.
        raw_detach(&mut conn, 4)?;
        conn.finish_control()?;
        let _ = conn.wait_closed(Duration::from_secs(10));
    }

    // 4. Connection loss mid-session: attach on a replacement connection.
    {
        let binding = {
            let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
            let binding = raw_create_session(&mut conn)?;
            raw_declare(&mut conn, 2, &declare_op, 0, &[1], true)?;
            let mut sha = [0u8; 32];
            sha.copy_from_slice(&crate::decode_hex(&payload_sha)?);
            let admit_op = oracle::operation_id(context.seed, "g6input", 7);
            let header = rawclient::input_header_framed(
                binding.generation,
                &admit_op,
                (0, 0, 1),
                32,
                &sha,
                "text/plain",
                "copy/v2",
                0,
                60_000,
                1,
                32,
            );
            let mut stream = conn.open_uni()?;
            conn.write_stream(&mut stream, &header)?;
            conn.write_stream(&mut stream, &oracle::dataset(context.seed, 8))?;
            // No FIN, no close frame: the peer simply vanishes mid-reception.
            conn.vanish();
            binding
        };
        // The server needs a moment to reclaim the vanished connection's slot;
        // the attach itself is a fresh connection and must not be affected.
        thread::sleep(Duration::from_millis(300));
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        conn.send_control(
            FRAME_SESSION,
            &rawclient::session_attach(1, &binding.authority, &binding.owner, binding.generation),
        )?;
        let attached = rawclient::parse_binding(&conn.expect_control(FRAME_SESSION)?)?;
        ensure!(
            attached.generation == binding.generation,
            "attach receipt generation {} differs from {}",
            attached.generation,
            binding.generation
        );
        conn.send_control(FRAME_SESSION, &rawclient::next_sequence(2))?;
        let sequence = rawclient::parse_next_sequence(&conn.expect_control(FRAME_SESSION)?)?;
        ensure!(
            sequence == 2,
            "{scenario_id} connection_loss against {} server: next_creation_sequence \
             {sequence}, expected exactly 2 (one creation consumed, none invented \
             by transport events)",
            server.name()
        );
        let (declared, entries) = raw_page(&mut conn, 3, 0)?;
        ensure!(
            declared == 1 && entries == vec![(1, 0)],
            "{scenario_id} connection_loss against {} server: declaration not \
             intact (declared={declared}, entries={entries:?})",
            server.name()
        );
        raw_detach(&mut conn, 4)?;
        conn.finish_control()?;
        let close = match conn.try_wait_closed(Duration::from_secs(3))? {
            Some(close) => {
                raw_event_server_close(&mut events, server, "CONNECTION_CLOSED", &close)?;
                format!("server-initiated {}", close_text(&close))
            }
            None => {
                conn.close_application(b"probe complete")?;
                "server left connection close to the client (MAY, Section 12.8); \
                 probe closed the connection with application code 0"
                    .to_string()
            }
        };
        observed.push((
            "connection_loss_mid_session",
            format!(
                "replacement connection attached generation {} (receipt validated); \
                 next_creation_sequence={sequence} (exactly one creation); scope \
                 page declared=1 entries=[(1, 0)] — no obligation changed by the \
                 loss; clean close after detach: {}",
                binding.generation, close
            ),
        ));
    }

    // 5. Stream credit: held inputs block at the ceiling; a refused input
    //    releases its credit while the other held streams stay held (the
    //    pipestream.4 regression surface).
    {
        let mut conn = raw_negotiate(&peer, &fixture, &owned_server, &mut events, &artifacts)?;
        let binding = raw_create_session(&mut conn)?;
        raw_declare(&mut conn, 2, &declare_op, 0, &[1], true)?;
        let mut held: Vec<quinn::SendStream> = Vec::new();
        for _ in 0..8 {
            match conn.open_uni_ceiling()? {
                Some(stream) => held.push(stream),
                None => break,
            }
        }
        ensure!(
            !held.is_empty(),
            "{scenario_id} stream_credit against {} server: no uni stream opened",
            server.name()
        );
        let held_count = held.len();
        // The refused input runs on a stream that was opened inside the
        // credit; its refusal must release that stream's slot while the
        // remaining held streams stay held.
        let mut refused_stream = held.remove(0);
        let refused_id = u64::from(refused_stream.id());
        let bad_header = rawclient::input_header_framed(
            binding.generation,
            &oracle::operation_id(context.seed, "g6input", 8),
            (0, 0, 1),
            32,
            &[0u8; 32],
            "text/plain",
            "copy/v2",
            0,
            60_000,
            1,
            32,
        );
        conn.write_stream(&mut refused_stream, &bad_header)?;
        conn.write_stream(&mut refused_stream, &payload)?;
        conn.finish_stream(&mut refused_stream)?;
        let refusal = rawclient::parse_refusal(&conn.expect_control(FRAME_REFUSAL)?)
            .context("stream_credit: refusal read")?;
        events.append_as(
            server.name(),
            "server",
            "REFUSAL",
            None,
            None,
            None,
            Some(refusal.code as u32),
            None,
        )?;
        ensure!(
            refusal.code == CODE_INTEGRITY_ERROR
                && refusal.tag_kind == 1
                && refusal.tag_id == refused_id,
            "{scenario_id} stream_credit against {} server: refused input \
             answered code {} ({}) tagged [{}, {}] on stream {refused_id}, \
             expected INTEGRITY_ERROR tagged [1, {refused_id}]",
            server.name(),
            refusal.code,
            refusal.detail,
            refusal.tag_kind,
            refusal.tag_id,
        );
        // The refused input's slot must be free again even while
        // `held_count - 1` streams stay held.
        let replacement = conn.open_uni_ceiling()?;
        ensure!(
            replacement.is_some(),
            "{scenario_id} stream_credit against {} server: the refused input did \
             not release its stream credit (replacement open still blocked with \
             {} streams held)",
            server.name(),
            held.len(),
        );
        observed.push((
            "stream_credit",
            format!(
                "held concurrent unidirectional opens: {held_count} (further opens \
                 blocked at the peer's transport ceiling: {}); refused input on \
                 stream {refused_id} answered INTEGRITY_ERROR and released its \
                 credit — a replacement open succeeded while {} streams stay \
                 held; MAX_STREAMS credit is released by refusals",
                held_count < 8,
                held.len(),
            ),
        ));
        for stream in held.iter_mut() {
            conn.reset_stream(stream, 0)?;
        }
        if let Some(mut stream) = replacement {
            conn.reset_stream(&mut stream, 0)?;
        }
        conn.vanish();
    }

    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, scenario_id, owned_server, events)
}

// ---------------------------------------------------------------------------
// Group R: resource boundaries — batch A (milestone 16)
// ---------------------------------------------------------------------------
//
// Measurement-scope rules (scenario-matrix-g6-resource.md) are binding here:
// RSS/HWM, threads, FDs, actual disk I/O, Java heap, file lengths, and
// allocated blocks are SEPARATE scopes recorded per sample; the collection
// method is recorded on every line; an unavailable MANDATORY metric fails the
// row (never recorded as zero); the optional Java-heap scope degrades to a
// named gap (RSS still collected); dead collectors and truncated records are
// detected by the validating readers in `resources`.
use crate::durable::resources;

/// Documented per-subject connection ceilings (reviewed subject source, run
/// with server defaults): rust `v2 serve` =
/// quinn/src/v2_authority/server.rs Options::default (connections 16,
/// connections_per_principal 4); java V2Main serve = DurableOptions.defaults
/// → CoreOptions (connections 32, connectionsPerOwner 8).
fn documented_connection_ceilings(server: Subject) -> (u64, u64) {
    match server {
        Subject::Rust => (16, 4),
        Subject::Java => (32, 8),
    }
}

fn r_capability_manifest(context: &ScenarioContext) -> Result<()> {
    let scenario_id = "r-capability-manifest";
    let scenario_dir = context.scenario_dir(scenario_id);
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, &scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    let facts = resources::host_facts(&scenario_dir)?;
    let permissions = resources::proc_permissions(std::process::id());
    let jstat = resources::tool_on_path("jstat");
    let jstat_text = jstat
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "absent (java heap scope = named gap where needed)".to_owned());
    let jcmd = resources::tool_on_path("jcmd");
    let calibration = resources::calibrate(std::process::id(), 20)?;

    let permission_text = format!(
        "io={} status={} fd={} net_dev={}",
        permissions.io, permissions.status, permissions.fd, permissions.net_dev
    );
    let manifest: Vec<(&str, String)> = vec![
        ("schema", "pipestream-resource-manifest-v1".into()),
        ("os_type", facts.os_type.clone()),
        ("kernel_release", facts.kernel_release.clone()),
        ("cpu_count", facts.cpu_count.to_string()),
        ("mem_total_kb", facts.mem_total_kb.to_string()),
        ("fixture_fs_type", facts.fixture_fs_type.clone()),
        ("fixture_mount", facts.fixture_mount.clone()),
        ("fixture_device", facts.fixture_device.clone()),
        ("proc_permissions_self", permission_text.clone()),
        ("tool_jstat", jstat_text.clone()),
        (
            "tool_jcmd",
            jcmd.map(|path| path.display().to_string())
                .unwrap_or_else(|| "absent".into()),
        ),
        ("calibration_samples", calibration.samples.to_string()),
        ("calibration_total_ns", calibration.total_ns.to_string()),
        (
            "calibration_per_sample_ns",
            calibration.per_sample_ns.to_string(),
        ),
        (
            "calibration_scope",
            "process scopes only (status/io/fd of one pid); the optional \
             java-heap probe is a separate jstat process on its own cadence \
             and is not included in this figure"
                .into(),
        ),
        (
            "scope_rss_hwm",
            "/proc/<pid>/status VmRSS+VmHWM; whole-process; per sample".into(),
        ),
        (
            "scope_threads",
            "/proc/<pid>/status Threads; per sample".into(),
        ),
        ("scope_fds", "/proc/<pid>/fd entry count; per sample".into()),
        (
            "scope_disk_io",
            "/proc/<pid>/io read_bytes/write_bytes/cancelled_write_bytes; per \
             sample. All three are MANDATORY and read from the same file: \
             write_bytes alone cannot be read as device traffic, because \
             pages a process accounted as written and then truncated away \
             before writeback are counted in write_bytes and again in \
             cancelled_write_bytes"
                .into(),
        ),
        (
            "resources_schema",
            format!(
                "{} ({} columns per record)",
                resources::PROCESS_HEADER,
                resources::PROCESS_FIELDS
            ),
        ),
        (
            "java_memory_freeze",
            format!(
                "{} on every Java subject process, frozen before any \
                 measurement row and identical in every direction and row. \
                 -Xmx bounds the heap and, through the JVM's default \
                 direct-memory ceiling, the direct scope with it; -Xms is \
                 held far below the ceiling so the RSS plateau is measured \
                 rather than pre-committed by the launch flags",
                crate::durable::process::java_memory_flags_text()
            ),
        ),
        (
            "scope_java_heap",
            format!(
                "jstat -gc <pid> 1 1 (S0U+S1U+EU+OU), JVM pids only, sampled \
                 every {}ms (each probe is itself a JVM launch, so it runs at \
                 a slower cadence than the /proc scopes); ticks that collected \
                 it carry a heap:jstat method note and ticks that did not \
                 leave the column absent, never zero; named gap when jstat \
                 reports nothing — the RSS scope is never substituted for it",
                resources::HEAP_SAMPLE_INTERVAL.as_millis()
            ),
        ),
        (
            "scope_rust_heap",
            "named gap: no black-box Rust heap collector exists for the \
             subject binary (no allocator instrumentation is exposed); RSS/HWM \
             is a separate scope and is never reported as Rust heap"
                .into(),
        ),
        (
            "scope_file_lengths",
            "stat(2) st_size per fixture-root file at named checkpoints".into(),
        ),
        (
            "scope_allocated_blocks",
            "stat(2) st_blocks*512 per fixture-root file at named checkpoints".into(),
        ),
        (
            "mandatory_scopes",
            "rss,hwm,threads,fd,disk_io (unavailable => the dependent row fails, \
             never zero); java_heap optional (named gap)"
                .into(),
        ),
    ];
    let mut manifest_text = String::new();
    for (key, value) in &manifest {
        manifest_text.push_str(&format!("{key}\t{value}\n"));
    }
    let manifest_path = artifacts.join("manifest.tsv");
    fs::write(&manifest_path, &manifest_text)?;

    write_kv(
        &scenario_dir,
        "expected.tsv",
        &[
            (
                "contract",
                "the capability manifest records host facts, selected collectors, \
                 /proc permissions, tool availability and collector-overhead \
                 calibration BEFORE any measurement row"
                    .into(),
            ),
            (
                "mandatory_metric_rule",
                "an unavailable MANDATORY metric fails its row; it is never \
                 recorded as zero"
                    .into(),
            ),
            (
                "dead_collector_rule",
                "a sampling error mid-run is recorded and fails the row; \
                 truncated records are rejected by the validating reader"
                    .into(),
            ),
        ],
    )?;
    write_kv(
        &scenario_dir,
        "observed.tsv",
        &[
            ("os_type", facts.os_type),
            ("kernel_release", facts.kernel_release),
            ("cpu_count", facts.cpu_count.to_string()),
            ("mem_total_kb", facts.mem_total_kb.to_string()),
            ("fixture_fs_type", facts.fixture_fs_type),
            ("fixture_mount", facts.fixture_mount),
            ("proc_permissions_self", permission_text),
            ("jstat", jstat_text),
            (
                "calibration_per_sample_ns",
                calibration.per_sample_ns.to_string(),
            ),
            (
                "resources_schema",
                resources::PROCESS_HEADER.trim_start_matches("# ").into(),
            ),
            (
                "java_memory_freeze",
                crate::durable::process::java_memory_flags_text(),
            ),
        ],
    )?;

    events.append(
        "COLLECTOR_MANIFEST",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/manifest.tsv".into(),
            len: manifest_text.len() as u64,
            sha256: oracle::sha256_hex(manifest_text.as_bytes()),
        }),
    )?;
    events.append("CALIBRATION_RECORDED", None, None, None, None, None)?;
    seal(context, &scenario_dir, scenario_id, events)
}

/// How one counted connection attempt ended.
enum OpenOutcome {
    Held(RawConn),
    Refused(String),
}

/// Open one counted connection and classify the refusal class when the server
/// turns it away: pre-authentication transport refusal (connect error) or a
/// post-authentication refusal (the connection dies right after the
/// handshake).
fn open_counted(
    peer: &Peer,
    fixture: &AuthorityFixture,
    server: &OwnedServer,
    principal: &str,
) -> Result<OpenOutcome> {
    match peer.connect(&fixture.certs, principal, &server.address) {
        Ok(conn) => match conn.try_wait_closed(Duration::from_millis(800))? {
            Some(close) => Ok(OpenOutcome::Refused(format!(
                "post-auth refusal (connection closed after handshake): {}",
                close_text(&close)
            ))),
            None => Ok(OpenOutcome::Held(conn)),
        },
        Err(error) => Ok(OpenOutcome::Refused(format!(
            "pre-auth transport refusal (connect failed): {error:#}"
        ))),
    }
}

fn r_connection_ceiling(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(
        context,
        "r-connection-ceiling",
        r_connection_ceiling_direction,
    )
}

fn r_connection_ceiling_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "r-connection-ceiling";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let certs = mtls::generate(
        &scenario_dir.join("certs"),
        &[
            ("alice", "alice"),
            ("bob", "bob"),
            ("carol", "carol"),
            ("dave", "dave"),
            ("erin", "erin"),
            ("frank", "frank"),
            ("grace", "grace"),
        ],
    )?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let owned = fixture.start_server()?;
    let sequence = fixture.next_sequence(&owned, "alice")?;
    ensure!(
        sequence == 1,
        "fresh authority must report NEXT_SEQUENCE 1, got {sequence}"
    );
    let (global_bound, per_principal_bound) = documented_connection_ceilings(server);
    // One principal cannot reach the global bound on its own: the per-principal
    // ceiling (4 rust / 8 java) refuses it first. Phase B therefore opens from
    // several extra principals until the GLOBAL refusal lands: global 16 with
    // per-principal 4 needs 4 principals (rust); global 32 with 8 needs 4
    // principals (java). Six extras cover both with margin.
    let extra_principals = ["bob", "carol", "dave", "erin", "frank", "grace"];
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "documented_global_bound",
                format!("{global_bound} ({})", server.name()),
            ),
            (
                "documented_per_principal_bound",
                format!("{per_principal_bound} ({})", server.name()),
            ),
            (
                "boundedness",
                "a refusal is observed at a finite connection count from one \
                 principal and from a second principal while the first holds; \
                 the observed admitted counts never exceed the documented bounds"
                    .into(),
            ),
            (
                "refusal_class",
                "pre-auth CONNECTION_REFUSED (transport refusal) or post-auth \
                 refusal — the observed class is recorded per subject"
                    .into(),
            ),
            (
                "recovery",
                "after every held connection closes, a fresh connection from the \
                 first principal succeeds"
                    .into(),
            ),
            (
                "incomplete_handshake_accounting",
                "named gap: the quinn client completes handshakes atomically; \
                 half-open handshake counts are not observable black-box"
                    .into(),
            ),
        ],
    )?;

    let peer = Peer::new()?;
    let mut log = String::from("phase\tattempt\tprincipal\toutcome\tdetail\n");
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        (
            "client_subject",
            "rust (raw probe, one held connection per attempt)".into(),
        ),
        ("documented_global_bound", global_bound.to_string()),
        (
            "documented_per_principal_bound",
            per_principal_bound.to_string(),
        ),
    ];

    // Phase A: one principal up to its per-principal refusal.
    let mut held_a: Vec<RawConn> = Vec::new();
    let mut refusal_a: Option<(u64, String)> = None;
    for attempt in 1..=per_principal_bound + 4 {
        match open_counted(&peer, &fixture, &owned, "alice")? {
            OpenOutcome::Held(conn) => {
                log.push_str(&format!("per-principal\t{attempt}\talice\theld\t-\n"));
                held_a.push(conn);
            }
            OpenOutcome::Refused(class) => {
                log.push_str(&format!(
                    "per-principal\t{attempt}\talice\trefused\t{class}\n"
                ));
                refusal_a = Some((attempt, class));
                break;
            }
        }
    }
    let (refusal_a_attempt, refusal_a_class) = refusal_a.with_context(|| {
        format!(
            "{scenario_id} {}: no per-principal ceiling observed from alice \
                 after {} attempts; BOUNDEDNESS fails",
            server.name(),
            per_principal_bound + 4
        )
    })?;
    ensure!(
        held_a.len() as u64 == per_principal_bound && refusal_a_attempt == per_principal_bound + 1,
        "{scenario_id} {}: per-principal observations (held {}, refusal at \
         attempt {refusal_a_attempt}) contradict the documented bound \
         {per_principal_bound}",
        server.name(),
        held_a.len()
    );
    observed.push(("per_principal_admitted", held_a.len().to_string()));
    observed.push((
        "per_principal_refusal_attempt",
        refusal_a_attempt.to_string(),
    ));
    observed.push(("per_principal_refusal_class", refusal_a_class.clone()));
    events.append("CONNECTION_REFUSAL_OBSERVED", None, None, None, None, None)?;
    fs::write(artifacts.join("connections.tsv"), &log)?;

    // Phase B: walk the extra principals while alice holds, until the GLOBAL
    // refusal lands. One principal cannot reach the global bound on its own —
    // the per-principal ceiling refuses it first — so each extra principal is
    // held up to its own refusal; a refusal is classified GLOBAL only once
    // total_held reaches the documented global bound.
    let mut held_b: Vec<RawConn> = Vec::new();
    let mut global_refusal: Option<(String, u64, String)> = None;
    let mut per_principal_refusals: Vec<(String, u64)> = Vec::new();
    'principals: for principal in extra_principals {
        for attempt in 1..=per_principal_bound + 1 {
            match open_counted(&peer, &fixture, &owned, principal)? {
                OpenOutcome::Held(conn) => {
                    log.push_str(&format!("global\t{attempt}\t{principal}\theld\t-\n"));
                    held_b.push(conn);
                }
                OpenOutcome::Refused(class) => {
                    let total_held = (held_a.len() + held_b.len()) as u64;
                    log.push_str(&format!(
                        "global\t{attempt}\t{principal}\trefused\ttotal_held={total_held} {class}\n"
                    ));
                    if total_held >= global_bound {
                        global_refusal = Some((principal.to_string(), total_held, class));
                        break 'principals;
                    }
                    per_principal_refusals.push((principal.to_string(), attempt));
                    continue 'principals;
                }
            }
        }
        bail!(
            "{scenario_id} {}: principal {principal} held {} connections from \
             alice's refusal without being refused; the documented per-principal \
             bound {per_principal_bound} is contradicted",
            server.name(),
            per_principal_bound + 1
        );
    }
    let (global_refusal_principal, global_admitted_total, global_refusal_class) = global_refusal
        .with_context(|| {
            format!(
                "{scenario_id} {}: no global ceiling observed from {} extra \
                 principals while alice holds {}; BOUNDEDNESS fails",
                server.name(),
                extra_principals.len(),
                held_a.len()
            )
        })?;
    ensure!(
        global_admitted_total == global_bound,
        "{scenario_id} {}: global refusal landed at total_held \
         {global_admitted_total}, contradicting the documented global bound \
         {global_bound}",
        server.name()
    );
    for (principal, attempt) in &per_principal_refusals {
        ensure!(
            *attempt == per_principal_bound + 1,
            "{scenario_id} {}: principal {principal} was refused at attempt \
             {attempt}, contradicting the documented per-principal bound \
             {per_principal_bound}",
            server.name()
        );
    }
    observed.push(("global_admitted_total", global_admitted_total.to_string()));
    observed.push(("global_refusal_principal", global_refusal_principal.clone()));
    observed.push((
        "per_principal_refusals_before_global",
        per_principal_refusals.len().to_string(),
    ));
    observed.push(("global_refusal_class", global_refusal_class.clone()));
    events.append("CONNECTION_REFUSAL_OBSERVED", None, None, None, None, None)?;
    // Evidence is written per phase so a later failure never loses it.
    fs::write(artifacts.join("connections.tsv"), &log)?;

    // Phase C: recovery — close everything; capacity must come back. The
    // close is reaped asynchronously, so poll with backoff before judging
    // non-recovery; every attempt is logged.
    drop(held_b);
    drop(held_a);
    let mut recovery: Option<String> = None;
    for attempt in 1..=6u64 {
        thread::sleep(Duration::from_millis(2_500));
        match open_counted(&peer, &fixture, &owned, "alice")? {
            OpenOutcome::Held(conn) => {
                conn.close_application(b"ceiling row complete")?;
                recovery = Some(format!("held on attempt {attempt}"));
                log.push_str(&format!(
                    "recovery\t{attempt}\talice\theld\tnew connection after full close\n"
                ));
                break;
            }
            OpenOutcome::Refused(class) => {
                log.push_str(&format!("recovery\t{attempt}\talice\trefused\t{class}\n"));
            }
        }
    }
    fs::write(artifacts.join("connections.tsv"), &log)?;
    let recovery = recovery.with_context(|| {
        format!(
            "{scenario_id} {}: capacity did not recover within 15s of closing \
             every connection; see artifacts/connections.tsv",
            server.name()
        )
    })?;
    observed.push(("recovery_after_close", recovery));
    observed.push((
        "incomplete_handshake_accounting",
        "named gap: not observable through the quinn client (handshakes complete \
         atomically)"
            .into(),
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    events.append(
        "CEILING_EVIDENCE",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/connections.tsv".into(),
            len: log.len() as u64,
            sha256: oracle::sha256_hex(log.as_bytes()),
        }),
    )?;
    stop_and_seal(context, scenario_dir, scenario_id, owned, events)
}

/// Healthy-principal deadline. The stall window itself is per subject:
/// max(90s, negotiated stream lifetime + 30s).
const BOB_OP_DEADLINE: Duration = Duration::from_secs(10);
/// Stalled input streams: three concurrent partial uploads (the negotiated
/// stream geometry is four concurrent object streams per direction on the
/// rust offer, sixteen on the java offer — three leaves headroom on both).
const STALLED_STREAMS: usize = 3;
const STALL_PAYLOAD_LEN: usize = 256 * 1024;
const STALL_PARTIAL_LEN: usize = 128 * 1024;
/// QUIC PING cadence on the abusive principal's connection. The row leaves
/// that connection silent for minutes between enforcement probes, which is
/// longer than the transport idle timeout: without keep-alive the FIXTURE's
/// own transport tears the connection down, every stalled stream dies with
/// it, and that is indistinguishable from the subject enforcing a deadline.
/// PINGs carry no application data, so they are not activity on any object
/// stream and must not renew an application receive deadline.
const STALL_KEEP_ALIVE: Duration = Duration::from_secs(5);
/// Settle window between closing the abusive connection and signalling the
/// subject to stop, so the subject's transport can retire a connection that
/// was still live one instant earlier.
///
/// Sized from observation, not taste: the rust authority runs its own 5 s
/// keep-alive with a 60 s transport idle timeout and gives its whole
/// shutdown a 5 s grace, of which the execution-pool wind-down can consume
/// all before the transport wait even starts. At 5 s of settle the drain
/// assertion in `OwnedServer::stop` was observed to fail intermittently
/// under batch load with everything except the transport reported idle. The
/// window is fixture timing, never evidence: no measurement is taken during
/// it, and the collector is already stopped.
const STALL_CLOSE_SETTLE: Duration = Duration::from_secs(30);
/// Bounded wait of one NON-WRITING stream probe. Short, because the probe
/// only asks whether a STOP_SENDING has already arrived and the peer's
/// runtime is driven continuously, so an answer that exists is already
/// applied; a long wait here would blur the first-observation bracket.
const STALL_PROBE_POLL: Duration = Duration::from_millis(250);

fn r_stalled_principal_progress(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(
        context,
        "r-stalled-principal-progress",
        r_stalled_principal_progress_direction,
    )
}

/// Median of a sorted series.
fn median(series: &[u64]) -> u64 {
    let mid = series.len() / 2;
    if series.len().is_multiple_of(2) {
        (series[mid - 1] + series[mid]) / 2
    } else {
        series[mid]
    }
}

/// RSS/FD plateau evidence for the anchor pid: baseline median over
/// [20s, 50s] against the tail p90 over the final 30s of the window.
fn plateau_evidence(
    samples: &[resources::ProcessSample],
    pid: u32,
) -> Result<(u64, u64, u64, u64)> {
    let rss: Vec<(u64, u64)> = samples
        .iter()
        .filter(|sample| sample.pid == pid && sample.rss_kb.is_some())
        .map(|sample| (sample.elapsed_ms, sample.rss_kb.unwrap_or(0)))
        .collect();
    let fds: Vec<(u64, u64)> = samples
        .iter()
        .filter(|sample| sample.pid == pid && sample.fds.is_some())
        .map(|sample| (sample.elapsed_ms, sample.fds.unwrap_or(0)))
        .collect();
    ensure!(
        rss.len() >= 30,
        "not enough RSS samples for plateau evidence ({} )",
        rss.len()
    );
    let end = rss.last().map(|(elapsed, _)| *elapsed).unwrap_or(0);
    let in_window = |series: &[(u64, u64)], from: u64, to: u64| -> Vec<u64> {
        series
            .iter()
            .filter(|(elapsed, _)| *elapsed >= from && *elapsed <= to)
            .map(|(_, value)| *value)
            .collect()
    };
    let mut base_rss = in_window(&rss, 20_000, 50_000);
    let mut tail_rss = in_window(&rss, end.saturating_sub(30_000), end);
    let mut base_fd = in_window(&fds, 20_000, 50_000);
    let mut tail_fd = in_window(&fds, end.saturating_sub(30_000), end);
    ensure!(
        !base_rss.is_empty() && !tail_rss.is_empty() && !base_fd.is_empty() && !tail_fd.is_empty(),
        "plateau windows are empty (window too short?)"
    );
    base_rss.sort_unstable();
    tail_rss.sort_unstable();
    base_fd.sort_unstable();
    tail_fd.sort_unstable();
    let tail_p90_index = |series: &[u64]| -> usize {
        // Nearest-rank p90: ceil(0.9 * n) - 1.
        (series.len() * 9)
            .div_ceil(10)
            .saturating_sub(1)
            .min(series.len() - 1)
    };
    Ok((
        median(&base_rss),
        tail_rss[tail_p90_index(&tail_rss)],
        median(&base_fd),
        tail_fd[tail_p90_index(&tail_fd)],
    ))
}

/// One timed healthy-principal op; records latency and appends the transcript.
fn bob_op(
    fixture: &AuthorityFixture,
    journal: &Path,
    sequence: u64,
    connection: &[String],
    operation: &[&str],
    label: &str,
    transcript: &mut String,
) -> Result<Duration> {
    let started = Instant::now();
    let output = fixture.run_client_op(journal, "bob", sequence, connection, operation)?;
    let latency = started.elapsed();
    let marker = match label {
        "next-sequence" => "NEXT_SEQUENCE",
        "declare" => "RECEIPT",
        "admit" => "RECEIPT",
        "lookup" => "RECEIPT",
        "page" => "SCOPE",
        other => bail!("unknown bob op label {other}"),
    };
    ensure!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains(marker),
        "bob {label} failed ({})\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    transcript.push_str(&format!(
        "=== bob {label} latency_ms={} ===\n{}\n",
        latency.as_millis(),
        String::from_utf8_lossy(&output.stdout)
    ));
    Ok(latency)
}

/// NON-WRITING probe of the stalled upload streams.
///
/// It polls each send stream's stopped state with a short bounded wait and
/// writes nothing. An earlier version wrote ten one-byte payloads per
/// stream per probe, and that was wrong twice over: on a subject whose
/// input receive deadline is measured from the LAST PAYLOAD BYTE (the Java
/// server's `InputTransfer.lastProgress`) those bytes are progress and
/// legitimately renew the very deadline the probe is there to observe, and
/// on a client whose transport is only driven inside `block_on` a write
/// that returns out of local send credit never yields to the endpoint
/// driver, so the peer's STOP_SENDING is applied at the next call that
/// happens to wait rather than when it arrived. Both are fixed: this probe
/// carries no application data at all, and the peer's runtime is driven
/// continuously (`rawclient::Peer::build`).
///
/// Returns the stream ids observed stopped on THIS probe. The connection's
/// own liveness is recorded alongside every probe, because a subject may
/// enforce at the connection level instead of per stream.
fn probe_stalled_streams(
    alice: &RawConn,
    streams: &mut [(u64, quinn::SendStream)],
    label: &str,
    stalls: &mut String,
) -> Result<BTreeSet<u64>> {
    let mut aborted = BTreeSet::new();
    // A transport idle timeout is the FIXTURE's own transport giving up, not
    // the subject enforcing anything. Every stream on the connection reports
    // lost afterwards, so counting those as enforcement would pass the row
    // on evidence the subject never produced.
    let mut attributable = true;
    match alice.try_wait_closed(Duration::from_millis(100))? {
        Some(close) => {
            let text = close_text(&close);
            attributable =
                !matches!(&close, Close::Transport(reason) if reason.contains("timed out"));
            stalls.push_str(&format!(
                "{label}\tconnection\tclosed by peer: {text}{}\n",
                if attributable {
                    ""
                } else {
                    " [NOT attributable to the subject: fixture transport idle \
                     timeout; stream aborts on this probe are not counted]"
                }
            ));
        }
        None => stalls.push_str(&format!("{label}\tconnection\tlive\n")),
    }
    for (stream_id, stream) in streams.iter_mut() {
        let outcome = match alice.poll_stream_stopped(stream, STALL_PROBE_POLL)? {
            rawclient::StreamState::Open => format!("still-open-at-{label}"),
            rawclient::StreamState::Stopped(code) if attributable => {
                aborted.insert(*stream_id);
                format!("aborted (STOP_SENDING {code})")
            }
            rawclient::StreamState::Stopped(code) => format!(
                "STOP_SENDING {code} seen after the fixture's transport idle timeout, \
                 NOT counted as enforcement"
            ),
            rawclient::StreamState::Acknowledged => {
                "acknowledged (the peer read a finished stream to completion, which a \
                 never-FINed stall should never reach)"
                    .to_owned()
            }
            rawclient::StreamState::Lost(reason) if attributable => {
                aborted.insert(*stream_id);
                format!("aborted (stream lost: {reason})")
            }
            rawclient::StreamState::Lost(reason) => format!(
                "stream lost after the fixture's transport idle timeout, NOT counted as \
                 enforcement ({reason})"
            ),
        };
        stalls.push_str(&format!("{label}\tstream-{stream_id}\t{outcome}\n"));
    }
    Ok(aborted)
}

/// Drain whatever the server queued on the abusive principal's control stream
/// and classify it. A Refusal carrying an input-stream tag is the
/// protocol-level record of stall enforcement (LIMIT_EXCEEDED, code 4); every
/// other frame is logged verbatim so the drain is auditable. Bounded: at most
/// 64 frames, and the first read that ends, times out, or fails stops the
/// drain. Called only after the measurement window, so the pending responses
/// stay genuinely unread while the principal stalls.
fn drain_control_refusals(
    alice: &mut RawConn,
    label: &str,
    stalls: &mut String,
) -> Vec<rawclient::Refusal> {
    let mut refusals = Vec::new();
    for _ in 0..64 {
        match alice.read_control_bounded(Duration::from_millis(500)) {
            Ok(Some(Frame::Control(FRAME_REFUSAL, body))) => {
                match rawclient::parse_refusal(&body) {
                    Ok(refusal) => {
                        stalls.push_str(&format!(
                            "{label}\tcontrol\trefusal tag_kind={} tag_id={} code={} detail={:?}\n",
                            refusal.tag_kind, refusal.tag_id, refusal.code, refusal.detail
                        ));
                        refusals.push(refusal);
                    }
                    Err(error) => stalls.push_str(&format!(
                        "{label}\tcontrol\tunparseable refusal ({error:#})\n"
                    )),
                }
            }
            Ok(Some(Frame::Control(kind, body))) => stalls.push_str(&format!(
                "{label}\tcontrol\tframe kind={kind} len={}\n",
                body.len()
            )),
            Ok(Some(Frame::Fin)) => {
                stalls.push_str(&format!("{label}\tcontrol\tFIN\n"));
                break;
            }
            Ok(None) => {
                stalls.push_str(&format!("{label}\tcontrol\tno further frames\n"));
                break;
            }
            Err(error) => {
                stalls.push_str(&format!("{label}\tcontrol\tread failed ({error:#})\n"));
                break;
            }
        }
    }
    refusals
}

fn r_stalled_principal_progress_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "r-stalled-principal-progress";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let certs = mtls::generate(
        &scenario_dir.join("certs"),
        &[("alice", "alice"), ("bob", "bob")],
    )?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let owned = fixture.start_server()?;
    let sequence = fixture.next_sequence(&owned, "alice")?;
    ensure!(
        sequence == 1,
        "fresh authority must report NEXT_SEQUENCE 1, got {sequence}"
    );

    // Measurement gates: the mandatory /proc scopes must be readable for the
    // server process group, and the collector must start clean.
    let anchor = owned.pid()?;
    let permissions = resources::proc_permissions(anchor);
    ensure!(
        permissions.status && permissions.fd && permissions.io,
        "{scenario_id}: mandatory /proc scopes unreadable for the server pid \
         {anchor} (status={} fd={} io={}); an unavailable mandatory metric \
         fails the row, never recorded as zero",
        permissions.status,
        permissions.fd,
        permissions.io
    );
    let collector = resources::ProcessCollector::start(
        anchor,
        resources::SAMPLE_INTERVAL,
        &scenario_dir.join("resources.tsv"),
    )?;
    let store_file = scenario_dir.join("store.tsv");
    resources::sample_store(
        "start",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        (
            "client_subject",
            "alice = rust raw peer (stalls); bob = rust CLI (healthy principal)".into(),
        ),
        (
            "java_memory_freeze",
            match server {
                Subject::Java => format!(
                    "{} (frozen before the run; applies to this server process)",
                    crate::durable::process::java_memory_flags_text()
                ),
                Subject::Rust => format!(
                    "not applicable: no JVM in this subject group (the frozen \
                     Java limits are {})",
                    crate::durable::process::java_memory_flags_text()
                ),
            },
        ),
    ];
    let mut stalls = String::from("probe\tstream\tobservation\n");

    // ---- principal A (alice): establish the stalls ----
    // Keep-alive: see STALL_KEEP_ALIVE. The abusive connection is silent for
    // minutes between probes and must outlive the fixture's own transport.
    let peer = Peer::with_keep_alive(STALL_KEEP_ALIVE)?;
    let mut alice = raw_negotiate_as(&peer, &fixture, &owned, &mut events, &artifacts, "alice")?;
    // Enforcement bounds are the NEGOTIATED selection's, per subject (the
    // rust offer advertises idle 5s/lifetime 30s, the java offer 30s/300s).
    let caps = alice
        .caps()
        .context("capabilities selection was not recorded during negotiation")?;
    let negotiated_idle = Duration::from_millis(caps.idle_ms);
    let negotiated_lifetime = Duration::from_millis(caps.lifetime_ms);
    let stall_window = Duration::from_secs(90).max(negotiated_lifetime + Duration::from_secs(30));
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "healthy_principal_deadline_ms",
                BOB_OP_DEADLINE.as_millis().to_string(),
            ),
            (
                "window_seconds",
                format!(
                    "{} (lifetime {}s + 30s margin, 90s floor)",
                    stall_window.as_secs(),
                    negotiated_lifetime.as_secs()
                ),
            ),
            (
                "abusive_principal",
                format!(
                    "alice (raw peer): {STALLED_STREAMS} input streams with \
                     partial {STALL_PARTIAL_LEN}-byte prefixes of declared \
                     {STALL_PAYLOAD_LEN}-byte payloads and no FIN, one pending \
                     30s watch held unread on a declared-never-admitted work, \
                     one result stream requested and never read"
                ),
            ),
            (
                "control_independence",
                "bob's next-sequence/declare/admit/lookup/page complete within \
                 the deadline throughout the window while alice stalls"
                    .into(),
            ),
            (
                "stall_enforcement",
                format!(
                    "every stalled input stream is enforced at the negotiated \
                     idle/lifetime bounds (idle {}s, lifetime {}s): either the \
                     transport aborts it (STOP_SENDING, observed by a \
                     NON-WRITING probe at idle+2s, +5s, +10s and \
                     lifetime+10s) or a LIMIT_EXCEEDED (code {}) Refusal \
                     naming that input stream tag is queued on the control \
                     stream and read at window end — both channels are \
                     recorded per stream in artifacts/stalls.tsv",
                    negotiated_idle.as_secs(),
                    negotiated_lifetime.as_secs(),
                    rawclient::CODE_LIMIT_EXCEEDED
                ),
            ),
            (
                "stall_enforcement_bracket",
                "per stream, the row records the LAST probe that saw it open \
                 and the FIRST that saw it stopped, and claims no value \
                 inside that bracket. The probes carry no application data, \
                 so none of them renews a receive deadline measured from the \
                 last payload byte; the milestone-17b bracket, taken with \
                 writing probes on a client whose transport was only driven \
                 inside block_on, is withdrawn rather than restated"
                    .into(),
            ),
            (
                "rss_plateau",
                "server-group RSS: tail p90 (final 30s) <= baseline median \
                 (20-50s) + max(baseline/N, fixed slack); rust N=4 slack=64MiB, \
                 java N=2 slack=128MiB (JVM warmup allowance) — never \
                 monotonic growth"
                    .into(),
            ),
            (
                "fd_plateau",
                "server-group FDs: tail p90 <= baseline median + 8".into(),
            ),
            (
                "fixture_transport",
                format!(
                    "the abusive principal's connection sends QUIC PINGs every \
                     {}s with an explicit {}s transport idle timeout, so it \
                     outlives the silent gaps between enforcement probes; \
                     PINGs are transport traffic only and carry no object \
                     stream data. A probe that finds the connection gone with \
                     a transport idle timeout records its stream aborts as NOT \
                     attributable to the subject and does not count them",
                    STALL_KEEP_ALIVE.as_secs(),
                    rawclient::KEEP_ALIVE_MAX_IDLE.as_secs()
                ),
            ),
            (
                "disk_io_scope",
                "server-group write_bytes AND cancelled_write_bytes are both \
                 collected per sample and reported as rates over the window; \
                 neither is asserted against a bound here (this row's gates \
                 are progress and the RSS/FD plateaus) — they are recorded so \
                 an idle write rate can be told apart from cancelled \
                 page-cache writeback"
                    .into(),
            ),
            (
                "heap_scope",
                "java-server direction: heap via jstat when the JDK reports it, \
                 named gap otherwise; rust-server direction: no JVM exists, so \
                 the Java-heap scope is not applicable and the Rust heap scope \
                 is a named gap — RSS is never substituted for either"
                    .into(),
            ),
        ],
    )?;
    let binding = raw_create_session(&mut alice)?;
    let declare_op = oracle::operation_id(context.seed, "stall-declare", 0);
    events.append("REQUEST_SENT", Some(declare_op), None, None, None, None)?;
    // Unsealed: bob must keep declaring new entities in scope 0 throughout
    // the window, which a sealed membership would refuse.
    raw_declare(&mut alice, 2, &declare_op, 0, &[1, 2, 3, 4, 5, 6], false)?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(declare_op),
        None,
        None,
        None,
        None,
    )?;

    // W1: one admission that completes, so the result scope exists.
    let payload = oracle::dataset(context.seed, 64 * 1024);
    let payload_sha = oracle::sha256_hex(&payload);
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&crate::decode_hex(&payload_sha)?);
    let admit_op = oracle::operation_id(context.seed, "stall-admit", 1);
    let header = rawclient::input_header_framed(
        binding.generation,
        &admit_op,
        (0, 0, 1),
        payload.len() as u64,
        &sha,
        "application/octet-stream",
        "copy/v2",
        0,
        60_000,
        1,
        payload.len() as u64,
    );
    let mut w1_stream = alice.open_uni()?;
    let w1_stream_id = u64::from(w1_stream.id());
    alice.write_stream(&mut w1_stream, &header)?;
    alice.write_stream(&mut w1_stream, &payload)?;
    alice.finish_stream(&mut w1_stream)?;
    events.append(
        "REQUEST_SENT",
        Some(admit_op),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let admitted = rawclient::parse_admitted_stream(&alice.expect_control(FRAME_WORK)?)?;
    ensure!(
        admitted == w1_stream_id,
        "W1 admission receipt names stream {admitted}, expected {w1_stream_id}"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(admit_op),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    drop(w1_stream);

    // Watch W1 to terminal success. A watch answers as soon as the work
    // revision differs from after_revision — immediately for after_revision 0
    // — so the first answer can be a pre-terminal state; long-poll with the
    // observed revision until a terminal state (5..=8) lands. Request ids
    // must stay strictly increasing per connection: rounds use 3..=6,
    // leaving 7 and 8 for the pending watch and result read below.
    let terminal = {
        let mut after = 0u64;
        let mut terminal = None;
        for round in 0..4u64 {
            alice.send_control(
                FRAME_WORK,
                &rawclient::work_watch(3 + round, (0, 0, 1), after, 5_000),
            )?;
            let (revision, state) = match alice.read_control_bounded(Duration::from_secs(15))? {
                Some(Frame::Control(FRAME_WORK, body)) => rawclient::parse_view_state(&body)?,
                other => bail!("watch W1: expected Work::View, got {other:?}"),
            };
            if (5..=8).contains(&state) {
                terminal = Some(state);
                break;
            }
            ensure!(
                revision > after,
                "watch W1 made no progress (revision {revision}, after {after})"
            );
            after = revision;
        }
        terminal.context("watch W1 never reached a terminal state")?
    };
    ensure!(
        terminal == 5,
        "W1 must succeed (state 5 = SUCCEEDED), got state {terminal}"
    );

    // Stalled uploads: partial prefixes, never FINed. Receipts are NOT
    // awaited: the rust server certifies admission only after the input FINes,
    // so a partial stream gets no receipt — its Refusal (input receive
    // deadline, LIMIT_EXCEEDED class) lands on the control stream at the idle
    // bound and the stream reset is observed through the write probes below.
    let stall_payload = oracle::dataset(context.seed ^ 0x57a11, STALL_PAYLOAD_LEN);
    let stall_sha = oracle::sha256_hex(&stall_payload);
    let mut full_sha = [0u8; 32];
    full_sha.copy_from_slice(&crate::decode_hex(&stall_sha)?);
    let mut stalled: Vec<(u64, quinn::SendStream)> = Vec::new();
    for index in 0..STALLED_STREAMS {
        let entity = 2 + index as u64;
        let work = (0, 0, entity);
        let operation = oracle::operation_id(context.seed, "stall-input", entity as u32);
        let header = rawclient::input_header_framed(
            binding.generation,
            &operation,
            work,
            STALL_PAYLOAD_LEN as u64,
            &full_sha,
            "application/octet-stream",
            "copy/v2",
            0,
            60_000,
            1,
            STALL_PAYLOAD_LEN as u64,
        );
        let mut stream = alice.open_uni()?;
        let stream_id = u64::from(stream.id());
        alice.write_stream(&mut stream, &header)?;
        alice.write_stream(&mut stream, &stall_payload[..STALL_PARTIAL_LEN])?;
        stalls.push_str(&format!("establish\tstream-{stream_id}\tpartial {STALL_PARTIAL_LEN}/{STALL_PAYLOAD_LEN} bytes, no FIN\n"));
        stalled.push((stream_id, stream));
    }

    // Pending watch on a declared-never-admitted work; response never read.
    alice.send_control(FRAME_WORK, &rawclient::work_watch(7, (0, 0, 6), 0, 30_000))?;
    // Result stream for W1 requested and then never read (control or object).
    alice.send_control(
        FRAME_RESULT,
        &rawclient::result_read(8, (0, 0, 1), 1, 0, &sha),
    )?;
    events.append("STALLS_ESTABLISHED", None, None, None, None, None)?;
    resources::sample_store(
        "stalls-established",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    observed.push((
        "stalls",
        format!(
            "{STALLED_STREAMS} partial inputs (no FIN), pending watch on 0:0:6, \
             result stream for 0:0:1 unread"
        ),
    ));

    // ---- principal B (bob): healthy client, timed ops throughout ----
    let bob_sequence = fixture.next_sequence(&owned, "bob")?;
    let bob_journal = scenario_dir.join("client").join("bob.sqlite");
    fs::create_dir_all(bob_journal.parent().expect("journal has a parent"))?;
    let mut init = fixture.client_base()?;
    init.push("init-client".into());
    init.extend(fixture.journal_args(&bob_journal, "bob", bob_sequence));
    let init_out = crate::run_output_owned(&fixture.root, &init, OP_WAIT)?;
    require(
        &init_out,
        Subject::Rust.client_initialized_marker(),
        "bob init-client",
    )?;
    let bob_connection = fixture.connection_args(&owned, "bob")?;
    let bob_input = artifacts.join("bob-input.bin");
    fs::write(&bob_input, oracle::dataset(context.seed ^ 0xb0b, 4096))?;

    let window_start = Instant::now();
    let mut transcript = String::new();
    let mut ops_log = String::from("round\top\tlatency_ms\n");
    let mut worst = Duration::ZERO;
    let mut round = 0u64;
    let mut sampled_mid = false;
    let mut aborted_ids: BTreeSet<u64> = BTreeSet::new();
    // Enforcement probes at the NEGOTIATED bounds plus the intermediate
    // marks. The row brackets the subject's enforcement between the last
    // probe that saw a stream open and the first that saw it stopped, so
    // the marks are close together just after the idle bound where the
    // enforcement is expected, and one far out at the lifetime bound to
    // catch a subject that enforces there instead.
    let mut probe_marks: Vec<(String, Duration, bool)> = vec![
        (
            // Below the bound on purpose: without a probe that sees a stream
            // OPEN there is no lower end to the bracket, only "stopped by
            // the time anyone looked".
            "idle-bound-2s".to_owned(),
            negotiated_idle.saturating_sub(Duration::from_secs(2)),
            false,
        ),
        ("idle-bound+0s".to_owned(), negotiated_idle, false),
        (
            "idle-bound+2s".to_owned(),
            negotiated_idle + Duration::from_secs(2),
            false,
        ),
        (
            "idle-bound+5s".to_owned(),
            negotiated_idle + Duration::from_secs(5),
            false,
        ),
        (
            "idle-bound+10s".to_owned(),
            negotiated_idle + Duration::from_secs(10),
            false,
        ),
        (
            "lifetime-bound+10s".to_owned(),
            negotiated_lifetime + Duration::from_secs(10),
            false,
        ),
    ];
    probe_marks.sort_by_key(|(_, at, _)| *at);
    // Per stream: the last probe that saw it open, and the first that saw
    // it stopped. That pair IS the bracket the row reports.
    let mut last_open_at: BTreeMap<u64, (String, u64)> = BTreeMap::new();
    let mut first_stopped_at: BTreeMap<u64, (String, u64)> = BTreeMap::new();
    let mut probe_log: Vec<(String, u64, usize)> = Vec::new();
    while window_start.elapsed() < stall_window {
        round += 1;
        // next-sequence is a journal-free top-level command (server/src/v2.rs
        // Command::NextSequence), not a client subcommand; time it directly.
        {
            let started = Instant::now();
            let probe = fixture.next_sequence(&owned, "bob")?;
            let latency = started.elapsed();
            worst = worst.max(latency);
            transcript.push_str(&format!(
                "=== bob next-sequence -> {probe} latency_ms={} ===\n",
                latency.as_millis()
            ));
            ops_log.push_str(&format!(
                "{round}\tnext-sequence\t{}\n",
                latency.as_millis()
            ));
            ensure!(
                latency < BOB_OP_DEADLINE,
                "{scenario_id}: bob next-sequence latency {latency:?} exceeded                  the {BOB_OP_DEADLINE:?} deadline"
            );
        }
        let ops: Vec<(&str, Vec<String>)> = vec![
            (
                "declare",
                vec![
                    "declare".into(),
                    "--operation".into(),
                    oracle::operation_hex(oracle::operation_id(
                        context.seed,
                        "bob-declare",
                        round as u32,
                    )),
                    "--entities".into(),
                    (1000 + round).to_string(),
                ],
            ),
            (
                "admit",
                vec![
                    "admit".into(),
                    "--operation".into(),
                    oracle::operation_hex(oracle::operation_id(
                        context.seed,
                        "bob-admit",
                        round as u32,
                    )),
                    "--declaration".into(),
                    oracle::operation_hex(oracle::operation_id(
                        context.seed,
                        "bob-declare",
                        round as u32,
                    )),
                    "--work".into(),
                    format!("0:0:{}", 1000 + round),
                    "--input".into(),
                    crate::path(&bob_input),
                    "--application".into(),
                    "copy/v2".into(),
                    "--output-count".into(),
                    "1".into(),
                ],
            ),
            (
                "lookup",
                vec![
                    "lookup".into(),
                    "--operation".into(),
                    oracle::operation_hex(oracle::operation_id(
                        context.seed,
                        "bob-admit",
                        round as u32,
                    )),
                ],
            ),
            ("page", vec!["page".into(), "--scope".into(), "0".into()]),
        ];
        for (label, op) in &ops {
            let latency = bob_op(
                &fixture,
                &bob_journal,
                bob_sequence,
                &bob_connection,
                &op.iter().map(String::as_str).collect::<Vec<_>>(),
                label,
                &mut transcript,
            )?;
            worst = worst.max(latency);
            ops_log.push_str(&format!("{round}\t{label}\t{}\n", latency.as_millis()));
        }
        // Evidence is written every round so a later failure never loses it.
        fs::write(artifacts.join("bob-transcript.txt"), &transcript)?;
        fs::write(artifacts.join("b-ops.tsv"), &ops_log)?;
        if !sampled_mid && window_start.elapsed() >= stall_window / 2 {
            sampled_mid = true;
            resources::sample_store(
                "mid-window",
                &[&fixture.state_db, &fixture.object_dir],
                &store_file,
            )?;
        }
        // Enforcement probes at the NEGOTIATED-bound marks, checked inside
        // the round's idle time in short slices rather than once per round.
        // Every probe is non-writing, so running them often costs the
        // subject nothing and renews no deadline, and the resolution of the
        // first-observation bracket is the slice rather than the round.
        let cycle = Duration::from_secs(4);
        let target = cycle * round as u32;
        loop {
            let due: Vec<String> = probe_marks
                .iter_mut()
                .filter(|(_, at, done)| !*done && window_start.elapsed() >= *at)
                .map(|mark| {
                    mark.2 = true;
                    mark.0.clone()
                })
                .collect();
            for label in due {
                let elapsed_ms = window_start.elapsed().as_millis() as u64;
                let stopped = probe_stalled_streams(&alice, &mut stalled, &label, &mut stalls)?;
                for (stream_id, _) in stalled.iter() {
                    if stopped.contains(stream_id) {
                        first_stopped_at
                            .entry(*stream_id)
                            .or_insert_with(|| (label.clone(), elapsed_ms));
                    } else if !first_stopped_at.contains_key(stream_id) {
                        last_open_at.insert(*stream_id, (label.clone(), elapsed_ms));
                    }
                }
                probe_log.push((label, elapsed_ms, stopped.len()));
                aborted_ids.extend(stopped);
                fs::write(artifacts.join("stalls.tsv"), &stalls)?;
                if !aborted_ids.is_empty() {
                    events.append("STALL_ABORT_OBSERVED", None, None, None, None, None)?;
                }
            }
            let elapsed = window_start.elapsed();
            if elapsed >= stall_window || elapsed >= target {
                break;
            }
            thread::sleep(Duration::from_millis(200).min(target - elapsed));
        }
    }
    ensure!(
        probe_marks.iter().all(|(_, _, done)| *done),
        "window ended before every enforcement probe ran: {:?}",
        probe_marks
            .iter()
            .filter(|(_, _, done)| !done)
            .map(|(label, _, _)| label.clone())
            .collect::<Vec<_>>()
    );
    // The window is over: only now is the abusive principal's control stream
    // read, so the queued refusals are the second, protocol-level record of
    // the same enforcement the non-writing probes observed at the transport
    // level.
    let refusals = drain_control_refusals(&mut alice, "window-end", &mut stalls);
    fs::write(artifacts.join("stalls.tsv"), &stalls)?;
    let refused_ids: BTreeSet<u64> = refusals
        .iter()
        .filter(|refusal| {
            refusal.tag_kind == rawclient::TAG_INPUT_STREAM
                && refusal.code == rawclient::CODE_LIMIT_EXCEEDED
        })
        .map(|refusal| refusal.tag_id)
        .collect();
    let unenforced: Vec<u64> = stalled
        .iter()
        .map(|(stream_id, _)| *stream_id)
        .filter(|stream_id| !aborted_ids.contains(stream_id) && !refused_ids.contains(stream_id))
        .collect();
    ensure!(
        unenforced.is_empty(),
        "{scenario_id} {}: stalled input streams {unenforced:?} were never \
         enforced at the negotiated bounds (idle {}s, lifetime {}s): no \
         transport abort by lifetime+10s and no LIMIT_EXCEEDED refusal on the \
         control stream; see artifacts/stalls.tsv",
        server.name(),
        negotiated_idle.as_secs(),
        negotiated_lifetime.as_secs()
    );
    ensure!(
        worst < BOB_OP_DEADLINE,
        "{scenario_id} {}: bob op latency {worst:?} exceeded the {BOB_OP_DEADLINE:?} \
         deadline — control progress is not independent of the stalled principal",
        server.name()
    );
    observed.push((
        "bob_ops",
        format!("{round} rounds x 5 ops, worst latency {worst:?}"),
    ));
    observed.push((
        "stall_enforcement",
        format!(
            "negotiated idle {}s / lifetime {}s; non-writing probes at {}; \
             {}/{STALLED_STREAMS} distinct streams observed stopped; LIMIT_EXCEEDED \
             input refusals on control at window end: {}/{STALLED_STREAMS}",
            negotiated_idle.as_secs(),
            negotiated_lifetime.as_secs(),
            probe_log
                .iter()
                .map(|(label, at, count)| format!("{label} (+{at}ms: {count} stopped)"))
                .collect::<Vec<_>>()
                .join(", "),
            aborted_ids.len(),
            refused_ids.len()
        ),
    ));
    // FIRST-OBSERVATION BRACKET per stream: the last probe that saw it open
    // and the first that saw it stopped. Nothing inside that bracket is
    // claimed. Every probe carries no application data, so nothing here
    // renews the subject's receive deadline; the milestone-17b bracket that
    // did claim a value was an artefact of a client whose transport was
    // only driven inside `block_on`, and it is withdrawn, not restated.
    observed.push((
        "stall_enforcement_bracket",
        stalled
            .iter()
            .map(|(stream_id, _)| {
                let open = last_open_at
                    .get(stream_id)
                    .map(|(label, at)| format!("{label} (+{at}ms)"))
                    .unwrap_or_else(|| "no probe saw it open".to_owned());
                let stopped = first_stopped_at
                    .get(stream_id)
                    .map(|(label, at)| format!("{label} (+{at}ms)"))
                    .unwrap_or_else(|| "never observed stopped".to_owned());
                format!("stream-{stream_id}: last open {open}, first stopped {stopped}")
            })
            .collect::<Vec<_>>()
            .join("; "),
    ));
    fs::write(artifacts.join("bob-transcript.txt"), &transcript)?;
    fs::write(artifacts.join("b-ops.tsv"), &ops_log)?;
    events.append(
        "HEALTHY_PRINCIPAL_PROGRESS",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/b-ops.tsv".into(),
            len: ops_log.len() as u64,
            sha256: oracle::sha256_hex(ops_log.as_bytes()),
        }),
    )?;

    // ---- measurement close-out ----
    // Bounded wait for the close to finish draining: the keep-alive kept this
    // connection alive to the end of the window, so unlike an already-dead
    // connection it is still on the subject's books at this instant. The
    // subject is only signalled to stop after its side has had time to
    // retire the connection; SIGTERM on top of a still-draining connection
    // is a fixture race, and a server that then reports a non-idle transport
    // would be failed for the fixture's timing rather than its own drain.
    alice.close_and_wait_idle(b"stall row complete", Duration::from_secs(15))?;
    let summary = collector.stop()?;
    ensure!(
        summary.error_lines == 0,
        "{scenario_id}: dead collector — {} sampling error lines in resources.tsv",
        summary.error_lines
    );
    observed.push((
        "collector",
        format!(
            "{} ticks, {} sample lines, {} error lines",
            summary.ticks, summary.lines, summary.error_lines
        ),
    ));
    let samples = resources::read_process_samples(&scenario_dir.join("resources.tsv"))?;
    ensure!(
        samples.iter().all(|sample| sample.rss_kb.is_some()
            && sample.fds.is_some()
            && sample.threads.is_some()
            && sample.io_write_bytes.is_some()
            && sample.io_cancelled_write_bytes.is_some()),
        "{scenario_id}: a sample is missing a MANDATORY scope \
         (rss/fd/threads/write_bytes/cancelled_write_bytes)"
    );
    let heap_gap_notes = samples
        .iter()
        .filter(|sample| sample.note.contains("gap:heap"))
        .count();
    let heap_kb: Vec<u64> = samples.iter().filter_map(|sample| sample.heap_kb).collect();
    let (base_rss, tail_rss, base_fd, tail_fd) = plateau_evidence(&samples, anchor)?;
    let rss_growth = tail_rss.saturating_sub(base_rss);
    let (rss_fraction, rss_slack_kb) = match server {
        Subject::Rust => (4u64, 64 * 1024),
        Subject::Java => (2u64, 128 * 1024),
    };
    ensure!(
        rss_growth <= base_rss / rss_fraction + rss_slack_kb,
        "{scenario_id} {}: server RSS grew across the window (baseline median \
         {base_rss} KiB -> tail p90 {tail_rss} KiB, growth {rss_growth} KiB \
         exceeds the plateau tolerance) — not bounded",
        server.name()
    );
    ensure!(
        tail_fd <= base_fd + 8,
        "{scenario_id} {}: server FDs grew across the window (baseline median \
         {base_fd} -> tail p90 {tail_fd}) — not bounded",
        server.name()
    );
    resources::sample_store(
        "end",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    let store_records = resources::read_store_samples(&store_file)?;
    observed.push((
        "rss_plateau_kib",
        format!("baseline_median={base_rss} tail_p90={tail_rss} growth={rss_growth}"),
    ));
    observed.push((
        "fd_plateau",
        format!("baseline_median={base_fd} tail_p90={tail_fd}"),
    ));
    // Disk-I/O scope over the whole window for the anchor pid. write_bytes
    // and cancelled_write_bytes are reported side by side: the difference is
    // what the subject actually pushed towards the device, and an
    // unmeasurable rate is absent rather than zero.
    let write_rate = resources::counter_rate(&samples, anchor, |sample| sample.io_write_bytes);
    let cancelled_rate =
        resources::counter_rate(&samples, anchor, |sample| sample.io_cancelled_write_bytes);
    let rate_text = |rate: Option<(u64, u64, u64)>| match rate {
        Some((delta, span_ms, per_second)) => {
            format!("delta_bytes={delta} span_ms={span_ms} bytes_per_s={per_second}")
        }
        None => "unmeasurable over this window (absent, never zero)".to_owned(),
    };
    observed.push(("io_write_bytes_window", rate_text(write_rate)));
    observed.push(("io_cancelled_write_bytes_window", rate_text(cancelled_rate)));
    observed.push((
        "io_write_cancelled_share",
        match (write_rate, cancelled_rate) {
            (Some((written, _, _)), Some((cancelled, _, _))) if written > 0 => format!(
                "{}% of the window's accounted write_bytes was cancelled \
                 before writeback ({cancelled}/{written})",
                cancelled.saturating_mul(100) / written
            ),
            (Some((0, _, _)), _) => "no accounted write_bytes over the window".to_owned(),
            _ => "unmeasurable: one of the two scopes has no rate".to_owned(),
        },
    ));
    observed.push((
        "heap_scope",
        match server {
            // No JVM exists in a rust subject group, so the Java-heap scope is
            // not applicable rather than collected; the Rust heap scope has no
            // black-box collector at all. RSS is a separate scope and is never
            // reported as either heap.
            Subject::Rust => "not applicable: no JVM in the subject process group. Rust heap is a \
                              named gap (no black-box allocator counter); RSS/HWM is a separate \
                              scope and is never substituted for it"
                .to_owned(),
            Subject::Java if heap_kb.is_empty() => format!(
                "named gap: jstat returned no heap for the subject JVM ({heap_gap_notes} probe \
                 notes); RSS collected and never substituted for the heap scope"
            ),
            Subject::Java => format!(
                "java heap via jstat -gc (S0U+S1U+EU+OU) at the {}ms heap cadence: {} samples, \
                 min {} KiB, max {} KiB, {heap_gap_notes} probe gaps",
                resources::HEAP_SAMPLE_INTERVAL.as_millis(),
                heap_kb.len(),
                heap_kb.iter().min().copied().unwrap_or_default(),
                heap_kb.iter().max().copied().unwrap_or_default(),
            ),
        },
    ));
    observed.push((
        "store_checkpoints",
        format!(
            "{} records across start/stalls-established/mid-window/end",
            store_records.len()
        ),
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    // Fixture timing only, after every measurement is taken and the collector
    // is stopped: see STALL_CLOSE_SETTLE.
    thread::sleep(STALL_CLOSE_SETTLE);
    stop_and_seal(context, scenario_dir, scenario_id, owned, events)
}

// ---------------------------------------------------------------------------
// r-memory-ladder (milestone 18a)
// ---------------------------------------------------------------------------
//
// Two ladders against the limits the subject itself declares in the
// capability selection this row reads on the wire (object_limit,
// stream_limit, pending_limit, control_limit) plus the JVM heap ceiling
// frozen in `process::JAVA_MEMORY_FLAGS` before any measurement row. The
// ladder, the limits and the environment allowance are written into
// expected.tsv BEFORE the first rung's traffic and are never re-chosen from
// what the run produced.
//
// What the row asserts is that memory PLATEAUS at those bounds: the group's
// tail RSS at the largest rung may exceed the smallest rung's only by the
// frozen allowance, which is itself derived from the declared limits
// (stream_limit x object_limit of in-flight object bytes, pending_limit x
// control_limit of in-flight control state) plus a stated per-subject slack.
// Internal counters are not used as RSS evidence anywhere in this row: every
// figure comes from /proc of the whole sampled process group, and the Java
// heap scope is jstat only.

/// Measured payload rungs. 64 MiB is deliberately NOT a measured rung: both
/// subjects declare `object_limit` = 16 MiB in the selection this row reads,
/// so a 64 MiB object never becomes resident anywhere and measuring it would
/// measure a refusal. It is probed once as the over-limit rung instead, and
/// the refusal is recorded verbatim.
const LADDER_PAYLOADS: [usize; 3] = [64 * 1024, 1024 * 1024, 16 * 1024 * 1024];
/// Admissions per payload rung: more than one, because a single payload
/// proves nothing about constant memory.
const LADDER_REPEATS: usize = 4;
/// Declared length of the over-limit probe (4x the declared object_limit).
const LADDER_OVER_LIMIT: u64 = 64 * 1024 * 1024;
/// Cumulative resident admitted works at the end of each inventory rung.
const LADDER_INVENTORY: [u64; 3] = [1, 16, 64];
/// Fixed payload of every inventory-ladder admission, so that ladder varies
/// inventory alone.
const LADDER_INVENTORY_PAYLOAD: usize = 64 * 1024;
/// Entities are declared in batches this size: the rust authority's single
/// transaction cap binds well below the protocol's 256/batch schema bound
/// (g1-declaration-capacity), so the row never relies on one large batch.
const LADDER_DECLARE_BATCH: usize = 16;
/// Idle window measured after readiness and before the first rung.
const LADDER_BASELINE: Duration = Duration::from_secs(15);
/// Quiet settle after each rung's traffic.
const LADDER_SETTLE: Duration = Duration::from_secs(12);
/// Plateau statistics are taken over the LAST part of each rung's settle, so
/// the tail contains no transfer activity of its own rung.
const LADDER_TAIL: Duration = Duration::from_secs(6);
/// Bounded wait for the over-limit refusal.
const LADDER_REFUSAL_WAIT: Duration = Duration::from_secs(10);
/// Bounded wait, AFTER every measurement window, for the ladder's admitted
/// works to reach a terminal state, so the subject is not signalled to stop
/// while its execution pool is still winding them down. Fixture timing, never
/// evidence.
const LADDER_QUIESCE: Duration = Duration::from_secs(120);
/// Admissions per pacing batch. Both subjects declare a concurrent-job
/// ceiling (the Java session ceiling is the lowest at four executor jobs per
/// owner), so the ladders admit in batches of this size and settle each batch
/// before the next. Concurrency is not what these ladders vary.
const LADDER_ADMIT_BATCH: u64 = 2;

/// One ladder rung's window on the collector's clock.
struct Rung {
    label: String,
    kind: &'static str,
    payload_bytes: u64,
    admissions: u64,
    resident_works: u64,
    start_ms: u64,
    tail_from_ms: u64,
    end_ms: u64,
}

fn r_memory_ladder(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(context, "r-memory-ladder", r_memory_ladder_direction)
}

/// Admit one input over the raw peer and read its admission receipt.
/// Returns the stream id the receipt named.
#[allow(clippy::too_many_arguments)]
fn ladder_admit(
    alice: &mut RawConn,
    generation: u64,
    operation: &[u8; 16],
    work: (u64, u64, u64),
    payload: &[u8],
    sha: &[u8; 32],
) -> Result<u64> {
    let header = rawclient::input_header_framed(
        generation,
        operation,
        work,
        payload.len() as u64,
        sha,
        "application/octet-stream",
        "copy/v2",
        0,
        60_000,
        1,
        payload.len() as u64,
    );
    let mut stream = alice.open_uni()?;
    let stream_id = u64::from(stream.id());
    alice.write_stream(&mut stream, &header)?;
    alice.write_stream(&mut stream, payload)?;
    alice.finish_stream(&mut stream)?;
    let admitted = rawclient::parse_admitted_stream(&alice.expect_control(FRAME_WORK)?)?;
    ensure!(
        admitted == stream_id,
        "admission receipt names stream {admitted}, expected {stream_id}"
    );
    Ok(stream_id)
}

/// Bounded wait for every ladder work up to `admitted` to reach a terminal
/// state (5..=8), polling the scope page.
///
/// The ladders vary PAYLOAD and INVENTORY, never executor concurrency: both
/// subjects declare a concurrent-job ceiling well below the inventory ladder's
/// top rung (the Java session's `activeJobs` and per-owner executor bounds
/// refuse the excess with LIMIT_EXCEEDED "retained input, output or executor
/// capacity"), and a memory ladder that tripped a concurrency ceiling would
/// be measuring that refusal instead of memory. Admissions are therefore
/// paced in small batches and each batch is settled before the next, so the
/// rung's resident inventory is retained work, not work in flight. The
/// concurrency ceilings themselves are `r-staging-and-journal-bounds`.
fn ladder_wait_settled(
    alice: &mut RawConn,
    request: &mut u64,
    admitted: u64,
    deadline: Duration,
) -> Result<u64> {
    let until = Instant::now() + deadline;
    loop {
        let (_declared, members) = raw_page(alice, *request, 0)?;
        *request += 1;
        let terminal = members
            .iter()
            .filter(|(entity, state)| *entity <= admitted && (5..=8).contains(state))
            .count() as u64;
        if terminal >= admitted {
            return Ok(terminal);
        }
        ensure!(
            Instant::now() < until,
            "only {terminal}/{admitted} ladder works reached a terminal state \
             within {deadline:?}; the ladder cannot pace its admissions"
        );
        thread::sleep(Duration::from_millis(250));
    }
}

fn r_memory_ladder_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "r-memory-ladder";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let owned = fixture.start_server()?;
    let sequence = fixture.next_sequence(&owned, "alice")?;
    ensure!(
        sequence == 1,
        "fresh authority must report NEXT_SEQUENCE 1, got {sequence}"
    );

    // Measurement gates before any rung: mandatory /proc scopes readable for
    // the anchor, collector clean.
    let anchor = owned.pid()?;
    let permissions = resources::proc_permissions(anchor);
    ensure!(
        permissions.status && permissions.fd && permissions.io,
        "{scenario_id}: mandatory /proc scopes unreadable for the server pid \
         {anchor} (status={} fd={} io={}); an unavailable mandatory metric \
         fails the row, never recorded as zero",
        permissions.status,
        permissions.fd,
        permissions.io
    );
    let store_file = scenario_dir.join("store.tsv");
    let collector = resources::ProcessCollector::start(
        anchor,
        resources::SAMPLE_INTERVAL,
        &scenario_dir.join("resources.tsv"),
    )?;
    let clock = Instant::now();
    let elapsed_ms = |instant: Instant| instant.duration_since(clock).as_millis() as u64;
    resources::sample_store(
        "start",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;

    // Keep-alive: the ladder is quiet for LADDER_SETTLE between rungs and the
    // fixture's own transport must not be what ends the connection. PINGs are
    // transport traffic and carry no object-stream data.
    let peer = Peer::with_keep_alive(Duration::from_secs(5))?;
    let mut alice = raw_negotiate_as(&peer, &fixture, &owned, &mut events, &artifacts, "alice")?;
    let caps = *alice
        .caps()
        .context("capabilities selection was not recorded during negotiation")?;

    // ---- FROZEN before the first rung: ladder, limits, allowances ----
    // The allowances are derived from the limits the SUBJECT declared, plus a
    // stated per-subject slack; they are never widened later to fit what the
    // run produced.
    let in_flight_object_bound = caps.stream_limit.saturating_mul(caps.object_limit);
    let in_flight_control_bound = caps.pending_limit.saturating_mul(caps.control_limit);
    let (slack_kb, slack_reason) = match server {
        Subject::Rust => (
            64 * 1024,
            "64 MiB: allocator retention and page-cache-backed store mappings \
             in a native process with no heap ceiling to bound them",
        ),
        Subject::Java => (
            256 * 1024,
            "256 MiB: JVM warm-up, code cache, GC sawtooth and metaspace \
             growth under a 2 GiB max heap; the heap ceiling itself is the \
             frozen -Xmx and is not re-chosen here",
        ),
    };
    let payload_allowance_kb = in_flight_object_bound / 1024 + slack_kb;
    let inventory_allowance_kb = in_flight_control_bound / 1024 + slack_kb;
    let ladder_text = LADDER_PAYLOADS
        .iter()
        .map(|bytes| format!("{bytes}"))
        .collect::<Vec<_>>()
        .join(",");
    let inventory_text = LADDER_INVENTORY
        .iter()
        .map(|count| format!("{count}"))
        .collect::<Vec<_>>()
        .join(",");
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "frozen_before_decisive_run",
                "this file is written after negotiation and BEFORE the first \
                 rung's traffic; the ladder, the declared limits it is sized \
                 against and the allowances below are fixed here and are never \
                 re-chosen from what the run produced"
                    .into(),
            ),
            ("payload_ladder_bytes", ladder_text.clone()),
            ("payload_repeats_per_rung", LADDER_REPEATS.to_string()),
            ("inventory_ladder_resident_works", inventory_text.clone()),
            (
                "inventory_payload_bytes",
                LADDER_INVENTORY_PAYLOAD.to_string(),
            ),
            (
                "admission_pacing",
                format!(
                    "admissions are issued in batches of {LADDER_ADMIT_BATCH} and every \
                     batch is settled to a terminal state before the next; these ladders \
                     vary payload and RETAINED inventory, never executor concurrency, and \
                     both subjects declare a concurrent-job ceiling below the top inventory \
                     rung (that ceiling is r-staging-and-journal-bounds, not this row)"
                ),
            ),
            (
                "declared_object_limit",
                format!(
                    "{} (subject's capability selection, read on the wire)",
                    caps.object_limit
                ),
            ),
            ("declared_stream_limit", caps.stream_limit.to_string()),
            ("declared_pending_limit", caps.pending_limit.to_string()),
            ("declared_control_limit", caps.control_limit.to_string()),
            (
                "over_limit_rung",
                format!(
                    "one input header declaring {LADDER_OVER_LIMIT} bytes, \
                     {}x the declared object_limit {}: expected LIMIT_EXCEEDED \
                     (code {}) naming the input stream, recorded verbatim. The \
                     payload is never sent, so this rung measures a refusal, \
                     not memory",
                    LADDER_OVER_LIMIT / caps.object_limit.max(1),
                    caps.object_limit,
                    rawclient::CODE_LIMIT_EXCEEDED
                ),
            ),
            (
                "java_memory_freeze",
                format!(
                    "{} on every Java subject process, frozen before any \
                     measurement row (milestone 17) and unchanged here",
                    crate::durable::process::java_memory_flags_text()
                ),
            ),
            (
                "payload_plateau_allowance_kib",
                format!(
                    "{payload_allowance_kb} = stream_limit {} x object_limit {} \
                     ({in_flight_object_bound} B of in-flight object bytes the \
                     subject is configured to hold) + {slack_kb} KiB slack \
                     ({slack_reason})",
                    caps.stream_limit, caps.object_limit
                ),
            ),
            (
                "inventory_plateau_allowance_kib",
                format!(
                    "{inventory_allowance_kb} = pending_limit {} x control_limit \
                     {} ({in_flight_control_bound} B of in-flight control state \
                     the subject is configured to hold) + {slack_kb} KiB slack \
                     ({slack_reason})",
                    caps.pending_limit, caps.control_limit
                ),
            ),
            (
                "plateau_assertion",
                "for each ladder, the group's tail p90 RSS at the LARGEST rung \
                 minus the tail p90 at the SMALLEST rung must not exceed that \
                 ladder's frozen allowance; a subject whose memory scaled with \
                 payload or inventory beyond its configured bounds exceeds it"
                    .into(),
            ),
            (
                "fd_assertion",
                "group FD tail p90 at the last rung <= baseline median + 16".into(),
            ),
            (
                "thread_assertion",
                "group thread tail p90 at the last rung <= baseline median + 64 \
                 (a fixture-chosen bound above both subjects' declared worker \
                 pools, stated rather than derived)"
                    .into(),
            ),
            (
                "measurement_scope",
                "whole sampled process group (anchor pid plus every transitive \
                 descendant), per-tick group SUM then statistic; RSS/HWM, \
                 threads, FDs, disk I/O and Java heap are separate scopes and \
                 none is substituted for another"
                    .into(),
            ),
            (
                "native_direct_scope",
                "Java native/direct allocation is probed once per direction \
                 with jcmd VM.native_memory summary and its exact result is \
                 recorded; it is never inferred from RSS minus heap"
                    .into(),
            ),
            (
                "rust_heap_scope",
                "NAMED GAP: no black-box Rust heap collector exists for the \
                 subject binary; RSS/HWM is a separate scope and is never \
                 reported as Rust heap"
                    .into(),
            ),
        ],
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        (
            "client_subject",
            "rust raw peer (one connection, sequential admissions)".into(),
        ),
        (
            "declared_limits",
            format!(
                "object_limit={} stream_limit={} pending_limit={} \
                 control_limit={} idle_ms={} lifetime_ms={}",
                caps.object_limit,
                caps.stream_limit,
                caps.pending_limit,
                caps.control_limit,
                caps.idle_ms,
                caps.lifetime_ms
            ),
        ),
        (
            "java_memory_freeze",
            match server {
                Subject::Java => format!(
                    "{} (frozen before the run; applies to this server process)",
                    crate::durable::process::java_memory_flags_text()
                ),
                Subject::Rust => format!(
                    "not applicable: no JVM in this subject group (the frozen \
                     Java limits are {})",
                    crate::durable::process::java_memory_flags_text()
                ),
            },
        ),
        ("payload_ladder_bytes", ladder_text),
        ("inventory_ladder_resident_works", inventory_text),
        (
            "payload_plateau_allowance_kib",
            payload_allowance_kb.to_string(),
        ),
        (
            "inventory_plateau_allowance_kib",
            inventory_allowance_kb.to_string(),
        ),
    ];

    // ---- baseline rung: idle, no ladder traffic ----
    let mut rungs: Vec<Rung> = Vec::new();
    let baseline_start = elapsed_ms(Instant::now());
    thread::sleep(LADDER_BASELINE);
    let baseline_end = elapsed_ms(Instant::now());
    rungs.push(Rung {
        label: "baseline-idle".into(),
        kind: "baseline",
        payload_bytes: 0,
        admissions: 0,
        resident_works: 0,
        start_ms: baseline_start,
        tail_from_ms: baseline_end.saturating_sub(LADDER_TAIL.as_millis() as u64),
        end_ms: baseline_end,
    });

    let binding = raw_create_session(&mut alice)?;
    let mut request = 2u64;
    let total_entities = (LADDER_PAYLOADS.len() * LADDER_REPEATS) as u64
        + LADDER_INVENTORY[LADDER_INVENTORY.len() - 1];
    // One entity beyond the ladders is declared for the over-limit probe.
    // Membership is checked before the declared length is, so an undeclared
    // entity would be refused CONFLICT ("input membership was not declared")
    // and the row would never reach the object_limit decision it is there to
    // observe.
    let declare_target = total_entities + 1;
    let mut declared = 0u64;
    while declared < declare_target {
        let batch: Vec<u64> =
            (declared + 1..=(declared + LADDER_DECLARE_BATCH as u64).min(declare_target)).collect();
        let operation = oracle::operation_id(context.seed, "ladder-declare", declared as u32);
        raw_declare(&mut alice, request, &operation, 0, &batch, false)?;
        request += 1;
        declared += batch.len() as u64;
    }
    events.append("DECLARATION_COMMITTED", None, None, None, None, None)?;
    observed.push(("declared_entities", declared.to_string()));

    // ---- payload ladder ----
    let mut entity = 0u64;
    let mut resident = 0u64;
    for payload_bytes in LADDER_PAYLOADS {
        let payload = oracle::dataset(context.seed ^ payload_bytes as u64, payload_bytes);
        let mut sha = [0u8; 32];
        sha.copy_from_slice(&crate::decode_hex(&oracle::sha256_hex(&payload))?);
        let start = elapsed_ms(Instant::now());
        for repeat in 0..LADDER_REPEATS {
            entity += 1;
            let operation = oracle::operation_id(
                context.seed,
                "ladder-payload",
                (entity * 16 + repeat as u64) as u32,
            );
            ladder_admit(
                &mut alice,
                binding.generation,
                &operation,
                (0, 0, entity),
                &payload,
                &sha,
            )?;
            resident += 1;
            if resident.is_multiple_of(LADDER_ADMIT_BATCH) {
                ladder_wait_settled(&mut alice, &mut request, resident, LADDER_QUIESCE)?;
            }
        }
        ladder_wait_settled(&mut alice, &mut request, resident, LADDER_QUIESCE)?;
        thread::sleep(LADDER_SETTLE);
        let end = elapsed_ms(Instant::now());
        rungs.push(Rung {
            label: format!("payload-{payload_bytes}"),
            kind: "payload",
            payload_bytes: payload_bytes as u64,
            admissions: LADDER_REPEATS as u64,
            resident_works: resident,
            start_ms: start,
            tail_from_ms: end.saturating_sub(LADDER_TAIL.as_millis() as u64),
            end_ms: end,
        });
        resources::sample_store(
            &format!("payload-{payload_bytes}"),
            &[&fixture.state_db, &fixture.object_dir],
            &store_file,
        )?;
    }
    events.append("ADMISSION_COMMITTED", None, None, None, None, None)?;

    // ---- over-limit rung: declared length above the declared object_limit ----
    // The payload is never sent: the refusal is a decision about the declared
    // parameters, and sending 64 MiB the subject has already refused would
    // measure the fixture, not the subject.
    let over_entity = total_entities + 1;
    let over_operation = oracle::operation_id(context.seed, "ladder-over-limit", 0);
    let over_header = rawclient::input_header_framed(
        binding.generation,
        &over_operation,
        (0, 0, over_entity),
        LADDER_OVER_LIMIT,
        &[0u8; 32],
        "application/octet-stream",
        "copy/v2",
        0,
        60_000,
        1,
        LADDER_OVER_LIMIT,
    );
    let mut over_stream = alice.open_uni()?;
    let over_stream_id = u64::from(over_stream.id());
    alice.write_stream(&mut over_stream, &over_header)?;
    let over_outcome = match alice.read_control_bounded(LADDER_REFUSAL_WAIT)? {
        Some(Frame::Control(FRAME_REFUSAL, body)) => {
            let refusal = rawclient::parse_refusal(&body)?;
            ensure!(
                refusal.code == rawclient::CODE_LIMIT_EXCEEDED,
                "{scenario_id} {}: the over-limit rung was refused with code {} \
                 ({:?}), expected LIMIT_EXCEEDED ({})",
                server.name(),
                refusal.code,
                refusal.detail,
                rawclient::CODE_LIMIT_EXCEEDED
            );
            format!(
                "refused: tag_kind={} tag_id={} code={} detail={:?} \
                 (stream {over_stream_id})",
                refusal.tag_kind, refusal.tag_id, refusal.code, refusal.detail
            )
        }
        Some(other) => bail!(
            "{scenario_id} {}: the over-limit rung produced {other:?} instead of \
             a refusal",
            server.name()
        ),
        None => bail!(
            "{scenario_id} {}: no refusal within {:?} for an input declaring \
             {LADDER_OVER_LIMIT} bytes against a declared object_limit of {}; \
             the row records no memory figure for this rung",
            server.name(),
            LADDER_REFUSAL_WAIT,
            caps.object_limit
        ),
    };
    let _ = alice.reset_stream(&mut over_stream, rawclient::CODE_LIMIT_EXCEEDED);
    observed.push(("over_limit_rung", over_outcome));
    events.append("LIMIT_REFUSAL_OBSERVED", None, None, None, None, None)?;

    // ---- inventory ladder: fixed payload, growing resident inventory ----
    let inv_payload = oracle::dataset(context.seed ^ 0x1_0000, LADDER_INVENTORY_PAYLOAD);
    let mut inv_sha = [0u8; 32];
    inv_sha.copy_from_slice(&crate::decode_hex(&oracle::sha256_hex(&inv_payload))?);
    let inventory_base = resident;
    for target in LADDER_INVENTORY {
        let start = elapsed_ms(Instant::now());
        let mut admitted_here = 0u64;
        while resident - inventory_base < target {
            entity += 1;
            let operation = oracle::operation_id(context.seed, "ladder-inventory", entity as u32);
            ladder_admit(
                &mut alice,
                binding.generation,
                &operation,
                (0, 0, entity),
                &inv_payload,
                &inv_sha,
            )?;
            resident += 1;
            admitted_here += 1;
            if resident.is_multiple_of(LADDER_ADMIT_BATCH) {
                ladder_wait_settled(&mut alice, &mut request, resident, LADDER_QUIESCE)?;
            }
        }
        ladder_wait_settled(&mut alice, &mut request, resident, LADDER_QUIESCE)?;
        thread::sleep(LADDER_SETTLE);
        let end = elapsed_ms(Instant::now());
        rungs.push(Rung {
            label: format!("inventory-{target}"),
            kind: "inventory",
            payload_bytes: LADDER_INVENTORY_PAYLOAD as u64,
            admissions: admitted_here,
            resident_works: resident,
            start_ms: start,
            tail_from_ms: end.saturating_sub(LADDER_TAIL.as_millis() as u64),
            end_ms: end,
        });
        resources::sample_store(
            &format!("inventory-{target}"),
            &[&fixture.state_db, &fixture.object_dir],
            &store_file,
        )?;
    }
    observed.push(("resident_works_final", resident.to_string()));

    // ---- quiesce before the close-out ----
    // Every measurement window has closed. The ladder leaves the authority
    // with `resident` admitted works, and a subject signalled to stop while
    // its execution pool is still winding those down spends its fixed
    // shutdown grace on the pool instead of on the transport wait — which
    // reads as a failed drain and is fixture timing, not a subject defect
    // (the same mechanism milestone 17 recorded for the stall row). The row
    // therefore waits, bounded, for every admitted work to be terminal
    // before it signals anything. This is after the last rung's window: no
    // measurement is taken here.
    let quiesce_deadline = Instant::now() + LADDER_QUIESCE;
    let terminal_works = loop {
        let (_declared, members) = raw_page(&mut alice, request, 0)?;
        request += 1;
        let terminal = members
            .iter()
            .filter(|(entity, state)| *entity <= resident && (5..=8).contains(state))
            .count();
        if terminal as u64 >= resident || Instant::now() >= quiesce_deadline {
            break terminal;
        }
        thread::sleep(Duration::from_secs(2));
    };
    observed.push((
        "quiesce_before_stop",
        format!(
            "{terminal_works}/{resident} admitted works terminal within {:?} \
             (fixture timing after every measurement window; never evidence)",
            LADDER_QUIESCE
        ),
    ));

    // ---- Java native/direct scope: probed, never inferred ----
    let native_scope = match server {
        Subject::Rust => "not applicable: no JVM in the subject process group".to_owned(),
        Subject::Java => java_native_memory_probe(anchor, &artifacts)?,
    };
    observed.push(("java_native_direct_scope", native_scope));

    // ---- measurement close-out ----
    alice.close_and_wait_idle(b"memory ladder complete", Duration::from_secs(15))?;
    let summary = collector.stop()?;
    ensure!(
        summary.error_lines == 0,
        "{scenario_id}: dead collector — {} sampling error lines in resources.tsv",
        summary.error_lines
    );
    observed.push((
        "collector",
        format!(
            "{} ticks, {} sample lines, {} error lines",
            summary.ticks, summary.lines, summary.error_lines
        ),
    ));
    let samples = resources::read_process_samples(&scenario_dir.join("resources.tsv"))?;
    ensure!(
        samples.iter().all(|sample| sample.rss_kb.is_some()
            && sample.hwm_kb.is_some()
            && sample.fds.is_some()
            && sample.threads.is_some()
            && sample.io_write_bytes.is_some()
            && sample.io_cancelled_write_bytes.is_some()),
        "{scenario_id}: a sample is missing a MANDATORY scope \
         (rss/hwm/fd/threads/write_bytes/cancelled_write_bytes)"
    );

    // Per-rung plateau evidence over the whole process group.
    let mut table = String::from(
        "rung\tkind\tpayload_bytes\tadmissions\tresident_works\tstart_ms\ttail_from_ms\tend_ms\t\
         ticks\tpids_min\tpids_max\trss_median_kib\trss_p90_kib\trss_max_kib\thwm_max_kib\t\
         threads_median\tthreads_max\tfd_median\tfd_p90\theap_ticks\theap_min_kib\theap_max_kib\t\
         heap_gap_ticks\n",
    );
    let mut stats_by_label: Vec<(String, resources::WindowStats)> = Vec::new();
    for rung in &rungs {
        let stats =
            resources::group_window_stats(&samples, rung.tail_from_ms, rung.end_ms, &rung.label)?;
        table.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            rung.label,
            rung.kind,
            rung.payload_bytes,
            rung.admissions,
            rung.resident_works,
            rung.start_ms,
            rung.tail_from_ms,
            rung.end_ms,
            stats.ticks,
            stats.pids_min,
            stats.pids_max,
            stats.rss_median_kb,
            stats.rss_p90_kb,
            stats.rss_max_kb,
            stats.hwm_max_kb,
            stats.threads_median,
            stats.threads_max,
            stats.fd_median,
            stats.fd_p90,
            stats.heap_ticks,
            stats
                .heap_min_kb
                .map(|kb| kb.to_string())
                .unwrap_or_else(|| "-".into()),
            stats
                .heap_max_kb
                .map(|kb| kb.to_string())
                .unwrap_or_else(|| "-".into()),
            stats.heap_gap_ticks,
        ));
        stats_by_label.push((rung.label.clone(), stats));
    }
    fs::write(artifacts.join("rungs.tsv"), &table)?;
    events.append(
        "LADDER_EVIDENCE",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/rungs.tsv".into(),
            len: table.len() as u64,
            sha256: oracle::sha256_hex(table.as_bytes()),
        }),
    )?;

    let find = |label: &str| -> Result<&resources::WindowStats> {
        stats_by_label
            .iter()
            .find(|(name, _)| name == label)
            .map(|(_, stats)| stats)
            .with_context(|| format!("rung {label} has no window statistics"))
    };
    let baseline = find("baseline-idle")?;
    let payload_first = find(&format!("payload-{}", LADDER_PAYLOADS[0]))?;
    let payload_last = find(&format!(
        "payload-{}",
        LADDER_PAYLOADS[LADDER_PAYLOADS.len() - 1]
    ))?;
    let inventory_first = find(&format!("inventory-{}", LADDER_INVENTORY[0]))?;
    let inventory_last = find(&format!(
        "inventory-{}",
        LADDER_INVENTORY[LADDER_INVENTORY.len() - 1]
    ))?;

    let payload_growth = payload_last
        .rss_p90_kb
        .saturating_sub(payload_first.rss_p90_kb);
    let inventory_growth = inventory_last
        .rss_p90_kb
        .saturating_sub(inventory_first.rss_p90_kb);
    observed.push((
        "baseline_rss_kib",
        format!(
            "median={} p90={} max={} over {} ticks",
            baseline.rss_median_kb, baseline.rss_p90_kb, baseline.rss_max_kb, baseline.ticks
        ),
    ));
    observed.push((
        "payload_ladder_rss_kib",
        format!(
            "{}B tail_p90={} ({} ticks) -> {}B tail_p90={} ({} ticks), growth={} \
             against the frozen allowance {}",
            LADDER_PAYLOADS[0],
            payload_first.rss_p90_kb,
            payload_first.ticks,
            LADDER_PAYLOADS[LADDER_PAYLOADS.len() - 1],
            payload_last.rss_p90_kb,
            payload_last.ticks,
            payload_growth,
            payload_allowance_kb
        ),
    ));
    observed.push((
        "payload_ladder_scaling",
        format!(
            "payload grew by {} B per admission ({}x); group tail p90 RSS grew by \
             {} KiB. A subject that buffered each payload once would have grown \
             by at least {} KiB",
            LADDER_PAYLOADS[LADDER_PAYLOADS.len() - 1] - LADDER_PAYLOADS[0],
            LADDER_PAYLOADS[LADDER_PAYLOADS.len() - 1] / LADDER_PAYLOADS[0],
            payload_growth,
            (LADDER_PAYLOADS[LADDER_PAYLOADS.len() - 1] - LADDER_PAYLOADS[0]) / 1024
        ),
    ));
    observed.push((
        "inventory_ladder_rss_kib",
        format!(
            "{} resident tail_p90={} ({} ticks) -> {} resident tail_p90={} ({} \
             ticks), growth={} against the frozen allowance {}",
            LADDER_INVENTORY[0],
            inventory_first.rss_p90_kb,
            inventory_first.ticks,
            LADDER_INVENTORY[LADDER_INVENTORY.len() - 1],
            inventory_last.rss_p90_kb,
            inventory_last.ticks,
            inventory_growth,
            inventory_allowance_kb
        ),
    ));
    observed.push((
        "hwm_kib",
        format!(
            "baseline={} payload_last={} inventory_last={}",
            baseline.hwm_max_kb, payload_last.hwm_max_kb, inventory_last.hwm_max_kb
        ),
    ));
    observed.push((
        "threads",
        format!(
            "baseline median={} -> last rung median={} max={}",
            baseline.threads_median, inventory_last.threads_median, inventory_last.threads_max
        ),
    ));
    observed.push((
        "fds",
        format!(
            "baseline median={} -> last rung p90={} max={}",
            baseline.fd_median, inventory_last.fd_p90, inventory_last.fd_max
        ),
    ));
    observed.push((
        "heap_scope",
        match server {
            Subject::Rust => "not applicable: no JVM in the subject process group. Rust heap is a \
                              NAMED GAP (no black-box allocator counter); RSS/HWM is a separate \
                              scope and is never substituted for it"
                .to_owned(),
            Subject::Java => {
                let ticks: usize = stats_by_label.iter().map(|(_, s)| s.heap_ticks).sum();
                let gaps: usize = stats_by_label.iter().map(|(_, s)| s.heap_gap_ticks).sum();
                let min = stats_by_label
                    .iter()
                    .filter_map(|(_, s)| s.heap_min_kb)
                    .min();
                let max = stats_by_label
                    .iter()
                    .filter_map(|(_, s)| s.heap_max_kb)
                    .max();
                format!(
                    "java heap via jstat -gc (S0U+S1U+EU+OU) inside the measured rung \
                     windows: {ticks} heap ticks, {gaps} probe gaps, min {} KiB, max {} \
                     KiB, against the frozen -Xmx ceiling",
                    min.map(|kb| kb.to_string()).unwrap_or_else(|| "-".into()),
                    max.map(|kb| kb.to_string()).unwrap_or_else(|| "-".into()),
                )
            }
        },
    ));

    // ---- assertions ----
    ensure!(
        payload_growth <= payload_allowance_kb,
        "{scenario_id} {}: group RSS scaled with PAYLOAD beyond the configured \
         bound — tail p90 {} KiB at {} B grew to {} KiB at {} B (growth {} KiB, \
         frozen allowance {} KiB)",
        server.name(),
        payload_first.rss_p90_kb,
        LADDER_PAYLOADS[0],
        payload_last.rss_p90_kb,
        LADDER_PAYLOADS[LADDER_PAYLOADS.len() - 1],
        payload_growth,
        payload_allowance_kb
    );
    ensure!(
        inventory_growth <= inventory_allowance_kb,
        "{scenario_id} {}: group RSS scaled with INVENTORY beyond the configured \
         bound — tail p90 {} KiB at {} resident works grew to {} KiB at {} \
         resident works (growth {} KiB, frozen allowance {} KiB)",
        server.name(),
        inventory_first.rss_p90_kb,
        LADDER_INVENTORY[0],
        inventory_last.rss_p90_kb,
        LADDER_INVENTORY[LADDER_INVENTORY.len() - 1],
        inventory_growth,
        inventory_allowance_kb
    );
    ensure!(
        inventory_last.fd_p90 <= baseline.fd_median + 16,
        "{scenario_id} {}: group FDs grew across the ladder (baseline median {} \
         -> last rung p90 {})",
        server.name(),
        baseline.fd_median,
        inventory_last.fd_p90
    );
    ensure!(
        inventory_last.threads_max <= baseline.threads_median + 64,
        "{scenario_id} {}: group threads grew across the ladder (baseline median \
         {} -> last rung max {})",
        server.name(),
        baseline.threads_median,
        inventory_last.threads_max
    );
    if server == Subject::Java {
        let gaps: usize = stats_by_label.iter().map(|(_, s)| s.heap_gap_ticks).sum();
        let ticks: usize = stats_by_label.iter().map(|(_, s)| s.heap_ticks).sum();
        ensure!(
            gaps == 0 && ticks > 0,
            "{scenario_id} java: the Java heap scope is MANDATORY for a JVM \
             subject and must be collected on every heap tick ({ticks} collected, \
             {gaps} gaps)"
        );
    }

    resources::sample_store(
        "end",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    let store_records = resources::read_store_samples(&store_file)?;
    observed.push((
        "store_checkpoints",
        format!(
            "{} records across the rung checkpoints",
            store_records.len()
        ),
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    thread::sleep(STALL_CLOSE_SETTLE);
    stop_and_seal(context, scenario_dir, scenario_id, owned, events)
}

/// Java native/direct allocation probe. The exact command and its exact
/// result are recorded; nothing is inferred from RSS minus heap, and the
/// frozen launch flags are NOT changed to enable a collector mid-matrix.
fn java_native_memory_probe(pid: u32, artifacts: &Path) -> Result<String> {
    let Some(jcmd) = resources::tool_on_path("jcmd") else {
        return Ok("UNAVAILABLE: no jcmd on PATH (named gap; never inferred from RSS)".to_owned());
    };
    let mut transcript = String::new();
    let mut summary = String::new();
    for (label, arguments) in [
        ("VM.native_memory", vec!["VM.native_memory", "summary"]),
        ("VM.flags", vec!["VM.flags"]),
    ] {
        let output = std::process::Command::new(&jcmd)
            .arg(pid.to_string())
            .args(&arguments)
            .output()
            .with_context(|| format!("run jcmd {} {label}", pid))?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        transcript.push_str(&format!(
            "=== jcmd {pid} {} (exit {}) ===\n{stdout}{stderr}\n",
            arguments.join(" "),
            output.status
        ));
        if label == "VM.native_memory" {
            let text = format!("{stdout}{stderr}");
            summary = if text.contains("Native memory tracking is not enabled") {
                "UNAVAILABLE: jcmd VM.native_memory summary reports \"Native memory \
                 tracking is not enabled\". Enabling it needs -XX:NativeMemoryTracking \
                 added to the JVM launch flags, which are FROZEN before this matrix's \
                 measurement rows (-Xms256m -Xmx2g); changing them mid-matrix would \
                 re-open every earlier R row's frozen environment. Recorded as a named \
                 gap, never inferred from RSS minus heap"
                    .to_owned()
            } else if output.status.success() {
                let total = text
                    .lines()
                    .find(|line| line.trim_start().starts_with("Total:"))
                    .unwrap_or("(no Total: line)")
                    .trim()
                    .to_owned();
                format!("collected by jcmd VM.native_memory summary: {total}")
            } else {
                format!(
                    "UNAVAILABLE: jcmd VM.native_memory summary exited {} — {}",
                    output.status,
                    text.lines().next().unwrap_or("(no output)")
                )
            };
        }
    }
    let path = artifacts.join("jcmd-native.txt");
    fs::write(&path, &transcript)?;
    Ok(format!("{summary} (transcript artifacts/jcmd-native.txt)"))
}

// ---------------------------------------------------------------------------
// r-staging-and-journal-bounds (milestone 18b)
// ---------------------------------------------------------------------------
//
// Drives the ceilings each subject DECLARES — the capability selection's
// pending_limit and the session binding receipt's retained-record limits —
// until new work is refused, and proves the four properties the matrix asks
// for: a named refusal for NEW work, EXISTING promises still completing,
// bounded file handles, and capacity that stays charged while physical I/O
// is busy and reconciles after safe cleanup and after restart.
//
// Every ceiling in this row is read off the wire, never quoted from a
// library default: `r-memory-ladder` already found the Java listener
// advertising a pending_limit its own `DurableOptions.defaults()` does not.

/// Declared length of one staging input. The prefix below is what is
/// actually sent, so every staging stream stays an incomplete transfer
/// holding a staging object without ever committing one.
const STAGING_DECLARED_LEN: usize = 64 * 1024;
const STAGING_PREFIX_LEN: usize = 4 * 1024;
/// Hard cap on the connections this row opens per principal. Both subjects
/// enforce a per-principal connection ceiling of their own
/// (r-connection-ceiling: rust 4, java 8), so the row walks principals when
/// one principal's connections run out.
const STAGING_MAX_CONNECTIONS_PER_PRINCIPAL: u64 = 8;
/// Hard cap on staging streams opened in total. A cap is not a bound: if it
/// is reached without a refusal, the staging arm is recorded as NOT REACHED
/// with the cap and the declared ceilings, never as a pass.
const STAGING_MAX_STREAMS: u64 = 200;
/// Bounded wait for a refusal or an admission receipt on control.
const STAGING_REPLY_WAIT: Duration = Duration::from_secs(10);
/// Bounded wait for a single-shot probe transfer.
///
/// It must stay BELOW the smaller of the two subjects. negotiated object
/// idle bounds (rust 5 s, java 30 s). A probe holds an incomplete transfer
/// open while it waits, so a longer wait lets the subject reap that very
/// transfer at its input receive deadline and the probe reads a
/// LIMIT_EXCEEDED "input receive deadline" refusal as if capacity had been
/// refused. A capacity refusal is immediate; a deadline refusal is not.
const STAGING_PROBE_WAIT: Duration = Duration::from_secs(2);
/// Bounded drain after each per-connection batch of staging attempts. The
/// row drains once per batch rather than waiting after every attempt: a
/// refusal carries the input-stream tag it belongs to, so batching loses no
/// attribution, and a per-attempt wait made the sweep last longer than the
/// subject's own input receive deadline, which then reaped the earliest
/// transfers while later ones were still being opened.
const STAGING_BATCH_DRAIN: Duration = Duration::from_millis(500);
/// Deadline on the watches that fill the pending-response ceiling. The
/// protocol bounds a wait at 30 s (`WaitMs`), and a larger value is a
/// FRAME_ERROR rather than a longer wait, so this is the maximum a filler
/// watch may ask for.
const STAGING_PENDING_DEADLINE_MS: u64 = 30_000;
/// Revision the filler watches wait PAST. A declared-never-admitted work
/// sits at the revision its declaration produced, so a watch for anything
/// after that revision stays genuinely pending until the work changes — and
/// then it is answered, which is how this row shows the granted waits were
/// kept. A revision that can never arrive would only ever be answered by the
/// wait deadline, which proves nothing about the promise.
const STAGING_PENDING_AFTER_REVISION: u64 = 1;
/// Bounded drain for the waits granted before exhaustion. It is longer than
/// the filler watches' own deadline, so a wait answered at its deadline
/// rather than at the next revision still counts as a promise kept.
const STAGING_PENDING_DRAIN: Duration = Duration::from_secs(50);
/// Quiet window after releasing the staging streams, so safe cleanup can run
/// before the row asks whether capacity came back.
const STAGING_CLEANUP_SETTLE: Duration = Duration::from_secs(20);
/// Bounded wait for capacity to come back after cleanup / after restart.
const STAGING_RECOVERY_WAIT: Duration = Duration::from_secs(60);

fn r_staging_and_journal_bounds(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(
        context,
        "r-staging-and-journal-bounds",
        r_staging_and_journal_bounds_direction,
    )
}

/// One staging attempt: an input header plus a partial payload and no FIN.
/// The stream is returned held open. `reply_wait` is `Some` only for the
/// single-shot probes, which must see their own answer immediately; the
/// sweep passes `None` and drains the whole batch's control frames once,
/// matching each refusal to its attempt by input-stream tag.
struct StagingAttempt {
    stream: quinn::SendStream,
    stream_id: u64,
    refusal: Option<rawclient::Refusal>,
}

fn staging_open(
    conn: &mut RawConn,
    generation: u64,
    operation: &[u8; 16],
    work: (u64, u64, u64),
    payload_sha: &[u8; 32],
    prefix: &[u8],
    reply_wait: Option<Duration>,
) -> Result<StagingAttempt> {
    let header = rawclient::input_header_framed(
        generation,
        operation,
        work,
        STAGING_DECLARED_LEN as u64,
        payload_sha,
        "application/octet-stream",
        "copy/v2",
        0,
        60_000,
        1,
        STAGING_DECLARED_LEN as u64,
    );
    let mut stream = conn.open_uni()?;
    let stream_id = u64::from(stream.id());
    conn.write_stream(&mut stream, &header)?;
    conn.write_stream(&mut stream, prefix)?;
    // A subject that refuses the staging reservation answers on control
    // straight away; one that accepts it stays silent until the transfer
    // FINs, because an incomplete input is not an admission.
    let refusal = match reply_wait {
        Some(wait) => match conn.read_control_bounded(wait)? {
            Some(Frame::Control(FRAME_REFUSAL, body)) => Some(rawclient::parse_refusal(&body)?),
            _ => None,
        },
        None => None,
    };
    Ok(StagingAttempt {
        stream,
        stream_id,
        refusal,
    })
}

/// One held connection of the staging phase.
struct StagingConn {
    /// Recorded on every ceilings.tsv line this connection produced.
    #[allow(dead_code)]
    principal: String,
    conn: RawConn,
    streams: Vec<quinn::SendStream>,
}

fn r_staging_and_journal_bounds_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "r-staging-and-journal-bounds";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;
    let certs = mtls::generate(
        &scenario_dir.join("certs"),
        &[
            ("alice", "alice"),
            ("bob", "bob"),
            ("carol", "carol"),
            ("dave", "dave"),
        ],
    )?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let mut owned = fixture.start_server()?;
    ensure!(
        fixture.next_sequence(&owned, "alice")? == 1,
        "fresh authority must report NEXT_SEQUENCE 1"
    );

    let anchor = owned.pid()?;
    let permissions = resources::proc_permissions(anchor);
    ensure!(
        permissions.status && permissions.fd && permissions.io,
        "{scenario_id}: mandatory /proc scopes unreadable for the server pid {anchor} \
         (status={} fd={} io={})",
        permissions.status,
        permissions.fd,
        permissions.io
    );
    let store_file = scenario_dir.join("store.tsv");
    let collector = resources::ProcessCollector::start(
        anchor,
        resources::SAMPLE_INTERVAL,
        &scenario_dir.join("resources.tsv"),
    )?;
    let clock = Instant::now();
    let elapsed_ms = |instant: Instant| instant.duration_since(clock).as_millis() as u64;
    resources::sample_store(
        "start",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    // Baseline handles/RSS before the row opens anything.
    thread::sleep(Duration::from_secs(10));
    let baseline_to_ms = elapsed_ms(Instant::now());

    let peer = Peer::with_keep_alive(Duration::from_secs(5))?;
    let mut alice = raw_negotiate_as(&peer, &fixture, &owned, &mut events, &artifacts, "alice")?;
    let caps = *alice
        .caps()
        .context("capabilities selection was not recorded during negotiation")?;
    let binding = raw_create_session(&mut alice)?;
    let session_limits = binding.limits;

    let mut log = String::from("phase\tattempt\tprincipal\tconnection\toutcome\tdetail\n");
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        (
            "client_subject",
            "rust raw peer (several connections, incomplete staging transfers held open)".into(),
        ),
        (
            "declared_pending_limit",
            format!(
                "{} (capability selection, read on the wire)",
                caps.pending_limit
            ),
        ),
        (
            "declared_stream_limit",
            format!("{} (capability selection)", caps.stream_limit),
        ),
        (
            "declared_session_limits",
            format!("{} (session binding receipt)", session_limits.text()),
        ),
        (
            "java_memory_freeze",
            match server {
                Subject::Java => format!(
                    "{} (frozen before the run)",
                    crate::durable::process::java_memory_flags_text()
                ),
                Subject::Rust => "not applicable: no JVM in this subject group".to_owned(),
            },
        ),
    ];

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "declared_ceilings_source",
                "every ceiling this row drives is read off the wire before it is driven: \
                 the capability selection (pending_limit, stream_limit, object_limit) and \
                 the session binding receipt's retained-record limits. No library default \
                 is quoted as a subject bound"
                    .into(),
            ),
            ("pending_limit", caps.pending_limit.to_string()),
            ("stream_limit", caps.stream_limit.to_string()),
            ("session_limits", session_limits.text()),
            (
                "new_work_refusal",
                format!(
                    "at exhaustion a NEW request or a NEW staging transfer is refused with \
                     LIMIT_EXCEEDED (code {}) and the subject's detail string is recorded \
                     verbatim, together with the attempt number the refusal arrived on",
                    rawclient::CODE_LIMIT_EXCEEDED
                ),
            ),
            (
                "existing_promises",
                "the requests already granted before exhaustion still answer, and a staging \
                 transfer already accepted before exhaustion still completes to an admission \
                 receipt when it FINs"
                    .into(),
            ),
            (
                "handles_bounded",
                "the server group's FD tail p90 returns to within baseline median + 8 after \
                 the staging streams are released and their connections closed"
                    .into(),
            ),
            (
                "charge_while_busy",
                "while the staging transfers are held the ceiling stays charged: a further \
                 attempt is refused again with the same named code"
                    .into(),
            ),
            (
                "reconcile_after_cleanup",
                format!(
                    "after the staging streams are released and a {}s cleanup settle, a fresh \
                     staging transfer is accepted within {}s",
                    STAGING_CLEANUP_SETTLE.as_secs(),
                    STAGING_RECOVERY_WAIT.as_secs()
                ),
            ),
            (
                "reconcile_after_restart",
                "after the subject is stopped and restarted on the same roots, a fresh \
                 staging transfer is accepted again — the charge did not survive as a leak — \
                 and the store's file lengths and allocated blocks are sampled either side \
                 of the restart"
                    .into(),
            ),
            (
                "journal_ceiling_scope",
                "the journal/retained-BYTE ceilings are recorded from the binding receipt \
                 and the subject's configuration and the actual retained bytes are sampled \
                 at every checkpoint, but they are NOT driven to exhaustion by this row: \
                 doing so needs hundreds of megabytes of committed records. That arm is \
                 PARTIAL with this reason, never a skip and never a pass"
                    .into(),
            ),
        ],
    )?;

    // Two further connections of the SAME owner are attached now, BEFORE
    // the pending phase fills the subject's control-side capacity: one that
    // will admit the watched work so the granted waits can be kept, and one
    // reserved for the "is the capacity still charged?" probe. An attach
    // attempted after the fill is itself refused (the rust authority answers
    // LIMIT_EXCEEDED "metadata concurrency exhausted"), which would make the
    // row's own scaffolding a casualty of the ceiling it is measuring.
    let mut admitter = raw_negotiate_as(&peer, &fixture, &owned, &mut events, &artifacts, "alice")?;
    admitter.send_control(
        FRAME_SESSION,
        &rawclient::session_attach(1, &binding.authority, &binding.owner, binding.generation),
    )?;
    let attached = rawclient::parse_binding(&admitter.expect_control(FRAME_SESSION)?)?;
    ensure!(
        attached.generation == binding.generation,
        "attach receipt generation {} differs from {}",
        attached.generation,
        binding.generation
    );
    let mut prober = raw_negotiate_as(&peer, &fixture, &owned, &mut events, &artifacts, "alice")?;
    prober.send_control(
        FRAME_SESSION,
        &rawclient::session_attach(1, &binding.authority, &binding.owner, binding.generation),
    )?;
    rawclient::parse_binding(&prober.expect_control(FRAME_SESSION)?)?;
    let prober_request = 2u64;

    // ---- Phase A: the pending-control-work ceiling ----
    // Filled with watches on a declared-never-admitted work, waiting past the
    // revision its declaration produced, so each one stays genuinely pending
    // until the work changes. Each attempt is checked for its OWN refusal
    // before the next is sent: a refusal carries the request tag it belongs
    // to, and reading one frame after a whole burst would attribute an early
    // refusal to the last attempt. The declared pending_limit is recorded
    // beside the ceiling that actually fires, which need not be the same
    // thing — the rust authority refuses on metadata concurrency first.
    let declare_op = oracle::operation_id(context.seed, "staging-declare-a", 0);
    let mut request = 2u64;
    raw_declare(&mut alice, request, &declare_op, 0, &[1], false)?;
    request += 1;
    let pending_target = caps.pending_limit;
    let mut pending_accepted = 0u64;
    let mut pending_answered_early = 0u64;
    let mut pending_refusal: Option<(u64, u64, rawclient::Refusal)> = None;
    for attempt in 1..=(pending_target + 4) {
        let watch_request = request;
        request += 1;
        alice.send_control(
            FRAME_WORK,
            &rawclient::work_watch(
                watch_request,
                (0, 0, 1),
                STAGING_PENDING_AFTER_REVISION,
                STAGING_PENDING_DEADLINE_MS,
            ),
        )?;
        match alice.read_control_bounded(Duration::from_millis(400))? {
            Some(Frame::Control(FRAME_REFUSAL, body)) => {
                let refusal = rawclient::parse_refusal(&body)?;
                log.push_str(&format!(
                    "pending-fill\t{attempt}\talice\t0\trefused\trequest {watch_request} \
                     tag_kind={} tag_id={} code={} detail={:?}\n",
                    refusal.tag_kind, refusal.tag_id, refusal.code, refusal.detail
                ));
                pending_refusal = Some((attempt, watch_request, refusal));
                break;
            }
            Some(Frame::Control(FRAME_WORK, _)) => {
                pending_answered_early += 1;
                log.push_str(&format!(
                    "pending-fill\t{attempt}\talice\t0\tanswered\trequest {watch_request} was \
                     answered immediately and is not holding a pending slot\n"
                ));
            }
            Some(Frame::Control(kind, body)) => log.push_str(&format!(
                "pending-fill\t{attempt}\talice\t0\tframe\trequest {watch_request} kind={kind} \
                 len={}\n",
                body.len()
            )),
            Some(Frame::Fin) => {
                log.push_str("pending-fill\t-\talice\t0\tfin\tcontrol FIN\n");
                break;
            }
            None => {
                pending_accepted += 1;
                log.push_str(&format!(
                    "pending-fill\t{attempt}\talice\t0\tpending\trequest {watch_request} \
                     granted and unanswered (after_revision {STAGING_PENDING_AFTER_REVISION}, \
                     wait {STAGING_PENDING_DEADLINE_MS}ms)\n"
                ));
            }
        }
    }
    match &pending_refusal {
        Some((attempt, watch_request, refusal)) => {
            ensure!(
                refusal.code == rawclient::CODE_LIMIT_EXCEEDED,
                "{scenario_id} {}: pending control work was refused with code {} ({:?}) on \
                 attempt {attempt}, expected LIMIT_EXCEEDED ({})",
                server.name(),
                refusal.code,
                refusal.detail,
                rawclient::CODE_LIMIT_EXCEEDED
            );
            ensure!(
                refusal.tag_kind == 0 && refusal.tag_id == *watch_request,
                "{scenario_id} {}: the refusal names request tag ({}, {}) but it answered \
                 request {watch_request}; the row will not attribute a refusal to an attempt \
                 it does not name",
                server.name(),
                refusal.tag_kind,
                refusal.tag_id
            );
            observed.push((
                "pending_ceiling",
                format!(
                    "declared pending_limit {pending_target}; {pending_accepted} waits granted \
                     and left unanswered ({pending_answered_early} answered immediately and \
                     held no slot), and attempt {attempt} (request {watch_request}) was refused \
                     LIMIT_EXCEEDED (4) detail={:?}. The ceiling that fires is not necessarily \
                     the declared pending_limit and the row does not claim it is",
                    refusal.detail
                ),
            ));
            events.append("LIMIT_REFUSAL_OBSERVED", None, None, None, None, None)?;
        }
        None => observed.push((
            "pending_ceiling",
            format!(
                "NOT REACHED: {pending_accepted} waits were granted and left unanswered \
                 against a declared pending_limit of {pending_target} over \
                 {} attempts, with no refusal. Recorded as not reached, never as a pass; \
                 see artifacts/ceilings.tsv",
                pending_target + 4
            ),
        )),
    }
    fs::write(artifacts.join("ceilings.tsv"), &log)?;

    // EXISTING PROMISES: the queued watches must still answer. They are
    // waiting on a revision that only an admission can produce, so this
    // connection is left as it is and the SECOND connection of the same
    // owner — attached before the fill, see above — admits the work; the
    // first connection then drains its granted waits.
    let promise_payload = oracle::dataset(context.seed ^ 0x5a, STAGING_DECLARED_LEN);
    let mut promise_sha = [0u8; 32];
    promise_sha.copy_from_slice(&crate::decode_hex(&oracle::sha256_hex(&promise_payload))?);
    let promise_op = oracle::operation_id(context.seed, "staging-promise", 1);
    ladder_admit(
        &mut admitter,
        binding.generation,
        &promise_op,
        (0, 0, 1),
        &promise_payload,
        &promise_sha,
    )?;
    // The admitter has done its one job. It is closed immediately, because
    // both subjects enforce a per-owner CONNECTION ceiling of their own
    // (r-connection-ceiling: rust 4, java 8) and the staging phase below
    // needs those slots for staging connections, not for a connection that
    // is finished with.
    admitter.close_application(b"staging row: admission delivered")?;
    // Drain what the first connection had been promised. Watches waiting on
    // the declaration revision answer as soon as the admission moves the
    // work on, or at their own deadline; both are the promise being kept,
    // and the row records how many were.
    let mut answered = 0u64;
    let mut drained_other = 0u64;
    let drain_deadline = Instant::now() + STAGING_PENDING_DRAIN;
    while answered < pending_accepted && Instant::now() < drain_deadline {
        match alice.read_control_bounded(Duration::from_secs(5))? {
            Some(Frame::Control(FRAME_WORK, _)) => answered += 1,
            Some(Frame::Control(FRAME_REFUSAL, body)) => {
                let refusal = rawclient::parse_refusal(&body)?;
                log.push_str(&format!(
                    "pending-drain\t-\talice\t0\trefusal\tcode={} detail={:?}\n",
                    refusal.code, refusal.detail
                ));
                drained_other += 1;
            }
            Some(Frame::Control(kind, _)) => {
                log.push_str(&format!("pending-drain\t-\talice\t0\tframe\tkind={kind}\n"));
                drained_other += 1;
            }
            Some(Frame::Fin) => break,
            // Nothing yet: a granted wait may answer at the work's next
            // revision or at its own deadline, and the row waits for either
            // rather than concluding the promise was dropped.
            None => continue,
        }
    }
    log.push_str(&format!(
        "pending-drain\t-\talice\t0\tsummary\t{answered}/{pending_accepted} granted waits \
         answered, {drained_other} other frames\n"
    ));
    observed.push((
        "pending_existing_promises",
        format!(
            "{answered}/{pending_accepted} waits granted before exhaustion were answered \
             after the watched work was admitted on a second connection ({drained_other} \
             other control frames drained and logged)"
        ),
    ));
    ensure!(
        answered >= pending_accepted,
        "{scenario_id} {}: only {answered}/{pending_accepted} granted waits were answered \
         after exhaustion; existing promises were not kept",
        server.name()
    );
    fs::write(artifacts.join("ceilings.tsv"), &log)?;
    resources::sample_store(
        "pending-ceiling",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;

    // ---- Phase B: the staging-object ceiling ----
    // Incomplete input transfers are staging objects: the header reserves,
    // the payload never finishes, nothing commits. They are opened in
    // per-connection batches and the control stream is drained once per
    // batch, because a refusal carries the INPUT STREAM tag it belongs to
    // and can therefore be attributed exactly. Batching also matters for the
    // measurement: a per-attempt wait made the sweep outlast the subject's
    // own input receive deadline, which then reaped the earliest transfers
    // while the row was still opening later ones.
    let staging_payload = oracle::dataset(context.seed ^ 0x57, STAGING_DECLARED_LEN);
    let mut staging_sha = [0u8; 32];
    staging_sha.copy_from_slice(&crate::decode_hex(&oracle::sha256_hex(&staging_payload))?);
    let prefix = &staging_payload[..STAGING_PREFIX_LEN];
    let mut held: Vec<StagingConn> = Vec::new();
    let mut staging_refusal: Option<(u64, String, rawclient::Refusal)> = None;
    let mut staging_open_count = 0u64;
    let mut staging_reaped = 0u64;
    let mut last_accepted: Option<(usize, usize, u64, u64)> = None;
    // Entity ids must strictly increase within a scope across declaration
    // batches, so every later phase allocates from this cursor and never
    // re-uses a lower id.
    let mut entity_cursor = 100_000u64;
    'principals: for principal in ["alice", "bob", "carol", "dave"] {
        for connection_index in 0..STAGING_MAX_CONNECTIONS_PER_PRINCIPAL {
            if staging_open_count >= STAGING_MAX_STREAMS {
                log.push_str(&format!(
                    "staging\t{staging_open_count}\t{principal}\t{connection_index}\tcap\t\
                     hard cap {STAGING_MAX_STREAMS} streams reached without a refusal\n"
                ));
                break 'principals;
            }
            let mut conn = match peer.connect(&fixture.certs, principal, &owned.address) {
                Ok(conn) => conn,
                Err(error) => {
                    log.push_str(&format!(
                        "staging\t-\t{principal}\t{connection_index}\tconnect-refused\t{error:#}\n"
                    ));
                    continue 'principals;
                }
            };
            let batch: Vec<u64> = (0..caps.stream_limit)
                .map(|slot| entity_cursor + slot + 1)
                .collect();
            entity_cursor += caps.stream_limit + 1;
            // A connection that authenticates and is then turned away by a
            // per-owner ceiling fails on its first control read. That is
            // this row walking into ANOTHER subject bound, not a fixture
            // error: it is logged and the row moves to the next principal.
            let prepared = (|| -> Result<u64> {
                let offer = rawclient::frozen("capabilities-offer")?;
                conn.send_frozen(&offer.frame)?;
                let selection =
                    rawclient::parse_capabilities(&conn.expect_control(FRAME_CAPABILITIES)?)?;
                conn.set_caps(selection);
                // Per-principal session: alice attaches to the existing one,
                // a fresh principal creates its own.
                let generation = if principal == "alice" {
                    conn.send_control(
                        FRAME_SESSION,
                        &rawclient::session_attach(
                            1,
                            &binding.authority,
                            &binding.owner,
                            binding.generation,
                        ),
                    )?;
                    rawclient::parse_binding(&conn.expect_control(FRAME_SESSION)?)?.generation
                } else {
                    raw_create_session(&mut conn)?.generation
                };
                let declare = oracle::operation_id(
                    context.seed,
                    "staging-declare-b",
                    (batch[0] + principal.len() as u64) as u32,
                );
                raw_declare(&mut conn, 2, &declare, 0, &batch, false)?;
                Ok(generation)
            })();
            let generation = match prepared {
                Ok(generation) => generation,
                Err(error) => {
                    log.push_str(&format!(
                        "staging\t-\t{principal}\t{connection_index}\tsetup-refused\t{error:#}\n"
                    ));
                    continue 'principals;
                }
            };
            // Open this connection's whole batch, then read control once.
            let mut streams: Vec<quinn::SendStream> = Vec::new();
            let mut opened: Vec<(u64, u64, u64)> = Vec::new(); // attempt, entity, stream id
            let mut transport_failed: Option<String> = None;
            for entity in &batch {
                if staging_open_count >= STAGING_MAX_STREAMS {
                    break;
                }
                let operation = oracle::operation_id(context.seed, "staging-input", *entity as u32);
                match staging_open(
                    &mut conn,
                    generation,
                    &operation,
                    (0, 0, *entity),
                    &staging_sha,
                    prefix,
                    None,
                ) {
                    Ok(attempt) => {
                        staging_open_count += 1;
                        opened.push((staging_open_count, *entity, attempt.stream_id));
                        streams.push(attempt.stream);
                    }
                    Err(error) => {
                        // The subject stopped this transfer as it was being
                        // written. That is the subject's own enforcement,
                        // recorded rather than treated as a fixture error.
                        staging_reaped += 1;
                        transport_failed = Some(format!("{error:#}"));
                        break;
                    }
                }
            }
            if let Some(error) = &transport_failed {
                log.push_str(&format!(
                    "staging\t{staging_open_count}\t{principal}\t{connection_index}\treaped\t\
                     a staging transfer was stopped by the subject while it was being \
                     written: {error}\n"
                ));
            }
            // One bounded drain, matched to the attempts by input-stream tag.
            let mut refused_here: Vec<(u64, rawclient::Refusal)> = Vec::new();
            for _ in 0..(caps.stream_limit * 2 + 2) {
                match conn.read_control_bounded(STAGING_BATCH_DRAIN)? {
                    Some(Frame::Control(FRAME_REFUSAL, body)) => {
                        let refusal = rawclient::parse_refusal(&body)?;
                        let attempt = opened
                            .iter()
                            .find(|(_, _, stream_id)| {
                                refusal.tag_kind == rawclient::TAG_INPUT_STREAM
                                    && *stream_id == refusal.tag_id
                            })
                            .map(|(attempt, _, _)| *attempt);
                        log.push_str(&format!(
                            "staging\t{}\t{principal}\t{connection_index}\trefused\t\
                             tag_kind={} tag_id={} code={} detail={:?}\n",
                            attempt
                                .map(|a| a.to_string())
                                .unwrap_or_else(|| "unmatched".into()),
                            refusal.tag_kind,
                            refusal.tag_id,
                            refusal.code,
                            refusal.detail
                        ));
                        if let Some(attempt) = attempt {
                            refused_here.push((attempt, refusal));
                        }
                    }
                    Some(Frame::Control(kind, body)) => log.push_str(&format!(
                        "staging\t-\t{principal}\t{connection_index}\tframe\tkind={kind} len={}\n",
                        body.len()
                    )),
                    Some(Frame::Fin) => {
                        log.push_str(&format!(
                            "staging\t-\t{principal}\t{connection_index}\tfin\tcontrol FIN\n"
                        ));
                        break;
                    }
                    None => break,
                }
            }
            refused_here.sort_by_key(|(attempt, _)| *attempt);
            let refused_attempts: BTreeSet<u64> =
                refused_here.iter().map(|(attempt, _)| *attempt).collect();
            for (attempt, entity, stream_id) in &opened {
                if refused_attempts.contains(attempt) {
                    continue;
                }
                log.push_str(&format!(
                    "staging\t{attempt}\t{principal}\t{connection_index}\tstaged\tstream={stream_id} \
                     work=0:0:{entity} {STAGING_PREFIX_LEN}/{STAGING_DECLARED_LEN} bytes, no FIN\n"
                ));
                let slot = opened
                    .iter()
                    .position(|(a, _, _)| a == attempt)
                    .expect("the attempt came from this list");
                last_accepted = Some((held.len(), slot, *entity, *stream_id));
            }
            let first_refusal = refused_here.into_iter().next();
            held.push(StagingConn {
                principal: principal.to_owned(),
                conn,
                streams,
            });
            if let Some((attempt, refusal)) = first_refusal {
                staging_refusal = Some((attempt, principal.to_owned(), refusal));
                break 'principals;
            }
            if transport_failed.is_some() {
                break 'principals;
            }
        }
    }
    fs::write(artifacts.join("ceilings.tsv"), &log)?;
    let staging_held: u64 = held.iter().map(|c| c.streams.len() as u64).sum();
    observed.push((
        "staging_streams_opened",
        format!(
            "{staging_open_count} incomplete input transfers over {} connections \
             ({staging_held} still held at the end of the sweep, {staging_reaped} stopped by \
             the subject while being written); hard caps {STAGING_MAX_STREAMS} streams / \
             {STAGING_MAX_CONNECTIONS_PER_PRINCIPAL} connections per principal",
            held.len()
        ),
    ));
    let staging_partial = match &staging_refusal {
        Some((attempt, principal, refusal)) => {
            ensure!(
                refusal.code == rawclient::CODE_LIMIT_EXCEEDED,
                "{scenario_id} {}: staging exhaustion was refused with code {} ({:?}), \
                 expected LIMIT_EXCEEDED ({})",
                server.name(),
                refusal.code,
                refusal.detail,
                rawclient::CODE_LIMIT_EXCEEDED
            );
            observed.push((
                "staging_ceiling",
                format!(
                    "NEW work refused on staging attempt {attempt} (principal {principal}) with \
                     LIMIT_EXCEEDED (4), tag_kind={} tag_id={} detail={:?}",
                    refusal.tag_kind, refusal.tag_id, refusal.detail
                ),
            ));
            events.append("LIMIT_REFUSAL_OBSERVED", None, None, None, None, None)?;
            false
        }
        None => {
            observed.push((
                "staging_ceiling",
                format!(
                    "NOT REACHED: {staging_open_count} incomplete transfers were accepted \
                     without any count ceiling being refused, up to this row's hard caps \
                     ({STAGING_MAX_STREAMS} streams, {STAGING_MAX_CONNECTIONS_PER_PRINCIPAL} \
                     connections per principal), with {staging_reaped} transfer(s) stopped by \
                     the subject's own input receive deadline during the sweep. A cap is not a \
                     bound: recorded as not reached, never as a pass. What this shows about \
                     the subject is that it bounds incomplete staging transfers by TIME rather \
                     than by a count this row can reach"
                ),
            ));
            true
        }
    };
    resources::sample_store(
        "staging-full",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    let staging_full_ms = elapsed_ms(Instant::now());

    // ---- Phase C: capacity stays charged while the transfers are busy ----
    let recharge = if staging_refusal.is_some() {
        let entity = entity_cursor + 1;
        entity_cursor += 2;
        // The probe runs on the RESERVED connection, not on a held staging
        // connection: every staging connection has all `stream_limit` of its
        // object-stream slots occupied by transfers this row is holding, and
        // an open that waits for transport stream credit would time out and
        // be reported as the subject refusing capacity when it is the
        // fixture's own stream geometry. It must also be the SAME owner
        // whose capacity is exhausted, so a different principal is not an
        // option either.
        let declare = oracle::operation_id(context.seed, "staging-recharge-declare", 0);
        let declared = raw_declare(&mut prober, prober_request, &declare, 0, &[entity], false);
        match declared {
            Ok(()) => {
                let operation = oracle::operation_id(context.seed, "staging-recharge", 0);
                let attempt = staging_open(
                    &mut prober,
                    binding.generation,
                    &operation,
                    (0, 0, entity),
                    &staging_sha,
                    prefix,
                    Some(STAGING_PROBE_WAIT),
                )?;
                let text = match attempt.refusal {
                    Some(refusal) => format!(
                        "still charged: a further staging attempt while every transfer is held \
                         is refused again, code={} detail={:?}",
                        refusal.code, refusal.detail
                    ),
                    None => "NOT still charged: a further staging attempt was accepted while \
                             every earlier transfer is still held"
                        .to_owned(),
                };
                log.push_str(&format!("charged\t-\t-\t-\tprobe\t{text}\n"));
                drop(attempt.stream);
                text
            }
            Err(error) => {
                let text = format!(
                    "still charged: the recharge probe could not even declare its entity — \
                     {error:#}"
                );
                log.push_str(&format!("charged\t-\t-\t-\tprobe\t{text}\n"));
                text
            }
        }
    } else {
        "not applicable: the staging ceiling was never reached".to_owned()
    };
    observed.push(("capacity_charged_while_busy", recharge));

    // EXISTING PROMISES at the staging ceiling: a transfer accepted BEFORE
    // exhaustion still completes when it FINs.
    let promise_outcome = match last_accepted {
        Some((conn_index, slot, entity, stream_id)) => {
            // A transfer the subject already stopped is not a fixture error:
            // it is the subject's own deadline enforcement and is recorded
            // as such rather than propagated.
            let completion = (|| -> Result<String> {
                let target = held
                    .get_mut(conn_index)
                    .context("the accepted staging transfer's connection is gone")?;
                let stream = target
                    .streams
                    .get_mut(slot)
                    .context("the accepted staging transfer's stream is gone")?;
                target
                    .conn
                    .write_stream(stream, &staging_payload[STAGING_PREFIX_LEN..])?;
                target.conn.finish_stream(stream)?;
                Ok(
                    match target.conn.read_control_bounded(STAGING_REPLY_WAIT)? {
                        Some(Frame::Control(FRAME_WORK, body)) => {
                            let admitted = rawclient::parse_admitted_stream(&body)?;
                            format!(
                                "kept: the staging transfer accepted before exhaustion (work \
                             0:0:{entity}, stream {stream_id}) completed to an admission receipt \
                             naming stream {admitted} while NEW work stayed refused"
                            )
                        }
                        Some(Frame::Control(FRAME_REFUSAL, body)) => {
                            let refusal = rawclient::parse_refusal(&body)?;
                            format!(
                                "BROKEN: the staging transfer accepted before exhaustion was refused \
                             on completion, code={} detail={:?}",
                                refusal.code, refusal.detail
                            )
                        }
                        Some(Frame::Control(kind, _)) => {
                            format!("unexpected control frame kind={kind} on completion")
                        }
                        Some(Frame::Fin) => "control FIN before the completion receipt".to_owned(),
                        None => format!("no receipt within {STAGING_REPLY_WAIT:?}"),
                    },
                )
            })();
            match completion {
                Ok(text) => text,
                Err(error) => format!(
                    "not completable: the transfer (work 0:0:{entity}, stream {stream_id}) could \
                     not be finished — {error:#}. Recorded as the subject's own enforcement on \
                     an incomplete transfer, not as a promise broken at the ceiling"
                ),
            }
        }
        None => "not applicable: no staging transfer was accepted before exhaustion".to_owned(),
    };
    log.push_str(&format!(
        "existing\t-\t-\t-\tcompletion\t{promise_outcome}\n"
    ));
    observed.push(("staging_existing_promises", promise_outcome.clone()));
    if last_accepted.is_some() && staging_refusal.is_some() {
        ensure!(
            promise_outcome.starts_with("kept:"),
            "{scenario_id} {}: a staging transfer accepted before exhaustion did not complete \
             — {promise_outcome}",
            server.name()
        );
    }
    fs::write(artifacts.join("ceilings.tsv"), &log)?;

    // ---- Phase D: release, settle, and ask whether capacity came back ----
    let fd_peak_from_ms = elapsed_ms(Instant::now());
    for connection in held.drain(..) {
        let StagingConn { conn, streams, .. } = connection;
        drop(streams);
        conn.close_application(b"staging row: releasing held transfers")?;
    }
    prober.close_application(b"staging row: releasing the reserved prober")?;
    thread::sleep(STAGING_CLEANUP_SETTLE);
    let after_cleanup_ms = elapsed_ms(Instant::now());
    resources::sample_store(
        "after-cleanup",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    let recovery_after_cleanup = staging_recovery_probe(
        &peer,
        &fixture,
        &owned,
        &binding,
        &staging_sha,
        prefix,
        entity_cursor,
        context.seed,
        "after-cleanup",
        &mut log,
    )?;
    observed.push(("reconcile_after_cleanup", recovery_after_cleanup.clone()));

    // ---- Phase E: restart and ask again ----
    resources::sample_store(
        "before-restart",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    let before_restart: u64 = resources::read_store_samples(&store_file)?
        .iter()
        .filter(|record| record.checkpoint == "before-restart")
        .map(|record| record.len)
        .sum();
    // The collector is anchored on the old pid, so it stops with the old
    // process; the restarted subject is measured by a fresh collector.
    let summary_before = collector.stop()?;
    owned.stop()?;
    owned = fixture.start_server()?;
    let anchor_after = owned.pid()?;
    let collector_after = resources::ProcessCollector::start(
        anchor_after,
        resources::SAMPLE_INTERVAL,
        &scenario_dir.join("resources-after-restart.tsv"),
    )?;
    resources::sample_store(
        "after-restart",
        &[&fixture.state_db, &fixture.object_dir],
        &store_file,
    )?;
    let after_restart: u64 = resources::read_store_samples(&store_file)?
        .iter()
        .filter(|record| record.checkpoint == "after-restart")
        .map(|record| record.len)
        .sum();
    let recovery_after_restart = staging_recovery_probe(
        &peer,
        &fixture,
        &owned,
        &binding,
        &staging_sha,
        prefix,
        entity_cursor + 1_000,
        context.seed,
        "after-restart",
        &mut log,
    )?;
    observed.push(("reconcile_after_restart", recovery_after_restart.clone()));
    observed.push((
        "retained_bytes_across_restart",
        format!(
            "store file lengths total {before_restart} B before the restart and \
             {after_restart} B after it (separate scope from allocated blocks; both are in \
             store.tsv per file and per checkpoint)"
        ),
    ));
    let summary_after = collector_after.stop()?;
    fs::write(artifacts.join("ceilings.tsv"), &log)?;

    // Reconciliation is an ASSERTION, not just a record: capacity charged at
    // exhaustion must come back after safe cleanup and must not survive a
    // restart as a leak. Only meaningful once the ceiling was actually
    // reached — a capacity that was never exhausted has nothing to reconcile.
    if staging_refusal.is_some() {
        ensure!(
            recovery_after_cleanup.contains("accepted"),
            "{scenario_id} {}: staging capacity did not come back after the transfers were \
             released and a {:?} cleanup settle — {recovery_after_cleanup}",
            server.name(),
            STAGING_CLEANUP_SETTLE
        );
        ensure!(
            recovery_after_restart.contains("accepted"),
            "{scenario_id} {}: staging capacity did not reconcile across a restart on the \
             same roots — {recovery_after_restart}",
            server.name()
        );
    }

    // ---- measurement close-out ----
    ensure!(
        summary_before.error_lines == 0 && summary_after.error_lines == 0,
        "{scenario_id}: dead collector — {} + {} sampling error lines",
        summary_before.error_lines,
        summary_after.error_lines
    );
    observed.push((
        "collector",
        format!(
            "pre-restart {} ticks / {} lines / {} errors; post-restart {} ticks / {} lines / \
             {} errors",
            summary_before.ticks,
            summary_before.lines,
            summary_before.error_lines,
            summary_after.ticks,
            summary_after.lines,
            summary_after.error_lines
        ),
    ));
    let samples = resources::read_process_samples(&scenario_dir.join("resources.tsv"))?;
    ensure!(
        samples.iter().all(|sample| sample.rss_kb.is_some()
            && sample.hwm_kb.is_some()
            && sample.fds.is_some()
            && sample.threads.is_some()
            && sample.io_write_bytes.is_some()
            && sample.io_cancelled_write_bytes.is_some()),
        "{scenario_id}: a sample is missing a MANDATORY scope"
    );
    let baseline = resources::group_window_stats(&samples, 0, baseline_to_ms, "baseline")?;
    let loaded = resources::group_window_stats(
        &samples,
        staging_full_ms.saturating_sub(5_000),
        fd_peak_from_ms,
        "staging-held",
    )?;
    let released = resources::group_window_stats(
        &samples,
        after_cleanup_ms.saturating_sub(5_000),
        after_cleanup_ms,
        "after-cleanup",
    )?;
    observed.push((
        "fds",
        format!(
            "baseline median={} -> staging held p90={} (max {}) -> after release p90={} \
             (max {})",
            baseline.fd_median, loaded.fd_p90, loaded.fd_max, released.fd_p90, released.fd_max
        ),
    ));
    observed.push((
        "rss_kib",
        format!(
            "baseline median={} -> staging held p90={} -> after release p90={}",
            baseline.rss_median_kb, loaded.rss_p90_kb, released.rss_p90_kb
        ),
    ));
    observed.push((
        "threads",
        format!(
            "baseline median={} -> staging held max={} -> after release max={}",
            baseline.threads_median, loaded.threads_max, released.threads_max
        ),
    ));
    ensure!(
        released.fd_p90 <= baseline.fd_median + 8,
        "{scenario_id} {}: file handles did not come back after the staging transfers were \
         released (baseline median {} -> released p90 {}); handles are not bounded",
        server.name(),
        baseline.fd_median,
        released.fd_p90
    );

    // ---- journal / retained-byte ceilings: recorded, not driven ----
    let store_records = resources::read_store_samples(&store_file)?;
    let checkpoints: BTreeSet<&str> = store_records
        .iter()
        .map(|record| record.checkpoint.as_str())
        .collect();
    let journal_peak = store_records
        .iter()
        .filter(|record| record.path.ends_with("authority.sqlite"))
        .map(|record| record.len)
        .max()
        .unwrap_or(0);
    observed.push((
        "journal_ceiling_declared",
        format!(
            "session binding receipt: {}. Configured file ceilings that this row does NOT \
             drive to exhaustion: the java subject's bounded-SQLite policy (256 MiB database, \
             64 MiB WAL, 64 MiB rollback journal, 512 KiB shared memory) and its object store \
             policy (8 GiB bytes, 10,000 files, 128 handles); the rust authority's equivalent \
             record-completion capacity. Driving any of them needs hundreds of megabytes of \
             committed records, which is outside this row's bounded budget",
            session_limits.text()
        ),
    ));
    observed.push((
        "journal_observed_peak",
        format!(
            "largest authority.sqlite length observed across {} checkpoints: {journal_peak} B \
             (file length and allocated blocks are separate scopes, both per file in store.tsv)",
            checkpoints.len()
        ),
    ));
    observed.push((
        "store_checkpoints",
        format!(
            "{} records across {}",
            store_records.len(),
            checkpoints.into_iter().collect::<Vec<_>>().join(", ")
        ),
    ));
    let row_status = if staging_partial {
        "PARTIAL: the staging ceiling was not reached inside this row's hard caps, and the \
         journal/retained-BYTE ceilings are recorded rather than driven (see \
         journal_ceiling_declared). Named reasons, never skips"
    } else {
        "PARTIAL: the pending-response and staging-object ceilings are driven to exhaustion \
         and their refusals, promise-keeping, handle bounds and reconciliation are asserted, \
         but the journal/retained-BYTE ceilings are recorded rather than driven (see \
         journal_ceiling_declared). Named reason, never a skip"
    };
    observed.push(("row_status", row_status.to_owned()));

    write_kv(scenario_dir, "observed.tsv", &observed)?;
    events.append(
        "CEILING_EVIDENCE",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/ceilings.tsv".into(),
            len: log.len() as u64,
            sha256: oracle::sha256_hex(log.as_bytes()),
        }),
    )?;
    thread::sleep(STALL_CLOSE_SETTLE);
    stop_and_seal(context, scenario_dir, scenario_id, owned, events)
}

/// Ask whether staging capacity has come back: a fresh connection, a fresh
/// declaration and one staging transfer. Returns the outcome text.
#[allow(clippy::too_many_arguments)]
fn staging_recovery_probe(
    peer: &Peer,
    fixture: &AuthorityFixture,
    owned: &OwnedServer,
    binding: &rawclient::Binding,
    staging_sha: &[u8; 32],
    prefix: &[u8],
    entity: u64,
    seed: u64,
    label: &str,
    log: &mut String,
) -> Result<String> {
    let deadline = Instant::now() + STAGING_RECOVERY_WAIT;
    let mut last;
    let mut attempt_number = 0u64;
    loop {
        attempt_number += 1;
        let outcome = (|| -> Result<String> {
            let mut conn = peer.connect(&fixture.certs, "alice", &owned.address)?;
            let offer = rawclient::frozen("capabilities-offer")?;
            conn.send_frozen(&offer.frame)?;
            let selection =
                rawclient::parse_capabilities(&conn.expect_control(FRAME_CAPABILITIES)?)?;
            conn.set_caps(selection);
            conn.send_control(
                FRAME_SESSION,
                &rawclient::session_attach(
                    1,
                    &binding.authority,
                    &binding.owner,
                    binding.generation,
                ),
            )?;
            let attached = rawclient::parse_binding(&conn.expect_control(FRAME_SESSION)?)?;
            // Each attempt declares a NEW entity, so it must also carry a NEW
            // operation id: re-using one with different parameters is an
            // immutable-intent CONFLICT, which would mask whatever the
            // capacity answer actually is.
            let declare = oracle::operation_id(
                seed,
                "staging-recovery-declare",
                (entity + attempt_number) as u32,
            );
            raw_declare(&mut conn, 2, &declare, 0, &[entity + attempt_number], false)?;
            let operation =
                oracle::operation_id(seed, "staging-recovery", (entity + attempt_number) as u32);
            let probe = staging_open(
                &mut conn,
                attached.generation,
                &operation,
                (0, 0, entity + attempt_number),
                staging_sha,
                prefix,
                Some(STAGING_PROBE_WAIT),
            )?;
            let text = match probe.refusal {
                Some(refusal) => format!(
                    "refused on attempt {attempt_number}: code={} detail={:?}",
                    refusal.code, refusal.detail
                ),
                None => format!(
                    "accepted on attempt {attempt_number} (staging transfer {} opened)",
                    probe.stream_id
                ),
            };
            drop(probe.stream);
            conn.close_application(b"staging recovery probe complete")?;
            Ok(text)
        })();
        last = match outcome {
            Ok(text) => text,
            Err(error) => format!("attempt {attempt_number} failed: {error:#}"),
        };
        log.push_str(&format!(
            "recovery-{label}\t{attempt_number}\talice\t-\tprobe\t{last}\n"
        ));
        if last.starts_with("accepted") || Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_secs(5));
    }
    Ok(format!(
        "{label}: {last} (bounded wait {:?})",
        STAGING_RECOVERY_WAIT
    ))
}

// ---------------------------------------------------------------------------
// r-network-bytes (milestone 18d)
// ---------------------------------------------------------------------------
//
// Fixture-scoped network measurement, with the collection method recorded on
// every sample and never substituted mid-row. Two methods are used side by
// side and reported separately, never averaged or swapped:
//
// - `proc-net-dev`: the kernel's loopback interface counters. HOST-SCOPED on
//   this host, not fixture-scoped, because no network namespace is
//   available (the exact failing check is recorded). Loopback
//   double-counting: every datagram on `lo` is counted once in that
//   interface's RX and once in its TX, so an interface delta is twice the
//   wire bytes; the row reports both the raw delta and the halved figure and
//   never silently halves.
// - `quinn-conn-udp`: the source-pinned transport's own per-connection UDP
//   datagram byte totals. FIXTURE-SCOPED by construction — they belong to
//   one connection — and one-sided, being this endpoint's view.
//
// Handshake, TLS and retransmission bytes are measured in their own phase and
// recorded separately from logical payload bytes. Network bytes are never
// inferred from payload size; the row records the logical payload it sent as
// a separate number and compares, never derives.

/// Idle window used to quantify how much of the host-scoped loopback counter
/// is NOT this fixture.
const NET_BASELINE: Duration = Duration::from_secs(10);
/// Payload of the transfer phase. Large enough that framing, ACK and header
/// overhead is a small fraction and a retransmission would be visible.
const NET_PAYLOAD_LEN: usize = 4 * 1024 * 1024;

fn r_network_bytes(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(context, "r-network-bytes", r_network_bytes_direction)
}

/// Run one host-capability probe and record its exact result.
fn net_capability_probe(command: &[&str], transcript: &mut String) -> String {
    let output = Command::new(command[0])
        .args(&command[1..])
        .stdin(Stdio::null())
        .output();
    match output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            transcript.push_str(&format!(
                "=== {} (exit {}) ===\n{stdout}{stderr}\n",
                command.join(" "),
                output.status
            ));
            let text = format!("{stdout}{stderr}");
            let first = text
                .lines()
                .find(|line| !line.trim().is_empty())
                .unwrap_or("(no output)")
                .trim()
                .to_owned();
            format!("exit {} — {first}", output.status)
        }
        Err(error) => {
            transcript.push_str(&format!(
                "=== {} (not runnable) ===\n{error}\n",
                command.join(" ")
            ));
            format!("not runnable: {error}")
        }
    }
}

/// Sample the loopback interface into network.tsv and return the counters.
fn net_sample(
    file: &Path,
    checkpoint: &str,
    clock: Instant,
    anchor: u32,
) -> Result<resources::InterfaceCounters> {
    let counters = resources::interface_counters(anchor, "lo")?;
    resources::append_network_sample(
        file,
        &resources::NetworkSample {
            checkpoint: checkpoint.to_owned(),
            method: "proc-net-dev".to_owned(),
            interface: counters.interface.clone(),
            elapsed_ms: clock.elapsed().as_millis() as u64,
            rx_bytes: counters.rx_bytes,
            rx_packets: counters.rx_packets,
            tx_bytes: counters.tx_bytes,
            tx_packets: counters.tx_packets,
        },
    )?;
    Ok(counters)
}

/// Interface delta between two samples, as (rx bytes, tx bytes, rx packets,
/// tx packets).
fn net_delta(
    from: &resources::InterfaceCounters,
    to: &resources::InterfaceCounters,
) -> (u64, u64, u64, u64) {
    (
        to.rx_bytes.saturating_sub(from.rx_bytes),
        to.tx_bytes.saturating_sub(from.tx_bytes),
        to.rx_packets.saturating_sub(from.rx_packets),
        to.tx_packets.saturating_sub(from.tx_packets),
    )
}

/// One connection's UDP totals from the source-pinned transport.
fn quinn_udp_text(stats: &quinn::ConnectionStats) -> String {
    format!(
        "udp_tx={}B/{}dg udp_rx={}B/{}dg sent_packets={} lost_packets={} lost_bytes={} \
         congestion_events={} current_mtu={}",
        stats.udp_tx.bytes,
        stats.udp_tx.datagrams,
        stats.udp_rx.bytes,
        stats.udp_rx.datagrams,
        stats.path.sent_packets,
        stats.path.lost_packets,
        stats.path.lost_bytes,
        stats.path.congestion_events,
        stats.path.current_mtu
    )
}

fn r_network_bytes_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "r-network-bytes";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    // ---- host capability manifest for THIS scope, before any measurement ----
    // A method that is unavailable is recorded by the exact check that
    // failed, never by a note saying it was not attempted.
    let mut probes = String::new();
    let netns_plain = net_capability_probe(&["unshare", "-n", "true"], &mut probes);
    let netns_userns = net_capability_probe(&["unshare", "-r", "-n", "true"], &mut probes);
    let capture = net_capability_probe(
        &["tcpdump", "-i", "lo", "-c", "1", "-w", "/dev/null"],
        &mut probes,
    );
    fs::write(artifacts.join("capability-probes.txt"), &probes)?;
    let namespace_available = netns_plain.starts_with("exit exit status: 0")
        || netns_userns.starts_with("exit exit status: 0");
    let capture_available = capture.starts_with("exit exit status: 0");

    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let owned = fixture.start_server()?;
    ensure!(
        fixture.next_sequence(&owned, "alice")? == 1,
        "fresh authority must report NEXT_SEQUENCE 1"
    );
    let anchor = owned.pid()?;
    let permissions = resources::proc_permissions(anchor);
    ensure!(
        permissions.net_dev,
        "{scenario_id}: /proc/net/dev is unreadable, so the only remaining \
         network-byte method on this host is gone; the row fails rather than \
         recording zero bytes"
    );
    // The network scope lives under artifacts/ beside the capability probes
    // it depends on, so an event record can reference it by a relative label.
    let network_file = artifacts.join("network.tsv");
    let clock = Instant::now();

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "method_rule",
                "the collection method is recorded on EVERY sample line and is \
                 never substituted mid-row; two methods are reported side by \
                 side and never averaged or swapped"
                    .into(),
            ),
            (
                "method_proc_net_dev",
                "kernel loopback interface counters read through \
                 /proc/<subject pid>/net/dev, which reports that pid's network \
                 NAMESPACE. On this host that namespace is the host's, so the \
                 scope is HOST-WIDE, not fixture-wide; the row measures an idle \
                 baseline to quantify what is not this fixture"
                    .into(),
            ),
            (
                "method_quinn_conn_udp",
                "per-connection UDP datagram byte totals from the source-pinned \
                 transport (quinn 0.11.11 / quinn-proto 0.11.17, pinned in \
                 Cargo.lock). Fixture-scoped by construction because they belong \
                 to one connection; one-sided, being this endpoint's view"
                    .into(),
            ),
            (
                "loopback_double_counting",
                "every datagram on lo is counted once in that interface's RX and \
                 once in its TX, so an interface delta is TWICE the wire bytes. \
                 The row reports the raw delta and the halved figure side by \
                 side and never silently halves"
                    .into(),
            ),
            (
                "handshake_and_retransmit",
                "handshake, TLS and retry bytes are measured in their own phase, \
                 before any payload exists, and recorded separately from logical \
                 payload bytes. Retransmission is reported from the transport's \
                 own lost_packets/lost_bytes counters"
                    .into(),
            ),
            (
                "never_inferred",
                format!(
                    "logical payload bytes ({NET_PAYLOAD_LEN}) are recorded as \
                     their own number and COMPARED with measured transport bytes; \
                     no network figure in this row is derived from a payload size"
                ),
            ),
            (
                "dead_collector_rule",
                "the row proves its own detection: a deliberately truncated copy \
                 of the network artifact must be REJECTED by the validating \
                 reader, a record with no stated method must be rejected, and a \
                 counter read against a non-existent interface must fail rather \
                 than return zero"
                    .into(),
            ),
        ],
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        (
            "client_subject",
            "rust raw peer (one connection per phase)".into(),
        ),
        (
            "capability_network_namespace",
            format!(
                "{}: `unshare -n true` -> {netns_plain}; `unshare -r -n true` -> \
                 {netns_userns}",
                if namespace_available {
                    "AVAILABLE"
                } else {
                    "UNAVAILABLE (so no fixture-scoped interface counter exists on this host)"
                }
            ),
        ),
        (
            "capability_packet_capture",
            format!(
                "{}: `tcpdump -i lo -c 1 -w /dev/null` -> {capture}",
                if capture_available {
                    "AVAILABLE"
                } else {
                    "UNAVAILABLE (so packet-level byte accounting falls back to the \
                     source-pinned transport's own per-connection UDP counters)"
                }
            ),
        ),
        (
            "method_in_use",
            "proc-net-dev (host-scoped interface counters) AND quinn-conn-udp \
             (fixture-scoped per-connection transport counters), recorded per \
             sample, reported separately"
                .into(),
        ),
    ];

    // ---- Phase 1: host idle baseline ----
    let idle_from = net_sample(&network_file, "baseline-start", clock, anchor)?;
    thread::sleep(NET_BASELINE);
    let idle_to = net_sample(&network_file, "baseline-end", clock, anchor)?;
    let (idle_rx, idle_tx, idle_rxp, idle_txp) = net_delta(&idle_from, &idle_to);
    observed.push((
        "host_idle_baseline",
        format!(
            "over {}s with no fixture traffic the host's lo moved rx={idle_rx}B/{idle_rxp}pkt \
             tx={idle_tx}B/{idle_txp}pkt. That is the contamination floor of the host-scoped \
             method and is why every figure below is reported alongside the fixture-scoped one",
            NET_BASELINE.as_secs()
        ),
    ));

    // ---- Phase 2: handshake, TLS and session establishment only ----
    let peer = Peer::new()?;
    let handshake_from = net_sample(&network_file, "handshake-start", clock, anchor)?;
    let mut conn = raw_negotiate_as(&peer, &fixture, &owned, &mut events, &artifacts, "alice")?;
    let binding = raw_create_session(&mut conn)?;
    let handshake_stats = conn.stats();
    let handshake_to = net_sample(&network_file, "handshake-end", clock, anchor)?;
    let (hs_rx, hs_tx, hs_rxp, hs_txp) = net_delta(&handshake_from, &handshake_to);
    observed.push((
        "handshake_tls_bytes_interface",
        format!(
            "proc-net-dev, HOST-SCOPED: rx={hs_rx}B/{hs_rxp}pkt tx={hs_tx}B/{hs_txp}pkt over the \
             mTLS handshake, ALPN, capability negotiation and session creation, with no payload \
             in existence. Loopback double-counting: wire bytes are half the sum, \
             {}B",
            (hs_rx + hs_tx) / 2
        ),
    ));
    observed.push((
        "handshake_tls_bytes_transport",
        format!(
            "quinn-conn-udp, FIXTURE-SCOPED, same phase: {}",
            quinn_udp_text(&handshake_stats)
        ),
    ));
    events.append("NETWORK_HANDSHAKE_MEASURED", None, None, None, None, None)?;

    // ---- Phase 3: one known payload ----
    let payload = oracle::dataset(context.seed ^ 0x4e37, NET_PAYLOAD_LEN);
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&crate::decode_hex(&oracle::sha256_hex(&payload))?);
    let declare_op = oracle::operation_id(context.seed, "network-declare", 0);
    raw_declare(&mut conn, 2, &declare_op, 0, &[1], false)?;
    let transfer_from = net_sample(&network_file, "transfer-start", clock, anchor)?;
    let before = conn.stats();
    let operation = oracle::operation_id(context.seed, "network-admit", 1);
    ladder_admit(
        &mut conn,
        binding.generation,
        &operation,
        (0, 0, 1),
        &payload,
        &sha,
    )?;
    let after = conn.stats();
    let transfer_to = net_sample(&network_file, "transfer-end", clock, anchor)?;
    let (tx_rx, tx_tx, tx_rxp, tx_txp) = net_delta(&transfer_from, &transfer_to);
    let udp_tx_delta = after.udp_tx.bytes.saturating_sub(before.udp_tx.bytes);
    let udp_rx_delta = after.udp_rx.bytes.saturating_sub(before.udp_rx.bytes);
    let interface_wire = (tx_rx + tx_tx) / 2;
    observed.push((
        "logical_payload_bytes",
        format!(
            "{NET_PAYLOAD_LEN} bytes of application payload, plus its framed input header. This \
             is recorded as its own number and compared with the measured figures below; no \
             network figure in this row is derived from it"
        ),
    ));
    observed.push((
        "transfer_bytes_interface",
        format!(
            "proc-net-dev, HOST-SCOPED: rx={tx_rx}B/{tx_rxp}pkt tx={tx_tx}B/{tx_txp}pkt; wire \
             bytes after the loopback double-counting rule = {interface_wire}B against \
             {NET_PAYLOAD_LEN}B of logical payload"
        ),
    ));
    observed.push((
        "transfer_bytes_transport",
        format!(
            "quinn-conn-udp, FIXTURE-SCOPED: this connection sent {udp_tx_delta}B and received \
             {udp_rx_delta}B of UDP payload during the transfer, against {NET_PAYLOAD_LEN}B of \
             logical payload — overhead {}B ({}%)",
            udp_tx_delta.saturating_sub(NET_PAYLOAD_LEN as u64),
            udp_tx_delta
                .saturating_sub(NET_PAYLOAD_LEN as u64)
                .saturating_mul(100)
                / (NET_PAYLOAD_LEN as u64)
        ),
    ));
    observed.push((
        "retransmission",
        format!(
            "transport path counters over the whole connection: sent_packets={} \
             lost_packets={} lost_bytes={} congestion_events={} current_mtu={}. Retransmitted \
             bytes are inside the measured transport and interface totals and are reported here \
             rather than subtracted from them",
            after.path.sent_packets,
            after.path.lost_packets,
            after.path.lost_bytes,
            after.path.congestion_events,
            after.path.current_mtu
        ),
    ));
    events.append("NETWORK_TRANSFER_MEASURED", None, None, None, None, None)?;

    // Let the admitted work settle so the subject is not signalled to stop
    // mid-execution (fixture timing; no measurement is taken here).
    let mut request = 3u64;
    ladder_wait_settled(&mut conn, &mut request, 1, LADDER_QUIESCE)?;
    let closing = conn.stats();
    conn.close_and_wait_idle(b"network row complete", Duration::from_secs(15))?;
    let final_sample = net_sample(&network_file, "end", clock, anchor)?;
    let (all_rx, all_tx, _, _) = net_delta(&idle_from, &final_sample);
    observed.push((
        "whole_row_interface",
        format!(
            "proc-net-dev, HOST-SCOPED, first to last sample: rx={all_rx}B tx={all_tx}B (wire \
             {}B after halving) — includes the idle baseline and anything else on this host's \
             loopback, which is exactly the limitation the method carries here",
            (all_rx + all_tx) / 2
        ),
    ));
    observed.push((
        "whole_row_transport",
        format!(
            "quinn-conn-udp, FIXTURE-SCOPED, whole connection: {}",
            quinn_udp_text(&closing)
        ),
    ));

    // ---- Dead-collector / truncation proof, run in-row ----
    let samples = resources::read_network_samples(&network_file)?;
    ensure!(
        samples.len() >= 6,
        "{scenario_id}: expected at least six network samples, got {}",
        samples.len()
    );
    ensure!(
        samples.iter().all(|sample| !sample.method.is_empty()),
        "{scenario_id}: a network sample carries no collection method"
    );
    let truncated_path = artifacts.join("network-truncated-control.tsv");
    let text = fs::read_to_string(&network_file)?;
    fs::write(&truncated_path, &text[..text.len().saturating_sub(7)])?;
    let truncation_rejected = resources::read_network_samples(&truncated_path).is_err();
    let missing_interface = resources::interface_counters(anchor, "definitely-not-an-interface")
        .err()
        .map(|error| format!("{error:#}"))
        .unwrap_or_else(|| "NOT DETECTED".to_owned());
    ensure!(
        truncation_rejected,
        "{scenario_id}: the validating reader accepted a truncated network artifact; a dead \
         collector would read back as a smaller measurement"
    );
    ensure!(
        missing_interface != "NOT DETECTED",
        "{scenario_id}: reading a non-existent interface returned counters instead of failing"
    );
    observed.push((
        "dead_collector_control",
        format!(
            "PROVED in-row: a copy of the artifact truncated mid-record is REJECTED by the \
             validating reader ({} good samples read from the intact file), and a counter read \
             against a non-existent interface fails with: {missing_interface}",
            samples.len()
        ),
    ));
    events.append(
        "NETWORK_EVIDENCE",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/network.tsv".into(),
            len: text.len() as u64,
            sha256: oracle::sha256_hex(text.as_bytes()),
        }),
    )?;

    // ---- what this row does NOT establish ----
    observed.push((
        "row_status",
        if namespace_available || capture_available {
            "the host grants a fixture-scoped network method; see the capability fields".to_owned()
        } else {
            "PARTIAL: this host grants NEITHER a network namespace NOR packet capture (exact \
             failing checks recorded above and in artifacts/capability-probes.txt), so the \
             only fixture-SCOPED figures here are the source-pinned transport's own \
             per-connection UDP counters, and the interface counters are host-scoped with an \
             idle baseline quantifying the difference. Per-packet byte accounting of the \
             SUBJECT's side is not observable at all. Named reason, never a skip"
                .to_owned()
        },
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    thread::sleep(STALL_CLOSE_SETTLE);
    stop_and_seal(context, scenario_dir, scenario_id, owned, events)
}

// ---------------------------------------------------------------------------
// r-native-credit (milestone 18e)
// ---------------------------------------------------------------------------
//
// Separates three quantities the matrix insists are not the same thing:
//
// - BORROWED NATIVE FLOW CREDIT: the MAX_DATA, MAX_STREAM_DATA and
//   MAX_STREAMS frames the PEER actually put on the wire, counted by the
//   source-pinned transport as it decoded them out of received packets, plus
//   the DATA_BLOCKED / STREAM_DATA_BLOCKED frames this side put on the wire
//   when the application outran the credit it had been lent.
// - APPLICATION QUEUE BYTES: what the application handed to the transport.
//   Known exactly, because this row wrote them.
// - ACTUAL TRANSPORT COMPLETION: the peer acknowledging every byte of a
//   finished stream, observed through quinn's `stopped()` future, and the
//   UDP bytes the transport really sent.
//
// The counters come from `quinn::Connection::stats()` — quinn 0.11.11 /
// quinn-proto 0.11.17, both pinned in this workspace's Cargo.lock — which is
// the transport's own per-frame accounting, not a wrapper counting
// application calls. What it is NOT is a byte-for-byte packet capture: this
// host grants no CAP_NET_RAW (the exact failing check is recorded by
// r-network-bytes and repeated here), and it is one endpoint's view, so the
// SUBJECT's own credit accounting is not observable. Those two limits are
// why this row is PARTIAL.

/// Payload written in one application call to separate queueing from
/// completion. Equal to the declared object limit of both subjects, so it is
/// the largest single object either will accept.
const CREDIT_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
/// Bounded wait for actual transport completion of the finished stream.
const CREDIT_ACK_WAIT: Duration = Duration::from_secs(30);
/// Bounded wait used when asking whether completion has ALREADY happened.
const CREDIT_INSTANT: Duration = Duration::from_millis(50);
/// Bounded wait for the peer to return stream credit after refused streams.
const CREDIT_RELEASE_WAIT: Duration = Duration::from_secs(15);

fn r_native_credit(context: &ScenarioContext) -> Result<()> {
    run_raw_directions(context, "r-native-credit", r_native_credit_direction)
}

/// Frame-level credit accounting of one connection, as the source-pinned
/// transport counted it.
fn credit_frame_text(stats: &quinn::ConnectionStats) -> String {
    format!(
        "rx[MAX_DATA={} MAX_STREAM_DATA={} MAX_STREAMS_UNI={} MAX_STREAMS_BIDI={} \
         STOP_SENDING={} RESET_STREAM={} STREAM={} ACK={}] \
         tx[DATA_BLOCKED={} STREAM_DATA_BLOCKED={} STREAMS_BLOCKED_UNI={} STREAM={} ACK={}]",
        stats.frame_rx.max_data,
        stats.frame_rx.max_stream_data,
        stats.frame_rx.max_streams_uni,
        stats.frame_rx.max_streams_bidi,
        stats.frame_rx.stop_sending,
        stats.frame_rx.reset_stream,
        stats.frame_rx.stream,
        stats.frame_rx.acks,
        stats.frame_tx.data_blocked,
        stats.frame_tx.stream_data_blocked,
        stats.frame_tx.streams_blocked_uni,
        stats.frame_tx.stream,
        stats.frame_tx.acks
    )
}

/// One checkpoint row of credit.tsv.
#[allow(clippy::too_many_arguments)]
fn credit_row(checkpoint: &str, elapsed_ms: u64, stats: &quinn::ConnectionStats) -> String {
    format!(
        "{checkpoint}\t{elapsed_ms}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        stats.udp_tx.bytes,
        stats.udp_rx.bytes,
        stats.path.sent_packets,
        stats.path.lost_packets,
        stats.frame_rx.max_data,
        stats.frame_rx.max_stream_data,
        stats.frame_rx.max_streams_uni,
        stats.frame_rx.stop_sending,
        stats.frame_tx.data_blocked,
        stats.frame_tx.stream_data_blocked,
        stats.frame_tx.streams_blocked_uni,
        stats.frame_tx.stream
    )
}

fn r_native_credit_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
) -> Result<()> {
    let scenario_id = "r-native-credit";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, scenario_id, Subject::Rust)?;
    enforce_no_fault_schedule(context, scenario_id)?;

    // The capture capability is re-checked here rather than assumed from
    // r-network-bytes: a row states the method it used, on the run it used it.
    let mut probes = String::new();
    let capture = net_capability_probe(
        &["tcpdump", "-i", "lo", "-c", "1", "-w", "/dev/null"],
        &mut probes,
    );
    fs::write(artifacts.join("capability-probes.txt"), &probes)?;
    let capture_available = capture.starts_with("exit exit status: 0");

    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let owned = fixture.start_server()?;
    ensure!(
        fixture.next_sequence(&owned, "alice")? == 1,
        "fresh authority must report NEXT_SEQUENCE 1"
    );

    let peer = Peer::new()?;
    let mut conn = raw_negotiate_as(&peer, &fixture, &owned, &mut events, &artifacts, "alice")?;
    let caps = *conn
        .caps()
        .context("capabilities selection was not recorded during negotiation")?;
    let binding = raw_create_session(&mut conn)?;
    let clock = Instant::now();
    let mut table = String::from(
        "checkpoint\telapsed_ms\tudp_tx_bytes\tudp_rx_bytes\tsent_packets\tlost_packets\t\
         rx_max_data\trx_max_stream_data\trx_max_streams_uni\trx_stop_sending\ttx_data_blocked\t\
         tx_stream_data_blocked\ttx_streams_blocked_uni\ttx_stream\n",
    );
    let checkpoint = |label: &str, stats: &quinn::ConnectionStats, table: &mut String| {
        table.push_str(&credit_row(
            label,
            clock.elapsed().as_millis() as u64,
            stats,
        ));
    };
    checkpoint("session-established", &conn.stats(), &mut table);

    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "evidence_source",
                "quinn::Connection::stats() from the SOURCE-PINNED transport \
                 (quinn 0.11.11 / quinn-proto 0.11.17, pinned in Cargo.lock): \
                 the transport's own count of frames it decoded out of received \
                 packets and encoded into sent ones, plus its UDP byte totals \
                 and path loss counters. Not a wrapper counting application \
                 calls, and not the subject's accounting"
                    .into(),
            ),
            (
                "borrowed_native_flow_credit",
                "MAX_DATA, MAX_STREAM_DATA and MAX_STREAMS_UNI frames RECEIVED \
                 from the peer, and DATA_BLOCKED / STREAM_DATA_BLOCKED / \
                 STREAMS_BLOCKED_UNI frames SENT when the application outran the \
                 credit it had been lent"
                    .into(),
            ),
            (
                "application_queue_bytes",
                format!(
                    "{CREDIT_PAYLOAD_LEN} bytes handed to the transport in one \
                     application call; known exactly because this row wrote them"
                ),
            ),
            (
                "actual_transport_completion",
                "the peer acknowledging EVERY byte of the finished stream, \
                 observed through quinn's stopped() future resolving to \
                 acknowledged; a write call returning is not completion and this \
                 row measures the gap between them"
                    .into(),
            ),
            (
                "credit_release",
                format!(
                    "after {} refused object streams are retired, MAX_STREAMS_UNI \
                     must arrive from the peer and a further stream must actually \
                     open — pairing with the g6-stopped-* observation that refused \
                     inputs return stream credit",
                    caps.stream_limit
                ),
            ),
            (
                "not_established",
                "byte-for-byte packet capture (no CAP_NET_RAW on this host; the \
                 exact failing check is run by this row and archived) and the \
                 SUBJECT's own credit accounting (not observable from one \
                 endpoint). Both are named, neither is inferred"
                    .into(),
            ),
        ],
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        (
            "client_subject",
            "rust raw peer (source-pinned quinn transport)".into(),
        ),
        (
            "declared_limits",
            format!(
                "object_limit={} stream_limit={} pending_limit={} control_limit={}",
                caps.object_limit, caps.stream_limit, caps.pending_limit, caps.control_limit
            ),
        ),
        (
            "capability_packet_capture",
            format!(
                "{}: `tcpdump -i lo -c 1 -w /dev/null` -> {capture}",
                if capture_available {
                    "AVAILABLE"
                } else {
                    "UNAVAILABLE"
                }
            ),
        ),
    ];

    // ---- Phase A: application queue bytes vs actual transport completion ----
    let declare_op = oracle::operation_id(context.seed, "credit-declare", 0);
    raw_declare(
        &mut conn,
        2,
        &declare_op,
        0,
        &[1, 2, 3, 4, 5, 6, 7, 8],
        false,
    )?;
    let payload = oracle::dataset(context.seed ^ 0xc2ed, CREDIT_PAYLOAD_LEN);
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&crate::decode_hex(&oracle::sha256_hex(&payload))?);
    let operation = oracle::operation_id(context.seed, "credit-admit", 1);
    let header = rawclient::input_header_framed(
        binding.generation,
        &operation,
        (0, 0, 1),
        CREDIT_PAYLOAD_LEN as u64,
        &sha,
        "application/octet-stream",
        "copy/v2",
        0,
        // The session policy caps execution at 60 s; a larger value is a
        // LIMIT_EXCEEDED refusal of the admission, not a longer deadline.
        60_000,
        1,
        CREDIT_PAYLOAD_LEN as u64,
    );
    let mut stream = conn.open_uni()?;
    let stream_id = u64::from(stream.id());
    conn.write_stream(&mut stream, &header)?;
    checkpoint("header-written", &conn.stats(), &mut table);
    let write_started = Instant::now();
    conn.write_stream(&mut stream, &payload)?;
    let write_returned = write_started.elapsed();
    let after_write = conn.stats();
    checkpoint("payload-write-returned", &after_write, &mut table);
    // The application has now handed over every byte. Ask, at this instant,
    // whether the transport has completed: a write returning is a queueing
    // event, not a completion event.
    let at_write_return = conn.poll_stream_stopped(&stream, CREDIT_INSTANT)?;
    conn.finish_stream(&mut stream)?;
    let completion_started = Instant::now();
    let completion = conn.poll_stream_stopped(&stream, CREDIT_ACK_WAIT)?;
    let completion_took = completion_started.elapsed();
    let after_completion = conn.stats();
    checkpoint("transport-completion", &after_completion, &mut table);
    observed.push((
        "queue_vs_completion",
        format!(
            "the application handed {CREDIT_PAYLOAD_LEN} bytes to stream {stream_id} in one \
             call, which returned after {write_returned:?}. At that instant the transport \
             reported the stream as {at_write_return:?} — a write returning is a QUEUEING \
             event. Actual transport completion (the peer acknowledging every byte) was \
             {completion:?} and took a further {completion_took:?} after the FIN"
        ),
    ));
    observed.push((
        "transport_bytes_at_completion",
        format!(
            "udp_tx={}B/{}dg udp_rx={}B/{}dg sent_packets={} lost_packets={} lost_bytes={} \
             against {CREDIT_PAYLOAD_LEN}B of application queue bytes",
            after_completion.udp_tx.bytes,
            after_completion.udp_tx.datagrams,
            after_completion.udp_rx.bytes,
            after_completion.udp_rx.datagrams,
            after_completion.path.sent_packets,
            after_completion.path.lost_packets,
            after_completion.path.lost_bytes
        ),
    ));
    observed.push((
        "borrowed_credit_during_transfer",
        format!(
            "frames on the wire, counted by the source-pinned transport: {}",
            credit_frame_text(&after_completion)
        ),
    ));
    let receipt = rawclient::parse_admitted_stream(&conn.expect_control(FRAME_WORK)?)?;
    ensure!(
        receipt == stream_id,
        "{scenario_id} {}: admission receipt names stream {receipt}, expected {stream_id}",
        server.name()
    );
    checkpoint("admission-receipt", &conn.stats(), &mut table);
    events.append("CREDIT_TRANSFER_MEASURED", None, None, None, None, None)?;

    // ---- Phase B: credit release after refused streams ----
    // Every object stream slot is filled with a stream the subject must
    // refuse (a declared length above the negotiated object limit), then the
    // streams are retired and the peer's MAX_STREAMS_UNI is watched for the
    // returned credit. This is the same path g6-stopped-control-and-transfers
    // observes for MAX_STREAMS after refused inputs.
    let before_release = conn.stats();
    checkpoint("before-refusals", &before_release, &mut table);
    let oversize = caps.object_limit.saturating_mul(4).max(1);
    let mut refused_streams: Vec<quinn::SendStream> = Vec::new();
    let mut refusals = 0u64;
    let mut transport_stopped = 0u64;
    for slot in 0..caps.stream_limit {
        let entity = 2 + slot;
        let operation = oracle::operation_id(context.seed, "credit-oversize", entity as u32);
        let header = rawclient::input_header_framed(
            binding.generation,
            &operation,
            (0, 0, entity),
            oversize,
            &[0u8; 32],
            "application/octet-stream",
            "copy/v2",
            0,
            60_000,
            1,
            oversize,
        );
        let Some(mut oversize_stream) = conn.open_uni_ceiling()? else {
            break;
        };
        // A subject that refuses on the header can STOP_SENDING before this
        // write returns. That is the refusal arriving through the transport
        // channel rather than the control channel, and it is counted as
        // such, not treated as a fixture error.
        if conn.write_stream(&mut oversize_stream, &header).is_err() {
            transport_stopped += 1;
        }
        refused_streams.push(oversize_stream);
    }
    let opened_for_refusal = refused_streams.len() as u64;
    for _ in 0..opened_for_refusal {
        match conn.read_control_bounded(Duration::from_secs(5))? {
            Some(Frame::Control(FRAME_REFUSAL, body)) => {
                let refusal = rawclient::parse_refusal(&body)?;
                ensure!(
                    refusal.code == rawclient::CODE_LIMIT_EXCEEDED,
                    "{scenario_id} {}: an oversize object stream was refused with code {} \
                     ({:?}), expected LIMIT_EXCEEDED",
                    server.name(),
                    refusal.code,
                    refusal.detail
                );
                refusals += 1;
            }
            Some(Frame::Control(kind, _)) => bail!(
                "{scenario_id} {}: expected a refusal for an oversize object stream, got \
                 control frame kind {kind}",
                server.name()
            ),
            // Nothing more on control: whatever is left was refused through
            // the transport channel instead, which the counts record.
            Some(Frame::Fin) | None => break,
        }
    }
    checkpoint("refusals-read", &conn.stats(), &mut table);
    // Retire the refused streams and watch for the returned stream credit.
    for mut refused in refused_streams.drain(..) {
        let _ = conn.reset_stream(&mut refused, rawclient::CODE_LIMIT_EXCEEDED);
    }
    // Drain whatever the refusal phase still has in flight before the
    // control stream is used for anything else. A stream the peer stopped
    // mid-header can also produce a control refusal for the partial header
    // it did read, so a stream may be refused on BOTH channels and the
    // counts below are not disjoint.
    let mut drained_late = 0u64;
    let drain_deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < drain_deadline {
        match conn.read_control_bounded(Duration::from_millis(300))? {
            Some(Frame::Control(FRAME_REFUSAL, body)) => {
                let refusal = rawclient::parse_refusal(&body)?;
                drained_late += 1;
                refusals += u64::from(refusal.code == rawclient::CODE_LIMIT_EXCEEDED);
            }
            Some(Frame::Control(..)) => drained_late += 1,
            Some(Frame::Fin) => break,
            None => continue,
        }
    }
    let release_deadline = Instant::now() + CREDIT_RELEASE_WAIT;
    let mut released = conn.stats();
    while released.frame_rx.max_streams_uni <= before_release.frame_rx.max_streams_uni
        && Instant::now() < release_deadline
    {
        thread::sleep(Duration::from_millis(200));
        released = conn.stats();
    }
    checkpoint("credit-released", &released, &mut table);
    let max_streams_gain = released
        .frame_rx
        .max_streams_uni
        .saturating_sub(before_release.frame_rx.max_streams_uni);
    // Credit that is reported but not usable is not credit: open one more.
    let reopened = conn.open_uni_ceiling()?.is_some();
    checkpoint("credit-reused", &conn.stats(), &mut table);
    observed.push((
        "credit_release_after_refusals",
        format!(
            "{opened_for_refusal} object streams opened against a negotiated stream_limit of \
             {}: {refusals} refused LIMIT_EXCEEDED on the control channel and \
             {transport_stopped} stopped by the peer on the transport channel before the \
             header write returned ({drained_late} further control frames drained \
             afterwards; the two counts are not disjoint), for an oversize declared length, \
             then retired. \
             MAX_STREAMS_UNI frames received from the peer rose by {max_streams_gain} (from \
             {} to {}) within {:?}, and a further object stream {} open afterwards",
            caps.stream_limit,
            before_release.frame_rx.max_streams_uni,
            released.frame_rx.max_streams_uni,
            CREDIT_RELEASE_WAIT,
            if reopened { "DID" } else { "did NOT" }
        ),
    ));
    events.append("CREDIT_RELEASE_OBSERVED", None, None, None, None, None)?;

    let final_stats = conn.stats();
    checkpoint("end", &final_stats, &mut table);
    fs::write(artifacts.join("credit.tsv"), &table)?;
    observed.push(("frame_totals_at_end", credit_frame_text(&final_stats)));

    // ---- assertions ----
    ensure!(
        completion == rawclient::StreamState::Acknowledged,
        "{scenario_id} {}: the finished stream never reached actual transport completion \
         within {CREDIT_ACK_WAIT:?}; observed {completion:?}",
        server.name()
    );
    ensure!(
        final_stats.frame_rx.max_data > 0 || final_stats.frame_rx.max_stream_data > 0,
        "{scenario_id} {}: no MAX_DATA or MAX_STREAM_DATA frame was ever received, so no \
         borrowed flow credit was observed on the wire at all",
        server.name()
    );
    ensure!(
        refusals + transport_stopped >= opened_for_refusal && opened_for_refusal > 0,
        "{scenario_id} {}: {refusals} refused on control and {transport_stopped} stopped on \
         the transport, of {opened_for_refusal} oversize object streams opened; the \
         credit-release phase needs every one of them refused",
        server.name()
    );
    ensure!(
        max_streams_gain > 0 && reopened,
        "{scenario_id} {}: stream credit was not returned after refused object streams were \
         retired (MAX_STREAMS_UNI gain {max_streams_gain}, further stream opened: {reopened})",
        server.name()
    );
    observed.push((
        "row_status",
        if capture_available {
            "packet capture is available on this host; see the capability field".to_owned()
        } else {
            "PARTIAL: the credit evidence is the SOURCE-PINNED transport's own per-frame and \
             per-datagram accounting, which is what it decoded from received packets and \
             encoded into sent ones — not a wrapper counter, but also not a byte-for-byte \
             packet capture, because this host grants no CAP_NET_RAW (exact failing check \
             recorded above). It is also one endpoint's view: the SUBJECT's own credit \
             accounting is not observable from here. Both limits are named, neither is \
             inferred"
                .to_owned()
        },
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    events.append(
        "CREDIT_EVIDENCE",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/credit.tsv".into(),
            len: table.len() as u64,
            sha256: oracle::sha256_hex(table.as_bytes()),
        }),
    )?;
    // The control stream is deliberately NOT used again: the refusal phase
    // can leave a late FRAME_ERROR for a header the peer stopped mid-write,
    // and a row that then reads control for its own housekeeping would fail
    // on the subject's own refusal arriving on time. The single admitted
    // work is one object copy; the close-and-settle below is more than
    // enough for the execution pool to wind it down before SIGTERM.
    conn.close_and_wait_idle(b"native credit row complete", Duration::from_secs(15))?;
    thread::sleep(STALL_CLOSE_SETTLE);
    stop_and_seal(context, scenario_dir, scenario_id, owned, events)
}
// ---------------------------------------------------------------------------
// Milestone 19 rows (work in Kimi's role): the ten rows the matrix still
// listed as unimplemented. Seven run against both subjects; three run what
// they can and report the subject capability they are missing.
// ---------------------------------------------------------------------------

/// A row that cannot produce its evidence because a subject lacks a
/// capability the matrix needs (a hook boundary, a fixture clock). The row
/// has still run what it can, written expected.tsv/observed.tsv with
/// `row_status INCOMPLETE` and the reason, and sealed its events; the run
/// reports the named reason instead of "not implemented yet". In acceptance
/// mode this is a FAIL, because a certification cannot be claimed around a
/// capability nobody has.
#[derive(Debug)]
pub struct MissingCapability(pub String);

impl std::fmt::Display for MissingCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "missing subject capability: {}", self.0)
    }
}

impl std::error::Error for MissingCapability {}

/// Subject-side (server) records for one boundary naming one work key.
/// Claude's Java FixtureMain records every reached boundary with its work
/// key; the Rust subject emits only armed boundaries and never a work key,
/// so on a Rust server this is zero and the row says "not observable".
fn subject_records_for_work(events: &Path, boundary: &str, work: &str) -> Result<u64> {
    let text = match fs::read_to_string(events) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    Ok(text
        .lines()
        .filter(|line| {
            let columns: Vec<&str> = line.split('\t').collect();
            columns.len() > 9
                && columns[4] == "server"
                && columns[7] == boundary
                && columns[9] == work
        })
        .count() as u64)
}

/// Negotiate a raw connection as `principal` and attach it to the session
/// the CLI client bound (authority, owner, generation). Returns the
/// connection and the attach binding; request ids 1 (negotiation carries
/// none) and the attach are consumed, so callers start their own at 2.
#[allow(clippy::too_many_arguments)]
fn raw_attach(
    peer: &Peer,
    fixture: &AuthorityFixture,
    server: &OwnedServer,
    events: &mut EventWriter,
    artifacts: &Path,
    principal: &str,
    owner: &str,
    generation: u64,
) -> Result<(RawConn, rawclient::Binding)> {
    let mut conn = raw_negotiate_as(peer, fixture, server, events, artifacts, principal)?;
    conn.send_control(
        FRAME_SESSION,
        &rawclient::session_attach(1, fixture.authority(), owner, generation),
    )?;
    let binding = rawclient::parse_binding(&conn.expect_control(FRAME_SESSION)?)?;
    ensure!(
        binding.generation == generation && binding.owner == owner,
        "raw attach bound generation {} owner {}, expected {generation} {owner}",
        binding.generation,
        binding.owner
    );
    Ok((conn, binding))
}

/// Declared length of the in-flight admission of g2-not-found-in-flight and
/// the prefix that is written before the lookup; the rest plus FIN follows
/// only after the NOT_FOUND has been observed.
const PENDING_INPUT_LEN: usize = 256 * 1024;
const PENDING_PREFIX_LEN: usize = 128 * 1024;
/// Fixture wait between writing the header+prefix and the lookup, so the
/// subject has certainly read the header off the stream. Fixture timing,
/// never evidence: the evidence is the refusal and the later receipt on the
/// same stream.
const PENDING_SETTLE: Duration = Duration::from_millis(500);

/// g2-not-found-in-flight: an operation lookup answers NOT_FOUND (5) while an
/// admission under that id is genuinely pending before its commit, and the
/// admission then commits exactly once with receipts identical on every path.
///
/// The pending admission is held by a raw peer that has written the input
/// header and half the payload on an open object stream without FIN; a
/// second raw connection of the same principal, attached to the same
/// session, sends the wire lookup (Work::Operation) into that window. The
/// Rust subject never reaches INPUT_INSTALLED and its hooks pause only at the
/// reply pairs, so a stream held open mid-transfer is the one construction
/// that is deterministic on BOTH subjects; the Java INPUT_INSTALLED pause
/// would hold only the Java server. After the FIN the raw holder's
/// Work::Admitted receipt and the prober's Work::OperationResponse receipt
/// must be byte-identical; the CLI client then replays the same admission
/// (same id, same bytes, same parameters) and gets the durable receipt back,
/// looks the operation up twice with identical output, and reads the result
/// byte-exact; the scope page shows one member.
fn g2_not_found_in_flight(context: &ScenarioContext) -> Result<()> {
    let id = "g2-not-found-in-flight";
    g2_not_found_in_flight_direction(
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
            g2_not_found_in_flight_direction(context, direction_dir, Subject::Java, Subject::Rust)
        },
    )?;
    run_hooked_direction(
        context,
        id,
        "java-client-rust-server",
        |context, direction_dir| {
            g2_not_found_in_flight_direction(context, direction_dir, Subject::Rust, Subject::Java)
        },
    )?;
    Ok(())
}

fn g2_not_found_in_flight_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g2-not-found-in-flight";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    // Armed with no schedule: nothing pauses or dies; the Java subject
    // records every boundary it reaches (one EXECUTION_CLAIMED per work is
    // the "exactly one effect" evidence there), the Rust subject records
    // nothing unarmed.
    let hooked = setup_recording(context, scenario_dir, id, server, client)?;
    let (session, events_path) = split_hooked(hooked);
    let binding_out = session.op(&["binding"])?;
    require(&binding_out, "BINDING", "client binding")?;
    let declare = declare_sealed(&session, &mut events, context.seed, "declare", &[1])?;
    let admit_hex = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let admit_id = oracle::operation_id(context.seed, "admit", 1);
    let input = oracle::dataset(context.seed, PENDING_INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let sha: [u8; 32] = Sha256::digest(&input).into();
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            ("prefix_len_before_lookup", PENDING_PREFIX_LEN.to_string()),
            (
                "pending_construction",
                "raw peer writes the input header and the prefix on an open object stream \
                 without FIN; a second raw connection of the same principal attached to the \
                 same session sends Work::Operation for the id"
                    .into(),
            ),
            (
                "lookup_while_pending",
                "named NOT_FOUND (5) on the lookup's control request tag; never a receipt, \
                 never a fabricated outcome"
                    .into(),
            ),
            (
                "after_fin",
                "Work::Admitted receipt to the holder and Work::OperationResponse receipt to \
                 the prober byte-identical; attempt 1"
                    .into(),
            ),
            (
                "cli_replay",
                "the CLI client replays the same admission (same id, bytes, parameters) and \
                 receives the durable receipt (attempt 1), no second effect"
                    .into(),
            ),
            (
                "exactly_one_effect",
                "scope page members=1; terminal SUCCEEDED under attempt 1; result byte-exact; \
                 EXECUTION_CLAIMED recorded once for the work where the subject records it"
                    .into(),
            ),
        ],
    )?;
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

    let peer = Peer::new()?;
    let (mut holder, binding) = raw_attach(
        &peer,
        &session.fixture,
        &session.server,
        &mut events,
        &artifacts,
        "alice",
        "alice",
        1,
    )?;
    let (mut prober, _) = raw_attach(
        &peer,
        &session.fixture,
        &session.server,
        &mut events,
        &artifacts,
        "alice",
        "alice",
        1,
    )?;

    // Pending: header plus prefix, no FIN. The admission cannot commit
    // before the FIN because the subject must verify the whole payload
    // against the header's length and digest first.
    let header = rawclient::input_header_framed(
        binding.generation,
        &admit_id,
        (0, 0, 1),
        input.len() as u64,
        &sha,
        "application/octet-stream",
        "copy/v2",
        0,
        60_000,
        1,
        input.len() as u64,
    );
    let mut stream = holder.open_uni()?;
    holder.write_stream(&mut stream, &header)?;
    holder.write_stream(&mut stream, &input[..PENDING_PREFIX_LEN])?;
    events.append(
        "REQUEST_SENT",
        Some(admit_id),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    thread::sleep(PENDING_SETTLE);

    // Lookup while pending: NOT_FOUND on the lookup's own request tag.
    prober.send_control(FRAME_WORK, &rawclient::operation_lookup(2, &admit_id))?;
    let pending_lookup = match prober.read_control()? {
        Frame::Control(FRAME_REFUSAL, body) => rawclient::parse_refusal(&body)?,
        Frame::Control(FRAME_WORK, body) => {
            let (kind, receipt) = rawclient::work_receipt_bytes(&body)?;
            bail!(
                "{id}: lookup answered a receipt (kind {kind}, {}) while the input stream \
                 was still open without FIN: the admission committed before its payload was \
                 complete, which is a candidate subject defect on the {} server",
                hex(&receipt),
                server.name()
            );
        }
        Frame::Control(other, body) => bail!(
            "{id}: lookup while pending answered frame type {other} (body {})",
            hex(&body)
        ),
        Frame::Fin => bail!("{id}: control stream finished during the pending lookup"),
    };
    fs::write(
        artifacts.join("lookup-while-pending.txt"),
        format!(
            "tag_kind={} tag_id={} code={} ({}) detail={}\n",
            pending_lookup.tag_kind,
            pending_lookup.tag_id,
            pending_lookup.code,
            refusal_code_name(pending_lookup.code),
            pending_lookup.detail
        ),
    )?;
    ensure!(
        pending_lookup.code == 5 && pending_lookup.tag_kind == 0 && pending_lookup.tag_id == 2,
        "{id}: lookup while pending must refuse NOT_FOUND (5) on control request 2, got \
         code {} ({}) tag {}:{} detail {:?}",
        pending_lookup.code,
        refusal_code_name(pending_lookup.code),
        pending_lookup.tag_kind,
        pending_lookup.tag_id,
        pending_lookup.detail
    );
    events.append(
        "REFUSAL_RECEIVED",
        Some(admit_id),
        Some("0:0:1"),
        Some(1),
        Some(5),
        None,
    )?;

    // Complete the transfer: the holder gets the admission receipt on the
    // stream's input tag.
    holder.write_stream(&mut stream, &input[PENDING_PREFIX_LEN..])?;
    holder.finish_stream(&mut stream)?;
    let admitted_body = holder.expect_control(FRAME_WORK)?;
    let (admitted_kind, holder_receipt) = rawclient::work_receipt_bytes(&admitted_body)?;
    ensure!(
        admitted_kind == 1,
        "{id}: the holder expected Work::Admitted (kind 1), got kind {admitted_kind}"
    );
    let (attempt, admitted_at, deadline) = rawclient::parse_receipt_admitted(&holder_receipt)?;
    ensure!(
        attempt == 1,
        "{id}: the admission receipt must allocate attempt 1, got {attempt}"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(admit_id),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    // Lookup after the commit: the identical receipt bytes.
    prober.send_control(FRAME_WORK, &rawclient::operation_lookup(3, &admit_id))?;
    let response = prober.expect_control(FRAME_WORK)?;
    let (response_kind, prober_receipt) = rawclient::work_receipt_bytes(&response)?;
    ensure!(
        response_kind == 3,
        "{id}: the prober expected Work::OperationResponse (kind 3), got kind {response_kind}"
    );
    fs::write(
        artifacts.join("receipts.txt"),
        format!(
            "holder_admitted_receipt={}\nprober_lookup_receipt={}\n",
            hex(&holder_receipt),
            hex(&prober_receipt)
        ),
    )?;
    ensure!(
        holder_receipt == prober_receipt,
        "{id}: the lookup receipt differs from the admission receipt (see artifacts/receipts.txt)"
    );
    raw_detach(&mut holder, 4)?;
    raw_detach(&mut prober, 4)?;
    drop(holder);
    drop(prober);

    // The CLI client replays the same admission and gets the durable receipt.
    events.append(
        "REQUEST_SENT",
        Some(admit_id),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let replay = session.op(&[
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
    ])?;
    let replay_receipt = require(&replay, "RECEIPT", "CLI replay of the raw admission")?;
    fs::write(artifacts.join("cli-replay-receipt.txt"), &replay_receipt)?;
    let replay_deadline = parse_field_u64(&replay_receipt, "deadline")?
        .context("CLI replay receipt did not render a deadline")?;
    ensure!(
        replay_deadline == deadline,
        "{id}: the CLI replay receipt deadline {replay_deadline} differs from the raw \
         admission receipt deadline {deadline}"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(admit_id),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let lookup_one = session.op(&["lookup", "--operation", &admit_hex])?;
    let lookup_one = require(&lookup_one, "RECEIPT", "CLI lookup after commit")?;
    let lookup_two = session.op(&["lookup", "--operation", &admit_hex])?;
    let lookup_two = require(&lookup_two, "RECEIPT", "second CLI lookup after commit")?;
    fs::write(artifacts.join("cli-lookup-receipt.txt"), &lookup_one)?;
    ensure!(
        lookup_one.trim() == lookup_two.trim(),
        "{id}: two CLI lookups of the committed operation differ:\n{lookup_one}\n{lookup_two}"
    );
    ensure!(
        lookup_one.trim() == replay_receipt.trim(),
        "{id}: the CLI lookup receipt differs from the CLI replay receipt:\n{lookup_one}\n\
         {replay_receipt}"
    );

    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit_hex, WATCH_TIMEOUT)?;
    ensure!(
        parse_attempt(&terminal)? == 1,
        "{id}: terminal success must stay under attempt 1:\n{terminal}"
    );
    let page = session.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "scope page")?;
    let (declared, members) = parse_scope_page(&page_stdout)?;
    ensure!(
        declared == 1 && members == 1,
        "{id}: expected exactly one admitted entity (declared=1, members=1), got \
         declared={declared} members={members}:\n{page_stdout}"
    );
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
    let claims = match server {
        Subject::Java => {
            let claims = subject_records_for_work(&events_path, "EXECUTION_CLAIMED", "0:0:1")?;
            ensure!(
                claims == 1,
                "{id}: the Java subject recorded EXECUTION_CLAIMED {claims} times for 0:0:1, \
                 expected exactly 1"
            );
            format!("{claims} (subject record)")
        }
        Subject::Rust => {
            "not observable: the Rust subject emits only armed boundaries and its hooks \
             cannot arm EXECUTION_CLAIMED without a kill"
                .to_owned()
        }
    };
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        (
            "pending_window",
            format!(
                "header + {PENDING_PREFIX_LEN} of {} bytes written, no FIN, {} ms settle \
                 before the lookup",
                input.len(),
                PENDING_SETTLE.as_millis()
            ),
        ),
        (
            "lookup_while_pending",
            format!(
                "refused {} ({}) detail {:?} on control request {}",
                refusal_code_name(pending_lookup.code),
                pending_lookup.code,
                pending_lookup.detail,
                pending_lookup.tag_id
            ),
        ),
        (
            "admission_receipt",
            format!("attempt={attempt} admitted_at={admitted_at} deadline={deadline}"),
        ),
        (
            "raw_receipts_identical",
            "true (Work::Admitted and Work::OperationResponse receipt bytes equal)".into(),
        ),
        (
            "cli_replay",
            format!("RECEIPT with deadline {replay_deadline}; identical to both CLI lookups"),
        ),
        ("terminal_attempt", "1".into()),
        ("page_members", members.to_string()),
        ("execution_claimed_records", claims),
        (
            "result_read",
            "attempt 1 byte-exact against the independent oracle".into(),
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
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

// ---------------------------------------------------------------------------
// G5 rows added at milestone 19: rotation, remap, cross-authority, expiry
// ---------------------------------------------------------------------------

/// Fingerprints of a mapped identity as the principal map keys it, for
/// evidence files.
fn fingerprint(identity: &mtls::Identity) -> Result<String> {
    mtls::Material::leaf_sha256(identity)
}

/// Bind `owner`'s journal under one identity and return a Session whose
/// connection presents that identity.
fn session_with_identity(
    fixture: AuthorityFixture,
    server: OwnedServer,
    journal: PathBuf,
    identity: &mtls::Identity,
) -> Result<Session> {
    let connection = fixture.connection_args_for(&server, identity)?;
    Ok(Session {
        fixture,
        server,
        sequence: 1,
        journal,
        connection,
        policy_args: Vec::new(),
    })
}

/// Poll one work until it reaches `wanted` or a terminal state that is not
/// `wanted`; returns the final watch stdout.
fn watch_until_state(
    session: &Session,
    work: &str,
    wanted: u64,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let stdout = session.watch(work)?;
        let state = parse_state(&stdout)?;
        if state == wanted {
            return Ok(stdout);
        }
        ensure!(
            !(5..=8).contains(&state),
            "work {work} settled in state {state} ({}) while waiting for {wanted} ({})\n{stdout}",
            state_code_name(&state.to_string()),
            state_code_name(&wanted.to_string())
        );
        ensure!(
            Instant::now() < deadline,
            "work {work} did not reach state {wanted} within {timeout:?}\nlast view:\n{stdout}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// g5-cert-rotation-same-owner: the principal map carries TWO leaf hashes for
/// owner alice (the original and the rotated certificate). The session is
/// created and two retry-copy works are admitted under certificate 1; the
/// server is stopped and restarted on the same roots; the client attaches
/// with certificate 2 and must receive the identical binding, see the
/// certificate-1 operations still committed (lookup), and have its retry and
/// cancel accepted as the same owner; the retried work's result reads back
/// byte-exact under certificate 2.
fn g5_cert_rotation_same_owner(context: &ScenarioContext) -> Result<()> {
    g5_row(
        context,
        "g5-cert-rotation-same-owner",
        g5_cert_rotation_same_owner_direction,
    )
}

fn g5_cert_rotation_same_owner_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let id = "g5-cert-rotation-same-owner";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, Subject::Rust)?;
    enforce_no_fault_schedule(context, id)?;
    let (fixture, server) = g5_fixture(
        context,
        scenario_dir,
        server_subject,
        &[("alice", "alice"), ("alice-rotated", "alice")],
        &[],
        &[],
    )?;
    let cert_one = fixture.certs.principal("alice")?.clone();
    let cert_two = fixture.certs.principal("alice-rotated")?.clone();
    let fingerprint_one = fingerprint(&cert_one)?;
    let fingerprint_two = fingerprint(&cert_two)?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            (
                "principal_map",
                "two leaf hashes, both mapped to owner alice".into(),
            ),
            ("cert_1_sha256", fingerprint_one.clone()),
            ("cert_2_sha256", fingerprint_two.clone()),
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            (
                "sequence",
                "create + declare [1,2] + admit retry-copy/v2 twice under cert 1 (both park in \
                 AWAITING_RETRY); stop; restart on the same roots; attach under cert 2"
                    .into(),
            ),
            ("attach_under_cert_2", "identical BINDING text".into()),
            (
                "committed_under_cert_1",
                "lookup of the cert-1 admission returns its receipt".into(),
            ),
            (
                "mutations_under_cert_2",
                "retry 0:0:1 (expected 1) accepted with replacement attempt 2 and settles \
                 SUCCEEDED; cancel 0:0:2 accepted and settles CANCELLED"
                    .into(),
            ),
            (
                "read_under_cert_2",
                "0:0:1 attempt 2 byte-exact against the oracle".into(),
            ),
        ],
    )?;
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

    // Certificate 1: create, declare, two retry-copy admissions parked.
    let (journal, _connection) = bind_owner(&fixture, scenario_dir, &server, "alice", "alice", 1)?;
    let alice = session_with_identity(fixture, server, journal, &cert_one)?;
    let binding_one = require(&alice.op(&["binding"])?, "BINDING", "binding under cert 1")?;
    fs::write(artifacts.join("binding-cert-1.txt"), &binding_one)?;
    let declare = declare_sealed(&alice, &mut events, context.seed, "declare", &[1, 2])?;
    let admit_one = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    admit_application(
        &alice,
        &mut events,
        context.seed,
        "admit",
        &declare,
        "0:0:1",
        &input_path,
        "retry-copy/v2",
        1,
    )?;
    admit_application(
        &alice,
        &mut events,
        context.seed,
        "admit-2",
        &declare,
        "0:0:2",
        &input_path,
        "retry-copy/v2",
        1,
    )?;
    watch_until_state(&alice, "0:0:1", 2, RECOVERY_TIMEOUT)?;
    watch_until_state(&alice, "0:0:2", 2, RECOVERY_TIMEOUT)?;

    // Stop, restart on the same roots (same map with both hashes).
    let Session {
        fixture,
        server,
        journal,
        ..
    } = alice;
    server.stop()?;
    let restarted = fixture.start_server()?;
    events.append("", None, None, None, None, None)?;

    // Certificate 2 attaches to the same binding.
    let alice = session_with_identity(fixture, restarted, journal, &cert_two)?;
    let binding_two = require(&alice.op(&["binding"])?, "BINDING", "binding under cert 2")?;
    fs::write(artifacts.join("binding-cert-2.txt"), &binding_two)?;
    ensure!(
        binding_one.trim() == binding_two.trim(),
        "{id}: the binding under certificate 2 differs from certificate 1:\n{binding_one}\n\
         {binding_two}"
    );
    let lookup = alice.op(&["lookup", "--operation", &admit_one])?;
    let lookup = require(
        &lookup,
        "RECEIPT",
        "lookup of the cert-1 admission under cert 2",
    )?;
    fs::write(artifacts.join("lookup-cert-1-admission.txt"), &lookup)?;

    // Retry and cancel under certificate 2 are accepted as the same owner.
    let retry = oracle::operation_hex(oracle::operation_id(context.seed, "retry", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&retry)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let retry_out = alice.op(&[
        "retry",
        "--operation",
        &retry,
        "--work",
        "0:0:1",
        "--expected-attempt",
        "1",
    ])?;
    let retry_receipt = require(&retry_out, "RECEIPT", "retry under cert 2")?;
    ensure!(
        parse_replacement_attempt(&retry_receipt)? == 2,
        "{id}: retry under cert 2 must name replacement attempt 2:\n{retry_receipt}"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&retry)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let cancel = oracle::operation_hex(oracle::operation_id(context.seed, "cancel", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&cancel)?),
        Some("0:0:2"),
        Some(1),
        None,
        None,
    )?;
    let cancel_out = alice.op(&["cancel", "--operation", &cancel, "--work", "0:0:2"])?;
    let cancel_receipt = require(&cancel_out, "RECEIPT", "cancel under cert 2")?;
    let cancel_disposition = receipt_disposition(&cancel_receipt)?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&cancel)?),
        Some("0:0:2"),
        Some(1),
        None,
        None,
    )?;
    let retried = watch_until_state(&alice, "0:0:1", 5, RECOVERY_TIMEOUT)?;
    ensure!(
        parse_attempt(&retried)? == 2,
        "{id}: the retried work must succeed under attempt 2:\n{retried}"
    );
    let cancelled = watch_until_state(&alice, "0:0:2", 7, RECOVERY_TIMEOUT)?;
    read_output_verified(
        &alice,
        &mut events,
        "0:0:1",
        2,
        &input,
        &input_sha256,
        &artifacts,
        "output-attempt-2.bin",
    )?;
    detach(&alice)?;
    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.extend([
        ("cert_1_sha256", fingerprint_one),
        ("cert_2_sha256", fingerprint_two),
        ("binding_identical_across_rotation", "true".into()),
        ("cert_1_admission_lookup_under_cert_2", "RECEIPT".into()),
        (
            "retry_under_cert_2",
            "accepted: replacement attempt 2; SUCCEEDED(5) under attempt 2".into(),
        ),
        (
            "cancel_under_cert_2",
            format!(
                "accepted: disposition {cancel_disposition}; settled {} ({})",
                parse_state(&cancelled)?,
                state_code_name(&parse_state(&cancelled)?.to_string())
            ),
        ),
        (
            "read_under_cert_2",
            "attempt 2 byte-exact against the independent oracle".into(),
        ),
        (
            "restart",
            "graceful stop (SIGTERM, DRAINED) and restart on the same roots and map".into(),
        ),
    ]);
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, id, alice.server, events)
}

/// Refusal codes that would disclose retained state to a credential that
/// is no longer the owner: an authorization denial must come first.
const STATE_DISCLOSING_CODES: &[u32] = &[6, 7, 9, 12, 18];

/// g5-remapped-owner (matrix: g5-remapped-owner-denies): owner alice admits
/// and reads work; the operator stops the server, REMAPS alice's leaf hash
/// to owner mallory in the principal map, and restarts. Every committing
/// mutation and every read by that credential is then refused with a named
/// code that does not disclose the session's state; the output already
/// delivered before the remap stays valid on disk (re-hashed against the
/// oracle); nothing succeeds silently. Live-connection re-checking is not
/// observable through a stop/restart (every connection died with the stop)
/// and is recorded as such.
fn g5_remapped_owner(context: &ScenarioContext) -> Result<()> {
    g5_row(context, "g5-remapped-owner", g5_remapped_owner_direction)
}

fn g5_remapped_owner_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let id = "g5-remapped-owner";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, Subject::Rust)?;
    enforce_no_fault_schedule(context, id)?;
    let (fixture, server) = g5_fixture(
        context,
        scenario_dir,
        server_subject,
        &[("alice", "alice"), ("bob", "bob")],
        &[],
        &[],
    )?;
    let alice_identity = fixture.certs.principal("alice")?.clone();
    let alice_fingerprint = fingerprint(&alice_identity)?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("alice_leaf_sha256", alice_fingerprint.clone()),
            ("input_sha256", input_sha256.clone()),
            (
                "operator_action",
                "graceful stop; principal map edited so alice's leaf hash maps to owner \
                 mallory; restart on the same roots"
                    .into(),
            ),
            (
                "after_remap",
                "attach, retry, cancel, lookup and result read by that credential are each \
                 refused with a named code; none names a state-disclosing code \
                 (EXPIRED, CONFLICT, NOT_READY, CANCELLED, ALREADY_TERMINAL); no silent success"
                    .into(),
            ),
            (
                "delivered_bytes",
                "the output read before the remap is still byte-exact on disk".into(),
            ),
        ],
    )?;
    let (journal, _connection) = bind_owner(&fixture, scenario_dir, &server, "alice", "alice", 1)?;
    let alice = session_with_identity(fixture, server, journal, &alice_identity)?;
    let admit = alice_publish(&alice, &mut events, context.seed, &artifacts, &input)?;
    let delivered_sha = read_output_verified(
        &alice,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-before-remap.bin",
    )?;

    // Operator remap between a graceful stop and the restart.
    let Session {
        fixture,
        server,
        journal,
        ..
    } = alice;
    server.stop()?;
    let map_path = fixture.certs.principal_map.clone();
    let map = fs::read_to_string(&map_path)?;
    let mut remapped = String::new();
    let mut edited = 0;
    for line in map.lines() {
        match line.split_once('\t') {
            Some((hash, _owner)) if hash == alice_fingerprint => {
                remapped.push_str(&format!("{hash}\tmallory\n"));
                edited += 1;
            }
            _ => {
                remapped.push_str(line);
                remapped.push('\n');
            }
        }
    }
    ensure!(
        edited == 1,
        "{id}: alice's leaf hash must appear exactly once in the map"
    );
    fs::write(&map_path, &remapped)?;
    fs::write(artifacts.join("principals-remapped.tsv"), &remapped)?;
    // The readiness probe presents alice's certificate, which is now mallory
    // with no session: readiness is established with bob instead.
    let restarted = fixture.start_server_armed(None, false)?;
    fixture.next_sequence(&restarted, "bob")?;
    events.append("", None, None, None, None, None)?;
    let alice = session_with_identity(fixture, restarted, journal, &alice_identity)?;

    let retry = oracle::operation_hex(oracle::operation_id(context.seed, "retry", 1));
    let cancel = oracle::operation_hex(oracle::operation_id(context.seed, "cancel", 1));
    let read_target = crate::path(&artifacts.join("read-after-remap.bin"));
    let probes: Vec<(&str, Vec<&str>)> = vec![
        ("attach", vec!["binding"]),
        (
            "retry",
            vec![
                "retry",
                "--operation",
                &retry,
                "--work",
                "0:0:1",
                "--expected-attempt",
                "1",
            ],
        ),
        (
            "cancel",
            vec!["cancel", "--operation", &cancel, "--work", "0:0:1"],
        ),
        ("lookup", vec!["lookup", "--operation", &admit]),
        (
            "read",
            vec![
                "read",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0",
                "--output",
                &read_target,
            ],
        ),
    ];
    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.push(("alice_leaf_sha256", alice_fingerprint.clone()));
    observed.push((
        "remap",
        "alice's leaf hash -> owner mallory (map edited between stop and restart)".into(),
    ));
    let mut codes: BTreeSet<u32> = BTreeSet::new();
    for (name, operation) in &probes {
        let output = alice.op(operation)?;
        let outcome = probe_outcome(&output);
        let artifact = format!("after-remap-{name}.txt");
        let (len, sha256) = write_probe_artifact(&artifacts, &artifact, &outcome)?;
        let (code, line) =
            expect_named_refusal(&outcome, &format!("{id}: {name} after the remap"))?;
        ensure!(
            !STATE_DISCLOSING_CODES.contains(&code),
            "{id}: {name} after the remap named {} ({code}), which discloses the session's \
             state to a credential that is no longer its owner\n{}",
            refusal_code_name(u64::from(code)),
            outcome.transcript()
        );
        codes.insert(code);
        events.append(
            "REFUSAL_RECEIVED",
            None,
            Some("0:0:1"),
            Some(1),
            Some(code),
            Some(ArtifactRef {
                path: format!("artifacts/{artifact}"),
                len,
                sha256,
            }),
        )?;
        observed.push((
            Box::leak(format!("after_remap_{name}").into_boxed_str()),
            format!(
                "refused {} ({code}): {line}",
                refusal_code_name(u64::from(code))
            ),
        ));
    }
    ensure!(
        !artifacts.join("read-after-remap.bin").exists()
            || fs::metadata(artifacts.join("read-after-remap.bin"))?.len() == 0,
        "{id}: the refused read still wrote output bytes"
    );
    let on_disk = fs::read(artifacts.join("output-before-remap.bin"))?;
    let on_disk_sha = oracle::sha256_hex(&on_disk);
    ensure!(
        on_disk_sha == delivered_sha && on_disk_sha == input_sha256,
        "{id}: the pre-remap output artifact no longer matches the oracle"
    );
    observed.extend([
        (
            "refusal_codes_after_remap",
            codes
                .iter()
                .map(|code| format!("{} ({code})", refusal_code_name(u64::from(*code))))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        (
            "pre_remap_output_still_valid",
            format!("true (sha256={on_disk_sha})"),
        ),
        (
            "live_connection_recheck",
            "not observable through stop/restart: every connection died with the graceful \
             stop, so only fresh connections are re-evaluated here"
                .into(),
        ),
    ]);
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, id, alice.server, events)
}

/// Authority label of the second authority in g5-cross-authority-reference.
const AUTHORITY_Y: &str = "issuer-b";

/// g5-cross-authority-reference: authority X publishes a result and the
/// driver records its REFERENCE (the selected output as the client renders
/// it). Authority Y has separate roots, its own principal map (alice mapped)
/// and the label `issuer-b`, and no session of X's. Arm A resolves X's saved
/// selection against Y from X's journal: refused without disclosure, no
/// bytes delivered. Arm B binds a journal on Y and selects X's identifiers
/// there: refused without disclosure. The positive arm reads the exact bytes
/// on X. Neither published client dereferences a locator's authority (the
/// Rust `read` uses the journal selection on the configured connection;
/// Java: DurableClientLocatorTest, S12-280/283), which is recorded, not
/// asserted here.
fn g5_cross_authority_reference(context: &ScenarioContext) -> Result<()> {
    g5_row(
        context,
        "g5-cross-authority-reference",
        g5_cross_authority_reference_direction,
    )
}

fn g5_cross_authority_reference_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let id = "g5-cross-authority-reference";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, Subject::Rust)?;
    enforce_no_fault_schedule(context, id)?;
    let (fixture_x, server_x) = g5_fixture(
        context,
        scenario_dir,
        server_subject,
        &[("alice", "alice")],
        &[],
        &[],
    )?;
    let alice_identity = fixture_x.certs.principal("alice")?.clone();
    let alice_fingerprint = fingerprint(&alice_identity)?;
    // Authority Y: same CA and client identities, its own map and roots.
    let y_dir = scenario_dir.join("authority-y");
    fs::create_dir_all(&y_dir)?;
    let y_map = y_dir.join("principals.tsv");
    fs::write(
        &y_map,
        format!("sha256\tprincipal\n{alice_fingerprint}\talice\n"),
    )?;
    let fixture_y = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &y_dir.join("subject"),
        fixture_x.certs.with_principal_map(&y_map),
        server_subject,
        Subject::Rust,
    )?
    .with_authority(AUTHORITY_Y);
    fixture_y.run_init_authority()?;
    let server_y = fixture_y.start_server()?;
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("authority_x", fixture_x.authority().to_owned()),
            ("authority_y", AUTHORITY_Y.into()),
            ("input_sha256", input_sha256.clone()),
            (
                "arm_a",
                "X's journal (bound to X, selection saved) pointed at Y: attach, read, lookup \
                 and watch each refused with a named code; no result bytes written"
                    .into(),
            ),
            (
                "arm_b",
                "a journal bound on Y selects X's work/attempt/index: refused with a named \
                 code (Y holds no such work; nothing about X is disclosed)"
                    .into(),
            ),
            (
                "positive_arm",
                "the same selection reads byte-exact on X".into(),
            ),
        ],
    )?;
    let (journal_x, _) = bind_owner(&fixture_x, scenario_dir, &server_x, "alice", "alice", 1)?;
    let alice_x = session_with_identity(fixture_x, server_x, journal_x, &alice_identity)?;
    let admit = alice_publish(&alice_x, &mut events, context.seed, &artifacts, &input)?;
    let select = alice_x.op(&[
        "select",
        "--work",
        "0:0:1",
        "--attempt",
        "1",
        "--index",
        "0",
    ])?;
    let reference = require(&select, "REFERENCE", "select on X")?;
    fs::write(artifacts.join("reference-x.txt"), &reference)?;
    read_output_verified(
        &alice_x,
        &mut events,
        "0:0:1",
        1,
        &input,
        &input_sha256,
        &artifacts,
        "output-x.bin",
    )?;

    // Arm A: X's journal against Y.
    let connection_y = fixture_y.connection_args(&server_y, "alice")?;
    let cross_read = crate::path(&artifacts.join("cross-read.bin"));
    let probes: Vec<(&str, Vec<&str>)> = vec![
        ("attach", vec!["binding"]),
        (
            "read",
            vec![
                "read",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0",
                "--output",
                &cross_read,
            ],
        ),
        ("lookup", vec!["lookup", "--operation", &admit]),
        ("watch", vec!["watch", "--work", "0:0:1"]),
    ];
    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.push(("authority_y", AUTHORITY_Y.into()));
    observed.push(("reference_x", reference.trim().to_owned()));
    for (name, operation) in &probes {
        let output = alice_x.fixture.run_client_op(
            &alice_x.journal,
            "alice",
            1,
            &connection_y,
            operation,
        )?;
        let outcome = probe_outcome(&output);
        let artifact = format!("arm-a-{name}-against-y.txt");
        let (len, sha256) = write_probe_artifact(&artifacts, &artifact, &outcome)?;
        let (code, line) =
            expect_named_refusal(&outcome, &format!("{id}: arm A {name} against Y"))?;
        events.append(
            "REFUSAL_RECEIVED",
            None,
            Some("0:0:1"),
            Some(1),
            Some(code),
            Some(ArtifactRef {
                path: format!("artifacts/{artifact}"),
                len,
                sha256,
            }),
        )?;
        observed.push((
            Box::leak(format!("arm_a_{name}_against_y").into_boxed_str()),
            format!(
                "refused {} ({code}): {line}",
                refusal_code_name(u64::from(code))
            ),
        ));
    }
    ensure!(
        !artifacts.join("cross-read.bin").exists()
            || fs::metadata(artifacts.join("cross-read.bin"))?.len() == 0,
        "{id}: the cross-authority read wrote result bytes"
    );

    // Arm B: a journal bound on Y selects X's identifiers.
    let (journal_y, _) = bind_owner(&fixture_y, &y_dir, &server_y, "alice-y", "alice", 1)?;
    let select_y = fixture_y.run_client_op(
        &journal_y,
        "alice",
        1,
        &connection_y,
        &[
            "select",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
        ],
    )?;
    let outcome = probe_outcome(&select_y);
    let (len, sha256) = write_probe_artifact(&artifacts, "arm-b-select-on-y.txt", &outcome)?;
    let (code, line) = expect_named_refusal(&outcome, &format!("{id}: arm B select on Y"))?;
    events.append(
        "REFUSAL_RECEIVED",
        None,
        Some("0:0:1"),
        Some(1),
        Some(code),
        Some(ArtifactRef {
            path: "artifacts/arm-b-select-on-y.txt".into(),
            len,
            sha256,
        }),
    )?;
    ensure!(
        !outcome.stdout.contains(input_sha256.as_str())
            && !outcome.stderr.contains(input_sha256.as_str()),
        "{id}: Y's refusal disclosed X's output digest"
    );
    observed.push((
        "arm_b_select_on_y",
        format!(
            "refused {} ({code}): {line}",
            refusal_code_name(u64::from(code))
        ),
    ));
    observed.push((
        "positive_arm_read_on_x",
        "byte-exact against the independent oracle".into(),
    ));
    observed.push((
        "locator_dereference",
        "neither client dereferences a locator's authority: the Rust read uses the journal \
         selection on the configured connection (server/src/v2/client.rs Operation::Read), \
         the Java client stays on the configured connection (DurableClientLocatorTest, \
         S12-280/283); recorded, not asserted by this row"
            .into(),
    ));
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    detach(&alice_x)?;
    server_y.stop()?;
    stop_and_seal(context, scenario_dir, id, alice_x.server, events)
}

/// Validity window of the short-lived leaf in g5-expired-identity: long
/// enough to bind and attach a raw connection, short enough to wait out.
const EXPIRY_VALIDITY: Duration = Duration::from_secs(25);
/// Margin past not_after before the post-expiry probes (both subjects check
/// validity against host UTC with second resolution).
const EXPIRY_MARGIN: Duration = Duration::from_secs(4);

/// g5-expired-identity: alice's leaf is valid for 25 s from minting. While
/// valid: the CLI creates the session and declares, and a raw connection
/// attaches and stays open. After expiry: the existing connection's
/// behaviour is recorded per subject (the Java server closes it at expiry,
/// S12-098; the Rust server was unmeasured); a fresh connection with the
/// expired leaf must fail at the handshake with no application refusal; a
/// renewed leaf mapped to the same owner attaches to the same session and
/// sees the declaration. Host UTC is never changed.
fn g5_expired_identity(context: &ScenarioContext) -> Result<()> {
    g5_row(
        context,
        "g5-expired-identity",
        g5_expired_identity_direction,
    )
}

fn g5_expired_identity_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server_subject: Subject,
) -> Result<()> {
    let id = "g5-expired-identity";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, Subject::Rust)?;
    enforce_no_fault_schedule(context, id)?;
    let minted_at = Instant::now();
    let certs = mtls::generate_spec(
        &scenario_dir.join("certs"),
        &[("alice-renewed", "alice"), ("bob", "bob")],
        &[],
        &[],
        &[("alice", "alice", EXPIRY_VALIDITY)],
    )?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &scenario_dir.join("subject"),
        certs,
        server_subject,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let server = fixture.start_server()?;
    let short_identity = fixture.certs.principal("alice")?.clone();
    let renewed_identity = fixture.certs.principal("alice-renewed")?.clone();
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("validity_seconds", EXPIRY_VALIDITY.as_secs().to_string()),
            ("short_leaf_sha256", fingerprint(&short_identity)?),
            ("renewed_leaf_sha256", fingerprint(&renewed_identity)?),
            (
                "clock_mode",
                "real elapsed time; host UTC never changed".into(),
            ),
            (
                "while_valid",
                "session created and [1] declared under the short leaf; a raw connection \
                 attached and kept open"
                    .into(),
            ),
            (
                "existing_connection_after_expiry",
                "recorded per subject (java: closed UNAUTHORIZED at expiry per S12-098; \
                 rust: unmeasured before this row)"
                    .into(),
            ),
            (
                "fresh_connection_after_expiry",
                "handshake failure (TLS certificate expired), no application refusal".into(),
            ),
            (
                "renewed_leaf",
                "attaches to the same generation with the identical binding and sees the \
                 declaration"
                    .into(),
            ),
        ],
    )?;
    let (journal, _) = bind_owner(&fixture, scenario_dir, &server, "alice", "alice", 1)?;
    let alice = session_with_identity(fixture, server, journal, &short_identity)?;
    let binding_valid = require(&alice.op(&["binding"])?, "BINDING", "binding while valid")?;
    fs::write(artifacts.join("binding-while-valid.txt"), &binding_valid)?;
    declare_sealed(&alice, &mut events, context.seed, "declare", &[1])?;
    let peer = Peer::with_keep_alive(Duration::from_secs(5))?;
    let (mut live, _binding) = raw_attach(
        &peer,
        &alice.fixture,
        &alice.server,
        &mut events,
        &artifacts,
        "alice",
        "alice",
        1,
    )?;
    let (declared_before, _) = raw_page(&mut live, 2, 0)?;
    ensure!(
        declared_before == 1,
        "{id}: the live connection must see the declaration while valid"
    );

    // Wait out the validity window.
    let expiry = minted_at + EXPIRY_VALIDITY + EXPIRY_MARGIN;
    let now = Instant::now();
    if now < expiry {
        thread::sleep(expiry - now);
    }
    events.append("", None, None, None, None, None)?;

    // Existing connection: served, or closed by the subject (recorded).
    let existing = match live
        .send_control(FRAME_SCOPE, &rawclient::scope_page(3, 0))
        .and_then(|()| live.read_control())
    {
        Ok(Frame::Control(FRAME_SCOPE, body)) => {
            let (declared, _) = rawclient::parse_page(&body)?;
            format!("served after expiry (page declared={declared})")
        }
        Ok(Frame::Control(FRAME_REFUSAL, body)) => {
            let refusal = rawclient::parse_refusal(&body)?;
            format!(
                "request refused on the live connection: {} ({}) detail {:?}",
                refusal_code_name(refusal.code),
                refusal.code,
                refusal.detail
            )
        }
        Ok(Frame::Control(other, body)) => {
            format!(
                "unexpected control frame {other} after expiry (body {})",
                hex(&body)
            )
        }
        Ok(Frame::Fin) => "control stream finished by the subject after expiry".to_owned(),
        Err(error) => match live.try_wait_closed(Duration::from_secs(3))? {
            Some(close) => format!("closed by the subject: {}", close_text(&close)),
            None => format!("request failed without a close within 3 s: {error:#}"),
        },
    };
    fs::write(
        artifacts.join("existing-connection-after-expiry.txt"),
        format!("{existing}\n"),
    )?;
    drop(live);

    // Fresh connection with the expired leaf: handshake failure only.
    let probe = alice
        .fixture
        .probe_next_sequence(&alice.server, &short_identity)?;
    let outcome = probe_outcome(&probe);
    let (len, sha256) =
        write_probe_artifact(&artifacts, "expired-leaf-fresh-connection.txt", &outcome)?;
    ensure!(
        !outcome.success,
        "{id}: a fresh connection with the expired leaf was accepted\n{}",
        outcome.transcript()
    );
    ensure!(
        outcome.refusal.is_none(),
        "{id}: the expired leaf was rejected with an application refusal instead of at the \
         handshake\n{}",
        outcome.transcript()
    );
    events.append(
        "REFUSAL_RECEIVED",
        None,
        None,
        None,
        None,
        Some(ArtifactRef {
            path: "artifacts/expired-leaf-fresh-connection.txt".into(),
            len,
            sha256,
        }),
    )?;

    // Renewed leaf, same owner, same session.
    let Session {
        fixture,
        server,
        journal,
        ..
    } = alice;
    let alice = session_with_identity(fixture, server, journal, &renewed_identity)?;
    let binding_renewed = require(
        &alice.op(&["binding"])?,
        "BINDING",
        "binding under the renewed leaf",
    )?;
    ensure!(
        binding_renewed.trim() == binding_valid.trim(),
        "{id}: the renewed leaf attached to a different binding:\n{binding_valid}\n{binding_renewed}"
    );
    let page = alice.op(&["page", "--scope", "0"])?;
    let page_stdout = require(&page, "SCOPE", "page under the renewed leaf")?;
    let (declared, _members) = parse_scope_page(&page_stdout)?;
    ensure!(
        declared == 1,
        "{id}: the renewed leaf does not see the declaration:\n{page_stdout}"
    );
    detach(&alice)?;
    let mut observed: Vec<(&str, String)> = Vec::new();
    g5_observed_preamble(&mut observed, server_subject, context);
    observed.extend([
        ("validity_seconds", EXPIRY_VALIDITY.as_secs().to_string()),
        ("waited_ms", (minted_at.elapsed().as_millis()).to_string()),
        ("existing_connection_after_expiry", existing),
        (
            "fresh_connection_after_expiry",
            format!(
                "refused at the handshake, no application refusal: {}",
                outcome.stderr.lines().next().unwrap_or("")
            ),
        ),
        ("renewed_leaf_binding_identical", "true".into()),
        ("renewed_leaf_sees_declaration", "true (declared=1)".into()),
    ]);
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, id, alice.server, events)
}

// ---------------------------------------------------------------------------
// G7 rows added at milestone 19: read pin past expiry, deadline queue time
// ---------------------------------------------------------------------------

/// Result object of g7-read-pin-past-expiry: the negotiated object limit,
/// so the read cannot finish inside the client's receive window.
const PIN_OBJECT_LEN: usize = 16 * 1024 * 1024;
/// Chunk and cadence of the slow reader: 256 KiB every 150 ms drains 16 MiB
/// in about ten seconds, spanning the 5 s output retention while never
/// leaving the stream idle for the Rust subject's 5 s stream idle bound.
const PIN_READ_CHUNK: usize = 256 * 1024;
const PIN_READ_PACE: Duration = Duration::from_millis(150);
const PIN_READ_TIMEOUT: Duration = Duration::from_secs(20);
/// Bounded wait for the subject to reclaim the expired object once the
/// pinned read has finished (supplementary evidence; recorded either way).
const PIN_RECLAIM_WAIT: Duration = Duration::from_secs(30);

/// g7-read-pin-past-expiry: output retention 5 s. A 16 MiB copy is
/// published and selected; a raw reader opens the result stream before
/// expiry and drains it slowly so the transfer is still open when the
/// output's availability passes. A NEW read after expiry is refused with the
/// named EXPIRED code while the pinned read is still open; the pinned read
/// then completes byte-exact. The object directory is sampled after
/// publication, at expiry during the read, and after the read (supplementary
/// file evidence: what the subject reclaimed and when is recorded, never
/// inferred).
fn g7_read_pin_past_expiry(context: &ScenarioContext) -> Result<()> {
    run_expiry_row(
        context,
        "g7-read-pin-past-expiry",
        g7_read_pin_past_expiry_direction,
    )
}

fn g7_read_pin_past_expiry_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g7-read-pin-past-expiry";
    let policy = (60_000u64, 5_000u64, 20_000u64);
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    enforce_no_fault_schedule(context, id)?;
    let session = setup_session_policy(
        context,
        scenario_dir,
        server,
        client,
        policy.0,
        policy.1,
        policy.2,
    )?;
    let input = oracle::dataset(context.seed, PIN_OBJECT_LEN);
    let input_sha256 = oracle::sha256_hex(&input);
    let sha: [u8; 32] = Sha256::digest(&input).into();
    let input_path = artifacts.join("input.bin");
    fs::write(&input_path, &input)?;
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("policy_execution_limit_ms", policy.0.to_string()),
            ("policy_output_retention_ms", policy.1.to_string()),
            ("policy_receipt_retention_ms", policy.2.to_string()),
            ("clock_mode", "real-short-policy".into()),
            ("input_len", input.len().to_string()),
            ("input_sha256", input_sha256.clone()),
            (
                "slow_reader",
                format!(
                    "raw result stream opened before output expiry and drained {PIN_READ_CHUNK} \
                     bytes every {} ms, so the transfer is open when availability passes",
                    PIN_READ_PACE.as_millis()
                ),
            ),
            (
                "new_read_after_expiry",
                "CLI read refuses named EXPIRED (6), never OUTPUT_UNAVAILABLE (16), while the \
                 pinned read is still open"
                    .into(),
            ),
            (
                "pinned_read",
                "completes byte-exact against the oracle after the expiry".into(),
            ),
            (
                "object_dir_evidence",
                "sampled after publication, at expiry during the read, after the read; \
                 reclamation recorded (supplementary), never inferred"
                    .into(),
            ),
        ],
    )?;
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
    // Through the session so the short policy triple is redeclared (the
    // Rust client refuses an op whose policy flags differ from its journal).
    let admitted = session.op(&[
        "admit",
        "--operation",
        &admit,
        "--declaration",
        &declare,
        "--work",
        "0:0:1",
        "--input",
        &crate::path(&input_path),
        "--application",
        "copy/v2",
    ])?;
    require(&admitted, "RECEIPT", "admit operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let terminal = watch_terminal(&session, &mut events, "0:0:1", &admit, RECOVERY_TIMEOUT)?;
    let output_until = parse_field_u64(&terminal, "output_until")?
        .context("terminal view did not report output_until")?;
    let select = session.op(&[
        "select",
        "--work",
        "0:0:1",
        "--attempt",
        "1",
        "--index",
        "0",
    ])?;
    require(&select, "REFERENCE", "select before expiry")?;
    let metrics_after_publish = storage_metrics(&session.fixture.object_dir)?;

    // The raw slow reader.
    let peer = Peer::with_keep_alive(Duration::from_secs(5))?;
    let (mut reader, _) = raw_attach(
        &peer,
        &session.fixture,
        &session.server,
        &mut events,
        &artifacts,
        "alice",
        "alice",
        1,
    )?;
    let read_started_ms = utc_now_millis();
    ensure!(
        read_started_ms < output_until,
        "{id}: the read started at {read_started_ms}, after output availability {output_until}; \
         the row cannot show a pin"
    );
    reader.send_control(
        FRAME_RESULT,
        &rawclient::result_read(2, (0, 0, 1), 1, 0, &sha),
    )?;
    events.append("REQUEST_SENT", None, Some("0:0:1"), Some(1), None, None)?;
    let mut stream = reader.accept_uni(PIN_READ_TIMEOUT)?;
    let mut prefix = [0u8; 4];
    reader.read_stream_exact(&mut stream, &mut prefix, PIN_READ_TIMEOUT)?;
    let header_len = u32::from_be_bytes(prefix) as usize;
    ensure!(
        header_len <= 4096,
        "{id}: result header length {header_len} unbounded"
    );
    let mut header = vec![0u8; header_len];
    reader.read_stream_exact(&mut stream, &mut header, PIN_READ_TIMEOUT)?;
    let (declared_len, declared_sha) = rawclient::parse_result_header(&header)?;
    ensure!(
        declared_len == input.len() as u64 && declared_sha == sha,
        "{id}: result header declares len {declared_len} sha {}, expected {} {}",
        hex(&declared_sha),
        input.len(),
        input_sha256
    );
    let mut received: Vec<u8> = Vec::with_capacity(input.len());
    let mut chunk = vec![0u8; PIN_READ_CHUNK];
    let mut bytes_before_expiry = 0usize;
    let mut expiry_probe: Option<(String, (u64, u64, u64))> = None;
    let mut fin = false;
    while !fin {
        let mut filled = 0usize;
        while filled < chunk.len() {
            match reader.read_stream(&mut stream, &mut chunk[filled..], PIN_READ_TIMEOUT)? {
                Some(n) => filled += n,
                None => {
                    fin = true;
                    break;
                }
            }
        }
        received.extend_from_slice(&chunk[..filled]);
        let now = utc_now_millis();
        if now < output_until {
            bytes_before_expiry = received.len();
        } else if expiry_probe.is_none() && now >= output_until + 1_000 {
            // Availability has passed while the transfer is open: a NEW read
            // must refuse EXPIRED now, and the object directory is sampled.
            let metrics = storage_metrics(&session.fixture.object_dir)?;
            let refusal = expect_expired_read(
                &session,
                &artifacts,
                "read-after-output-expiry.txt",
                "expired-read.bin",
            )?;
            expiry_probe = Some((refusal, metrics));
        }
        thread::sleep(PIN_READ_PACE);
    }
    let (refusal_text, metrics_at_expiry) = match expiry_probe {
        Some(probe) => probe,
        None => {
            // The transfer finished before availability passed plus a second:
            // the pin was never exercised. Say so rather than probing late.
            bail!(
                "{id}: the raw read finished {} bytes before output_until {output_until} + 1 s \
                 (now {}); the slow reader did not span the expiry",
                received.len(),
                utc_now_millis()
            );
        }
    };
    let read_finished_ms = utc_now_millis();
    let received_sha256 = oracle::sha256_hex(&received);
    ensure!(
        received.len() == input.len() && received == input,
        "{id}: the pinned read returned {} bytes sha256={received_sha256}, expected {} bytes \
         sha256={input_sha256}",
        received.len(),
        input.len()
    );
    events.append("RESULT_VERIFIED", None, Some("0:0:1"), Some(1), None, None)?;
    raw_detach(&mut reader, 3)?;
    drop(reader);

    // After the read: what the subject reclaims, bounded (supplementary).
    let reclaim_deadline = Instant::now() + PIN_RECLAIM_WAIT;
    let metrics_after_read = loop {
        let metrics = storage_metrics(&session.fixture.object_dir)?;
        if metrics.1 < metrics_after_publish.1 || Instant::now() >= reclaim_deadline {
            break metrics;
        }
        thread::sleep(Duration::from_millis(500));
    };
    let reclaimed = metrics_after_read.1 < metrics_after_publish.1;
    // Receipt retention still holds: the view is readable and frozen.
    let after_view = session.watch("0:0:1")?;
    ensure!(
        parse_state(&after_view)? == 5 && parse_attempt(&after_view)? == 1,
        "{id}: the terminal outcome changed after the pinned read:\n{after_view}"
    );
    detach(&session)?;
    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("clock_mode", "real-short-policy".into()),
        ("output_until", output_until.to_string()),
        ("raw_read_started_ms", read_started_ms.to_string()),
        ("raw_read_finished_ms", read_finished_ms.to_string()),
        (
            "bytes_received_before_expiry",
            bytes_before_expiry.to_string(),
        ),
        ("bytes_received_total", received.len().to_string()),
        (
            "pinned_read",
            format!("completed byte-exact after expiry (sha256={received_sha256})"),
        ),
        (
            "new_read_after_expiry",
            format!(
                "named EXPIRED (6) while the pinned read was open ({})",
                refusal_text
                    .lines()
                    .find(|line| transcript_named_code(line).is_some())
                    .unwrap_or("see artifacts/read-after-output-expiry.txt")
            ),
        ),
        (
            "object_dir_after_publish",
            metrics_text(metrics_after_publish),
        ),
        (
            "object_dir_at_expiry_during_read",
            metrics_text(metrics_at_expiry),
        ),
        ("object_dir_after_read", metrics_text(metrics_after_read)),
        (
            "reclaimed_after_read",
            if reclaimed {
                format!(
                    "true (within {} s of the read finishing)",
                    PIN_RECLAIM_WAIT.as_secs()
                )
            } else {
                format!(
                    "not observed within {} s (recorded, supplementary evidence only)",
                    PIN_RECLAIM_WAIT.as_secs()
                )
            },
        ),
        (
            "reclaimed_during_read",
            if metrics_at_expiry.1 < metrics_after_publish.1 {
                "one object left the directory while the read was still open and the \
                 pinned read still completed byte-exact; whether that was the expired output \
                 (unlinked under an open descriptor) or the retained input is not \
                 distinguished by directory totals; recorded as an observation, not asserted"
                    .into()
            } else {
                "false (object directory unchanged while the read was open)".into()
            },
        ),
        (
            "work_view_after_read",
            "state=5 attempt=1 (receipt retention holds)".into(),
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
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

/// Execution deadline of the queued probe work in g7-deadline-queue-time.
const QUEUE_PROBE_EXECUTION_MS: u64 = 1_000;
/// Margin past the probe's deadline before the queue is released or the
/// outcome is read.
const QUEUE_PROBE_MARGIN: Duration = Duration::from_millis(1_500);
/// Hold bound of the saturating pauses on the Java server.
const QUEUE_HOLD_DEADLINE_MS: u64 = 120_000;
/// Rust load: two chunk-copy parents of this size occupy alice's two
/// execution slots (quinn v2_authority/runtime.rs PoolConfig
/// workers_per_owner 2) for longer than the probe's deadline.
const QUEUE_LOAD_INPUT_LEN: usize = 4 * 1024 * 1024;
const QUEUE_LOAD_ATTEMPTS: u32 = 3;

/// What the queued probe did once the pool was released.
struct QueueSettlement {
    admitted_at: u64,
    deadline: u64,
    state_while_queued: String,
    state_past_deadline_queued: String,
    final_state: u64,
    diagnostic: String,
    terminal_at: Option<u64>,
    view: String,
}

/// Admit the probe work with the short execution deadline and return its
/// receipt fields.
fn queue_probe_admit(
    session: &Session,
    events: &mut EventWriter,
    seed: u64,
    declare: &str,
    work: &str,
    input_path: &Path,
) -> Result<(String, u64, u64)> {
    let admit = oracle::operation_hex(oracle::operation_id(seed, "admit-probe", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit)?),
        Some(work),
        Some(1),
        None,
        None,
    )?;
    let child = session.fixture.spawn_client_op(
        &session.journal,
        "alice",
        session.sequence,
        &session.connection,
        &[
            "admit",
            "--operation",
            &admit,
            "--declaration",
            declare,
            "--work",
            work,
            "--input",
            &crate::path(input_path),
            "--application",
            "copy/v2",
            "--execution-ms",
            &QUEUE_PROBE_EXECUTION_MS.to_string(),
        ],
    )?;
    let admitted = AuthorityFixture::wait_client_op(child, OP_WAIT)?;
    let receipt = require(&admitted, "RECEIPT", "probe admission")?;
    let admitted_at = parse_field_u64(&receipt, "admitted_at")?
        .context("probe receipt did not render admitted_at")?;
    let deadline = parse_field_u64(&receipt, "deadline")?
        .context("probe receipt did not render a deadline")?;
    ensure!(
        deadline == admitted_at + QUEUE_PROBE_EXECUTION_MS,
        "probe deadline {deadline} != admitted_at {admitted_at} + {QUEUE_PROBE_EXECUTION_MS} \
         (the client's --execution-ms was not honoured)"
    );
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some(work),
        Some(1),
        None,
        None,
    )?;
    Ok((admit, admitted_at, deadline))
}

/// Wait for the probe to settle and describe it.
fn queue_probe_settle(
    session: &Session,
    work: &str,
    admitted_at: u64,
    deadline: u64,
    state_while_queued: String,
    state_past_deadline_queued: String,
) -> Result<QueueSettlement> {
    let until = Instant::now() + RECOVERY_TIMEOUT;
    let view = loop {
        let stdout = session.watch(work)?;
        let state = parse_state(&stdout)?;
        if (5..=8).contains(&state) {
            break stdout;
        }
        ensure!(
            Instant::now() < until,
            "probe {work} did not settle within {RECOVERY_TIMEOUT:?}\nlast view:\n{stdout}"
        );
        thread::sleep(Duration::from_millis(50));
    };
    let final_state = parse_state(&view)?;
    let diagnostic = view_diagnostic_code(&view)
        .map(|code| format!("{} ({code})", refusal_code_name(code)))
        .unwrap_or_else(|| "none".to_owned());
    let terminal_at = parse_field_u64(&view, "terminal_at")?;
    Ok(QueueSettlement {
        admitted_at,
        deadline,
        state_while_queued,
        state_past_deadline_queued,
        final_state,
        diagnostic,
        terminal_at,
        view,
    })
}

fn state_text(view: &str) -> Result<String> {
    let state = parse_state(view)?;
    Ok(format!("{state} ({})", state_code_name(&state.to_string())))
}

/// g7-deadline-queue-time: the execution pool is saturated, more work is
/// admitted with a 1 s execution deadline, and the deadline passes while that
/// work is still queued. Expected: the deadline is accounted from admission
/// (the receipt says so: deadline = admitted_at + execution-ms), the queued
/// work settles FAILED with the DEADLINE_EXCEEDED diagnostic and is never
/// executed to success after its deadline, an explicit retry refuses a named
/// code (DEADLINE_EXCEEDED per the matrix; both subjects name
/// ALREADY_TERMINAL, recorded as in g4-deadline-settlement), and the result
/// read refuses a named code.
///
/// Java server: two `pause` rows at EXECUTION_CLAIMED hold alice's two
/// per-owner workers (DurableHost ExecutionLimits(4, 2, ...)) deterministically;
/// the probe queues behind them; the pool is released after the deadline.
/// Rust server: the fixture hooks cannot pause at EXECUTION_CLAIMED, so
/// alice's two slots (PoolConfig workers_per_owner 2) are loaded with two
/// 4 MiB chunk-copy parents and the probe is admitted behind them; if the
/// load does not outlast the deadline in three fresh authorities the
/// direction reports the missing hold instead of claiming the property.
fn g7_deadline_queue_time(context: &ScenarioContext) -> Result<()> {
    let id = "g7-deadline-queue-time";
    let scenario_dir = context.scenario_dir(id);
    match g7_deadline_queue_time_direction(context, &scenario_dir, Subject::Rust, Subject::Rust) {
        Ok(()) => {}
        Err(error) if error.downcast_ref::<MissingCapability>().is_some() => {
            fs::write(
                scenario_dir.join("INCOMPLETE"),
                format!("rust-client/rust-server direction is INCOMPLETE: {error:#}\n"),
            )?;
            println!("INCOMPLETE {id} rust-client/rust-server: {error:#}");
        }
        Err(error) => return Err(error),
    }
    run_hooked_direction(context, id, "rust-client-java-server", |context, dir| {
        g7_deadline_queue_time_direction(context, dir, Subject::Java, Subject::Rust)
    })?;
    Ok(())
}

fn g7_deadline_queue_time_direction(
    context: &ScenarioContext,
    scenario_dir: &Path,
    server: Subject,
    client: Subject,
) -> Result<()> {
    let id = "g7-deadline-queue-time";
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;
    let mut events = open_events(context, scenario_dir, id, client)?;
    let probe_input = oracle::dataset(context.seed ^ 0x7ead, INPUT_LEN);
    let probe_path = artifacts.join("probe-input.bin");
    fs::write(&probe_path, &probe_input)?;
    let saturation = match server {
        Subject::Java => "two schedule pauses at EXECUTION_CLAIMED hold alice's two \
                          per-owner workers (ExecutionLimits(4, 2, ...)); released after \
                          the probe's deadline passed"
            .to_owned(),
        Subject::Rust => format!(
            "no hold at EXECUTION_CLAIMED on the Rust subject (fixture pauses only at reply \
             pairs): alice's two slots (PoolConfig workers_per_owner 2) are loaded with two \
             {QUEUE_LOAD_INPUT_LEN}-byte chunk-copy/v2 parents admitted back to back, up to \
             {QUEUE_LOAD_ATTEMPTS} fresh authorities"
        ),
    };
    write_kv(
        scenario_dir,
        "expected.tsv",
        &[
            ("probe_execution_ms", QUEUE_PROBE_EXECUTION_MS.to_string()),
            ("probe_input_len", INPUT_LEN.to_string()),
            ("saturation", saturation.clone()),
            (
                "deadline_accounting",
                "receipt deadline = admitted_at + execution-ms (queue time counts from \
                 admission)"
                    .into(),
            ),
            (
                "settlement",
                "the probe settles FAILED(6) with the DEADLINE_EXCEEDED (11) diagnostic, \
                 terminal_at >= deadline, never SUCCEEDED after its deadline passed in the \
                 queue, never silently evicted (the view keeps the work)"
                    .into(),
            ),
            (
                "retry_after",
                "explicit retry refuses DEADLINE_EXCEEDED (11) per the matrix; both subjects \
                 check terminality first and answer ALREADY_TERMINAL (18); either named code \
                 is accepted and recorded"
                    .into(),
            ),
            ("read_after", "refused with a named code".into()),
        ],
    )?;

    let mut observed: Vec<(&str, String)> = vec![
        ("server_subject", server.name().into()),
        ("client_subject", client.name().into()),
        ("saturation", saturation),
    ];
    let (session, settlement, admit, work, extra): (
        Session,
        QueueSettlement,
        String,
        String,
        Vec<(&str, String)>,
    ) = match server {
        Subject::Java => {
            let rows: Vec<schedule::ScheduleRow> = [
                schedule::Action::Pause,
                schedule::Action::Release,
                schedule::Action::Pause,
            ]
            .into_iter()
            .map(|action| schedule::ScheduleRow {
                run_id: context.run_id.clone(),
                scenario_id: id.to_owned(),
                target: "server".into(),
                boundary: "EXECUTION_CLAIMED".into(),
                action,
                seed: context.seed,
                deadline_ms: QUEUE_HOLD_DEADLINE_MS,
            })
            .collect();
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
            require(&session.op(&["binding"])?, "BINDING", "client binding")?;
            let declare =
                declare_sealed(&session, &mut events, context.seed, "declare", &[1, 2, 3])?;
            let filler = oracle::dataset(context.seed ^ 0xf111, INPUT_LEN);
            let filler_path = artifacts.join("filler-input.bin");
            fs::write(&filler_path, &filler)?;
            for (domain, work) in [("admit-fill-1", "0:0:1"), ("admit-fill-2", "0:0:2")] {
                admit_input(
                    &session,
                    &mut events,
                    context.seed,
                    domain,
                    &declare,
                    work,
                    &filler_path,
                )?;
            }
            let held_by = Instant::now() + KILL_TIMEOUT;
            loop {
                if subject_record_count(&events_path, "EXECUTION_CLAIMED")? >= 2 {
                    break;
                }
                ensure!(
                    Instant::now() < held_by,
                    "{id}: the two filler claims were not recorded within {KILL_TIMEOUT:?}"
                );
                thread::sleep(Duration::from_millis(25));
            }
            let work = "0:0:3".to_owned();
            let (admit, admitted_at, deadline) = queue_probe_admit(
                &session,
                &mut events,
                context.seed,
                &declare,
                &work,
                &probe_path,
            )?;
            let queued_view = session.watch(&work)?;
            let state_while_queued = state_text(&queued_view)?;
            ensure!(
                parse_field_u64(&queued_view, "deadline")? == Some(deadline),
                "{id}: the queued view's deadline differs from the receipt:\n{queued_view}"
            );
            sleep_until_utc(deadline, QUEUE_PROBE_MARGIN);
            let past_view = session.watch(&work)?;
            let state_past = state_text(&past_view)?;
            let claims_before_release =
                subject_records_for_work(&events_path, "EXECUTION_CLAIMED", &work)?;
            write_release(&events_path, "EXECUTION_CLAIMED")?;
            for filler_work in ["0:0:1", "0:0:2"] {
                watch_until_state(&session, filler_work, 5, RECOVERY_TIMEOUT)?;
            }
            let settlement = queue_probe_settle(
                &session,
                &work,
                admitted_at,
                deadline,
                state_while_queued,
                state_past,
            )?;
            let claims_total = subject_records_for_work(&events_path, "EXECUTION_CLAIMED", &work)?;
            let extra = vec![
                (
                    "probe_claims_before_release",
                    claims_before_release.to_string(),
                ),
                ("probe_claims_total", claims_total.to_string()),
            ];
            (session, settlement, admit, work, extra)
        }
        Subject::Rust => {
            let mut result = None;
            let mut attempts_log = Vec::new();
            for attempt in 1..=QUEUE_LOAD_ATTEMPTS {
                let iteration_dir = scenario_dir.join(format!("iteration-{attempt}"));
                let session = setup_session(context, &iteration_dir, server, client)?;
                let declare = declare_sealed(
                    &session,
                    &mut events,
                    context.seed ^ u64::from(attempt),
                    "declare",
                    &[1, 2, 3],
                )?;
                let load = oracle::dataset(
                    context.seed ^ 0x10ad ^ u64::from(attempt),
                    QUEUE_LOAD_INPUT_LEN,
                );
                let load_path = artifacts.join(format!("load-input-{attempt}.bin"));
                fs::write(&load_path, &load)?;
                // Admitted one after the other (the Rust client journal is
                // locked per file); execution is asynchronous, so both
                // parents occupy alice's two slots while the probe queues.
                for (index, work) in ["0:0:1", "0:0:2"].iter().enumerate() {
                    let op = oracle::operation_hex(oracle::operation_id(
                        context.seed ^ u64::from(attempt),
                        "admit-load",
                        index as u32 + 1,
                    ));
                    let out = session.op(&[
                        "admit",
                        "--operation",
                        &op,
                        "--declaration",
                        &declare,
                        "--work",
                        work,
                        "--input",
                        &crate::path(&load_path),
                        "--application",
                        "chunk-copy/v2",
                        "--mode",
                        "2",
                        "--output-count",
                        "1",
                    ])?;
                    require(&out, "RECEIPT", "load admission")?;
                }
                let work = "0:0:3".to_owned();
                let (admit, admitted_at, deadline) = queue_probe_admit(
                    &session,
                    &mut events,
                    context.seed ^ u64::from(attempt),
                    &declare,
                    &work,
                    &probe_path,
                )?;
                let queued_view = session.watch(&work)?;
                let state_while_queued = state_text(&queued_view)?;
                sleep_until_utc(deadline, QUEUE_PROBE_MARGIN);
                let past_view = session.watch(&work)?;
                let state_past = state_text(&past_view)?;
                let settlement = queue_probe_settle(
                    &session,
                    &work,
                    admitted_at,
                    deadline,
                    state_while_queued,
                    state_past,
                )?;
                attempts_log.push(format!(
                    "attempt {attempt}: queued={} past-deadline={} final={} ({}) diagnostic={}",
                    settlement.state_while_queued,
                    settlement.state_past_deadline_queued,
                    settlement.final_state,
                    state_code_name(&settlement.final_state.to_string()),
                    settlement.diagnostic
                ));
                if settlement.final_state == 6 && view_diagnostic_code(&settlement.view) == Some(11)
                {
                    result = Some((session, settlement, admit, work));
                    break;
                }
                // Inconclusive: the load did not outlast the deadline; let the
                // load settle and try a fresh authority.
                for load_work in ["0:0:1", "0:0:2"] {
                    let _ = watch_until_state(&session, load_work, 5, RECOVERY_TIMEOUT);
                }
                session.server.stop()?;
            }
            let attempts_text = attempts_log.join("; ");
            let Some((session, settlement, admit, work)) = result else {
                observed.push(("load_attempts", attempts_text.clone()));
                observed.push((
                    "row_status",
                    "INCOMPLETE: no hold at EXECUTION_CLAIMED on the Rust subject and the \
                     load queue did not outlast the probe's deadline"
                        .into(),
                ));
                write_kv(scenario_dir, "observed.tsv", &observed)?;
                seal(context, scenario_dir, id, events)?;
                return Err(MissingCapability(format!(
                    "the Rust subject's fixture hooks cannot pause at EXECUTION_CLAIMED \
                     (src/v2/fixture.rs REPLY_PAIRS) and the hook-free load queue did not \
                     outlast the {QUEUE_PROBE_EXECUTION_MS} ms deadline in \
                     {QUEUE_LOAD_ATTEMPTS} fresh authorities ({attempts_text})"
                ))
                .into());
            };
            let extra = vec![
                ("load_attempts", attempts_text),
                (
                    "probe_claims_total",
                    "not observable (the Rust subject emits only armed boundaries)".into(),
                ),
            ];
            (session, settlement, admit, work, extra)
        }
    };

    // The property: settled by the deadline, never executed to success.
    fs::write(artifacts.join("probe-terminal-view.txt"), &settlement.view)?;
    if settlement.final_state == 5 {
        observed.push((
            "probe_outcome",
            format!(
                "SUCCEEDED(5) after its deadline passed in the queue (admitted_at {} deadline \
                 {} queued state {} past-deadline state {})",
                settlement.admitted_at,
                settlement.deadline,
                settlement.state_while_queued,
                settlement.state_past_deadline_queued
            ),
        ));
        observed.extend(extra);
        write_kv(scenario_dir, "observed.tsv", &observed)?;
        seal(context, scenario_dir, id, events)?;
        bail!(
            "{id}: the {} subject executed the queued probe to SUCCEEDED after its deadline \
             ({}) had passed while queued; candidate subject defect (queue time not counted \
             against the execution deadline); see observed.tsv",
            server.name(),
            settlement.deadline
        );
    }
    ensure!(
        settlement.final_state == 6 && view_diagnostic_code(&settlement.view) == Some(11),
        "{id}: the queued probe must settle FAILED(6) with DEADLINE_EXCEEDED (11), got state \
         {} diagnostic {}\n{}",
        settlement.final_state,
        settlement.diagnostic,
        settlement.view
    );
    if let Some(terminal_at) = settlement.terminal_at {
        ensure!(
            terminal_at >= settlement.deadline,
            "{id}: the probe settled at {terminal_at}, before its deadline {}",
            settlement.deadline
        );
    }
    let retry = oracle::operation_hex(oracle::operation_id(context.seed, "retry-probe", 1));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&retry)?),
        Some(&work),
        Some(1),
        None,
        None,
    )?;
    let retry_text = expect_failure(
        &session,
        &artifacts,
        "probe-retry-refusal.txt",
        &[
            "retry",
            "--operation",
            &retry,
            "--work",
            &work,
            "--expected-attempt",
            "1",
        ],
    )?;
    let retry_line = refusal_named_line(&retry_text, &["DEADLINE_EXCEEDED", "ALREADY_TERMINAL"])
        .with_context(|| {
            format!(
                "{id}: retry after the queued deadline settlement must name DEADLINE_EXCEEDED \
                 (11) or ALREADY_TERMINAL (18)\n{retry_text}"
            )
        })?;
    let retry_code =
        transcript_named_code(&retry_text).context("retry refusal carries no named code")?;
    events.append(
        "",
        Some(hex_to_id(&retry)?),
        Some(&work),
        Some(1),
        Some(retry_code),
        None,
    )?;
    let persisted = session.watch(&work)?;
    ensure!(
        view_line(&persisted)? == view_line(&settlement.view)?,
        "{id}: the retry refusal changed the settled view:\n{persisted}\noriginal:\n{}",
        settlement.view
    );
    let read_outcome =
        expect_named_read_refusal(&session, &artifacts, &work, "probe-read-refusal.txt")?;
    let _ = admit;
    detach(&session)?;
    observed.extend([
        ("probe_admitted_at", settlement.admitted_at.to_string()),
        ("probe_deadline", settlement.deadline.to_string()),
        (
            "deadline_accounting",
            format!(
                "receipt deadline = admitted_at + {QUEUE_PROBE_EXECUTION_MS} ms (queue time \
                 counted from admission)"
            ),
        ),
        (
            "probe_state_while_queued",
            settlement.state_while_queued.clone(),
        ),
        (
            "probe_state_past_deadline_still_queued",
            settlement.state_past_deadline_queued.clone(),
        ),
        (
            "probe_outcome",
            format!(
                "{} ({}) diagnostic {}",
                settlement.final_state,
                state_code_name(&settlement.final_state.to_string()),
                settlement.diagnostic
            ),
        ),
        (
            "probe_terminal_at",
            settlement
                .terminal_at
                .map(|at| at.to_string())
                .unwrap_or_else(|| "not rendered".into()),
        ),
        ("retry_after_settlement", retry_line),
        ("read_after_settlement", read_outcome),
    ]);
    observed.extend(extra);
    if server == Subject::Java || client == Subject::Java {
        let jar = context
            .java_jar
            .as_ref()
            .expect("a Java direction requires --java-jar");
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    stop_and_seal(context, scenario_dir, id, session.server, events)
}

// ---------------------------------------------------------------------------
// Rows that run what they can and report the subject capability they lack
// ---------------------------------------------------------------------------

/// Try to start a server armed with `rows`; a subject that cannot honour the
/// schedule refuses it at parse time and exits before readiness. Returns the
/// start error text (the subject's own refusal) or `None` if the server came
/// up, in which case it is stopped again.
fn subject_schedule_refusal(
    context: &ScenarioContext,
    direction_dir: &Path,
    id: &str,
    server: Subject,
    rows: &[schedule::ScheduleRow],
) -> Result<Option<String>> {
    fs::create_dir_all(direction_dir)?;
    let certs = mtls::generate(&direction_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(
        &context.rust_bin,
        context.java_jar.as_deref(),
        &direction_dir.join("subject"),
        certs,
        server,
        Subject::Rust,
    )?;
    fixture.run_init_authority()?;
    let schedule_path = direction_dir.join("schedule.tsv");
    fs::write(&schedule_path, schedule::render(rows)?)?;
    let arming = crate::durable::process::FixtureArming {
        events: direction_dir.join("subject-events.tsv"),
        run_id: context.run_id.clone(),
        scenario_id: id.to_owned(),
        schedule: Some(schedule_path),
    };
    match fixture.start_server_armed(Some(&arming), true) {
        Ok(server) => {
            server.stop()?;
            Ok(None)
        }
        Err(error) => Ok(Some(format!("{error:#}"))),
    }
}

/// Run one capability probe per server subject: the rust subject in the row
/// directory, the java subject in `java-server` when a jar is present (an
/// INCOMPLETE marker otherwise). Returns the per-subject findings.
fn per_server_findings(
    context: &ScenarioContext,
    scenario_dir: &Path,
    probe: impl Fn(&Path, Subject) -> Result<String>,
) -> Result<Vec<(Subject, String)>> {
    let mut findings = vec![(Subject::Rust, probe(scenario_dir, Subject::Rust)?)];
    let java_dir = scenario_dir.join("java-server");
    fs::create_dir_all(&java_dir)?;
    if context.java_jar.is_none() {
        fs::write(
            java_dir.join("INCOMPLETE"),
            b"no --java-jar provided; this direction was not run\n",
        )?;
    } else {
        findings.push((Subject::Java, probe(&java_dir, Subject::Java)?));
    }
    Ok(findings)
}

/// Write the row's expected/observed files for a missing-capability row and
/// seal its events; the caller returns the MissingCapability.
fn finish_missing_capability(
    context: &ScenarioContext,
    scenario_dir: &Path,
    id: &str,
    events: EventWriter,
    expected: &[(&str, String)],
    findings: &[(Subject, String)],
    reason: &str,
) -> Result<()> {
    write_kv(scenario_dir, "expected.tsv", expected)?;
    let mut observed: Vec<(&str, String)> = vec![("row_status", format!("INCOMPLETE: {reason}"))];
    for (subject, finding) in findings {
        observed.push((
            Box::leak(format!("{}_server", subject.name()).into_boxed_str()),
            finding.clone(),
        ));
    }
    if let Some(jar) = &context.java_jar {
        observed.push(("java_jar_sha256", oracle::sha256_hex(&fs::read(jar)?)));
    }
    write_kv(scenario_dir, "observed.tsv", &observed)?;
    seal(context, scenario_dir, id, events)
}

/// First line of a subject's schedule refusal that names the reason.
fn refusal_reason_line(text: &str) -> String {
    text.lines()
        .find(|line| {
            line.contains("drop-reply")
                || line.contains("clock-set")
                || line.contains("pause")
                || line.contains("schedule")
        })
        .unwrap_or_else(|| text.lines().next().unwrap_or(""))
        .trim()
        .to_owned()
}

/// g7-unsafe-clock-refusal: needs a subject fixture clock (`clock-set`) or an
/// untrusted-clock mode. Neither subject has one: the only clock control on
/// either `serve` is `--trust-system-clock`, the Rust hooks reject
/// `clock-set` as a driver-side action, and Claude's FixtureMain does not
/// parse it. The row records both usages and both schedule refusals; host
/// UTC is never changed.
fn g7_unsafe_clock_refusal(context: &ScenarioContext) -> Result<()> {
    let id = "g7-unsafe-clock-refusal";
    let (scenario_dir, _artifacts) = open_scenario(context, id)?;
    let events = open_events(context, &scenario_dir, id, Subject::Rust)?;
    let findings = per_server_findings(context, &scenario_dir, |dir, server| {
        let artifacts = dir.join("artifacts");
        fs::create_dir_all(&artifacts)?;
        // The subject's own usage text: every clock-related flag it offers.
        let usage_command: Vec<String> = match server {
            Subject::Rust => vec![
                crate::path(&context.rust_bin),
                "v2".into(),
                "serve".into(),
                "--help".into(),
            ],
            Subject::Java => {
                let jar = context
                    .java_jar
                    .as_ref()
                    .context("java usage needs a jar")?;
                vec![
                    "java".into(),
                    "-cp".into(),
                    crate::path(jar),
                    "ai.pipestream.quic.v2.V2Main".into(),
                ]
            }
        };
        let usage = crate::run_output_owned(dir, &usage_command, OP_WAIT)?;
        let usage_text = format!(
            "{}\n{}",
            String::from_utf8_lossy(&usage.stdout),
            String::from_utf8_lossy(&usage.stderr)
        );
        fs::write(artifacts.join("serve-usage.txt"), &usage_text)?;
        let clock_flags: Vec<&str> = usage_text
            .split(|ch: char| ch.is_whitespace() || ch == '[' || ch == ']')
            .filter(|token| token.contains("clock"))
            .collect();
        let rows = [g2_schedule_row(
            context,
            id,
            "ADMISSION_COMMITTED",
            schedule::Action::ClockSet,
        )];
        let refusal = subject_schedule_refusal(context, dir, id, server, &rows)?;
        let refusal_text = match refusal {
            Some(text) => {
                fs::write(artifacts.join("subject-schedule-refusal.txt"), &text)?;
                format!(
                    "clock-set refused at schedule parse: {}",
                    refusal_reason_line(&text)
                )
            }
            None => "ACCEPTED clock-set (the row must be implemented against this subject)".into(),
        };
        Ok(format!(
            "clock flags in usage: {}; {refusal_text}",
            if clock_flags.is_empty() {
                "none".to_owned()
            } else {
                clock_flags.join(",")
            }
        ))
    })?;
    let reason = "no subject fixture clock: the only clock control on either serve is \
                  --trust-system-clock, both subjects refuse clock-set at schedule parse, \
                  and host UTC is never changed by this driver";
    finish_missing_capability(
        context,
        &scenario_dir,
        id,
        events,
        &[
            (
                "would_do",
                "regress the subject's clock, then attempt admission, retry, publication and \
                 destructive expiry"
                    .into(),
            ),
            (
                "would_assert",
                "fresh promises refuse CLOCK_UNSAFE (17); read-only retrieval continues; no \
                 destructive expiry under unsafe time; after recovery the persisted watermark \
                 still refuses back-dated commits"
                    .into(),
            ),
            (
                "missing_capability",
                "a subject fixture clock (interface-v1 clock-set) or an untrusted-clock mode on \
                 both subjects"
                    .into(),
            ),
        ],
        &findings,
        reason,
    )?;
    Err(MissingCapability(reason.to_owned()).into())
}

/// g7-cleanup-interrupted-refund: kill the server during terminal cleanup
/// (after output expiry, before accounting reconciliation finishes). The
/// interface-v1 boundary vocabulary has no cleanup boundary: CLOSURE_COMMITTED
/// is scope closure, and the Rust subject's retention and retirement commit
/// keys are supplementary probes that cannot be armed (src/v2/fixture.rs
/// commit_label). The row records the vocabulary and the nearest boundaries
/// on both subjects.
fn g7_cleanup_interrupted_refund(context: &ScenarioContext) -> Result<()> {
    let id = "g7-cleanup-interrupted-refund";
    let (scenario_dir, artifacts) = open_scenario(context, id)?;
    let events = open_events(context, &scenario_dir, id, Subject::Rust)?;
    let boundaries = schedule::BOUNDARIES.join("\n");
    fs::write(
        artifacts.join("interface-v1-boundaries.txt"),
        format!("{boundaries}\n"),
    )?;
    let cleanup_labels: Vec<&&str> = schedule::BOUNDARIES
        .iter()
        .filter(|label| {
            label.contains("CLEANUP") || label.contains("RETIRE") || label.contains("REFUND")
        })
        .collect();
    let findings = vec![
        (
            Subject::Rust,
            "no armable cleanup boundary: retention-* and retirement-* commit keys are \
             supplementary and unarmable (src/v2/fixture.rs commit_label); nearest armable \
             boundary CLOSURE_COMMITTED is scope closure, not cleanup"
                .to_owned(),
        ),
        (
            Subject::Java,
            "no cleanup boundary in Boundaries.Boundary (FixtureMain accepts only interface-v1 \
             labels); nearest CLOSURE_COMMITTED is scope closure, not cleanup"
                .to_owned(),
        ),
    ];
    let reason = format!(
        "interface-v1 section 2.1 has no cleanup boundary ({} matching labels among {}); a kill \
         during terminal cleanup cannot be armed on either subject without a new boundary",
        cleanup_labels.len(),
        schedule::BOUNDARIES.len()
    );
    finish_missing_capability(
        context,
        &scenario_dir,
        id,
        events,
        &[
            (
                "would_do",
                "kill the server during terminal cleanup after output expiry, before accounting \
                 reconciliation finishes; restart"
                    .into(),
            ),
            (
                "would_assert",
                "cleanup is replayable; reserved capacity is never refunded while a dependent \
                 read-pin or callback retains it; the restart completes the refund exactly once \
                 (capacity accounting before/after recorded; double refund is a defect)"
                    .into(),
            ),
            (
                "missing_capability",
                "an interface-v1 cleanup boundary (an interface revision proposal) implemented \
                 by both subjects"
                    .into(),
            ),
        ],
        &findings,
        &reason,
    )?;
    Err(MissingCapability(reason).into())
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
        assert!(rows.len() >= 46);
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
            "g3-terminal-cleanup",
            "g3-partial-retirement",
            "g3-nonreusable-history",
            "g7-receipt-before-output-expiry",
            "g7-output-before-receipt-expiry",
            "g7-no-deadline-extension",
            "g4-publication-vs-cancel",
            "g4-publication-vs-skip",
            "g4-stale-attempt-retry",
            "g4-ancestor-fence-publication",
            "g4-deadline-settlement",
            "g4-revocation-vs-publication",
            "g4-eventual-settlement",
            "g5-untrusted-identity",
            "g5-missing-client-cert",
            "g5-unmapped-principal",
            "g5-foreign-owner",
            "g5-no-existence-disclosure",
            "g8-exact-root-complete",
            "g8-child-cut-conflict",
            "g8-complete-with-pending",
            "g8-detach-drains",
            "g8-half-close-preserves-responses",
            "g8-timeout-no-completion-claim",
            "g6-canonical-violations",
            "g6-direction-and-correlation",
            "g6-stream-identity-and-fin",
            "g6-stopped-control-and-transfers",
        ] {
            let row = rows.iter().find(|row| row.id == id).unwrap();
            assert!(row.rust_implemented, "{id} must be implemented");
        }
        assert_eq!(rows.iter().filter(|row| row.rust_implemented).count(), 66);
    }

    /// The owner's 2026-09-13 decision (coordinator board): publication is
    /// observed through a watch on both subjects, not a correlated reply
    /// pair, so a withheld reply that does not exist cannot be lost; the
    /// kill variant g2-kill-at-publication-commit is the boundary's
    /// evidence. The row is retired from the matrix (66 of 67 rows remain);
    /// the decision is recorded in scenario-matrix-g2.md and handoff.md §3k.
    #[test]
    fn drop_reply_publication_is_retired_from_the_matrix() {
        let rows = rows();
        assert!(
            rows.iter().all(|row| row.id != "g2-drop-reply-publication"),
            "g2-drop-reply-publication must not be registered: the row is retired"
        );
        assert!(
            rows.iter()
                .any(|row| row.id == "g2-kill-at-publication-commit"),
            "the kill variant remains the boundary evidence"
        );
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
        // The Rust hooks poll release-<BOUNDARY>; the Java FixtureMain polls
        // release-<target>-<BOUNDARY> with target `server`. A release the
        // driver writes must reach whichever subject is paused.
        assert!(dir.join("release-SESSION_COMMITTED").is_file());
        assert!(dir.join("release-server-SESSION_COMMITTED").is_file());
        // Clearing removes both so a later pause on the same boundary holds
        // again; clearing an absent release is not an error.
        clear_release(&events, "SESSION_COMMITTED").unwrap();
        assert!(!dir.join("release-SESSION_COMMITTED").exists());
        assert!(!dir.join("release-server-SESSION_COMMITTED").exists());
        clear_release(&events, "SESSION_COMMITTED").unwrap();
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn stale_retry_hold_pauses_the_second_execution_claim_on_java_only() {
        let context = ScenarioContext {
            run_id: "run-a".into(),
            run_root: std::env::temp_dir(),
            seed: 7,
            rust_bin: PathBuf::from("/nonexistent/pipestream-quinn"),
            java_jar: None,
        };
        // The Java FixtureMain consumes one pause row per reached boundary,
        // so attempt 1 takes the first pause and attempt 2 the second; the
        // release between them is the interface-v1 ordering rule, and the
        // rows must validate under the driver-side schedule checks.
        let java = stale_retry_hold_rows(&context, "g4-stale-attempt-retry", Subject::Java);
        assert_eq!(
            java.iter().map(|row| row.action).collect::<Vec<_>>(),
            vec![
                schedule::Action::Pause,
                schedule::Action::Release,
                schedule::Action::Pause
            ]
        );
        assert!(java.iter().all(|row| row.boundary == "EXECUTION_CLAIMED"
            && row.target == "server"
            && row.deadline_ms == STALE_HOLD_DEADLINE_MS));
        schedule::validate(&java).unwrap();
        let rendered = schedule::render(&java).unwrap();
        assert_eq!(
            schedule::parse(&rendered, "run-a", "g4-stale-attempt-retry").unwrap(),
            java
        );
        // The Rust subject rejects a pause outside its reply pairs, so its
        // direction gets no rows and names the gap instead of arming one.
        assert!(
            stale_retry_hold_rows(&context, "g4-stale-attempt-retry", Subject::Rust).is_empty()
        );
        assert!(STALE_RETRY_RUST_HOLD_GAP.contains("REPLY_PAIRS"));
    }

    #[test]
    fn changed_policy_probe_spells_the_execution_flag_per_client() {
        // The Java client ignores an unknown --max-execution-ms and would
        // replay the default policy, turning a changed-policy probe into an
        // identical replay that the server rightly answers with the binding.
        assert_eq!(execution_limit_flag(Subject::Java), "--execution-ms");
        assert_eq!(execution_limit_flag(Subject::Rust), "--max-execution-ms");
        assert_eq!(policy_args(Subject::Java, 1, 2, 3)[0], "--execution-ms");
        assert_eq!(policy_args(Subject::Rust, 1, 2, 3)[0], "--max-execution-ms");
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
        // The Rust admission receipt renders the same field without Some().
        let receipt = "RECEIPT OperationReceipt { body: Admitted { attempt: Id(1), \
                       admitted_at: Number(1789034836504), deadline: Number(1789034896504), \
                       child: None } }";
        assert_eq!(
            parse_field_u64(receipt, "deadline").unwrap(),
            Some(1789034896504)
        );
        assert_eq!(
            parse_field_u64(receipt, "admitted_at").unwrap(),
            Some(1789034836504)
        );
        // The Java client prints Records.WorkView as a Java record: the same
        // field reads deadline=N, an absent optional reads deadline=null, and
        // the snake_case names are camelCase there.
        let java = "WORK revision=4 state=5 attempt=1 child=none\n\
                    VIEW WorkView[work=WorkKey[scope=0, producer=0, entity=1], state=SUCCEEDED, \
                    attempt=1, input=Input[length=65536, sha256=Digest[bytes=[1, 2]], \
                    contentType=application/octet-stream], admittedAt=1789034836504, \
                    deadline=1789034896504, terminalAt=1789034836900, \
                    receiptUntil=1789121236900, outputUntil=1789038436900, child=null, \
                    manifest=Manifest[work=WorkKey[scope=0, producer=0, entity=1], attempt=1], \
                    diagnostic=null]";
        assert_eq!(
            parse_field_u64(java, "deadline").unwrap(),
            Some(1789034896504)
        );
        assert_eq!(
            parse_field_u64(java, "admitted_at").unwrap(),
            Some(1789034836504)
        );
        assert_eq!(
            parse_field_u64(java, "terminal_at").unwrap(),
            Some(1789034836900)
        );
        assert_eq!(
            parse_field_u64(java, "receipt_until").unwrap(),
            Some(1789121236900)
        );
        assert_eq!(
            parse_field_u64(java, "output_until").unwrap(),
            Some(1789038436900)
        );
        assert_eq!(parse_field_u64(java, "attempt").unwrap(), Some(1));
        let java_pending = "WORK revision=2 state=1 attempt=1 child=none\n\
                            VIEW WorkView[work=WorkKey[scope=0, producer=0, entity=1], \
                            state=ACTIVE, attempt=1, admittedAt=1789034836504, \
                            deadline=1789034896504, terminalAt=null, receiptUntil=null, \
                            outputUntil=null, child=null, manifest=null, diagnostic=null]";
        assert_eq!(
            parse_field_u64(java_pending, "deadline").unwrap(),
            Some(1789034896504)
        );
        assert_eq!(parse_field_u64(java_pending, "terminal_at").unwrap(), None);
        assert_eq!(
            parse_field_u64(java_pending, "receipt_until").unwrap(),
            None
        );
        // A field name inside another identifier is not that field.
        assert_eq!(
            parse_field_u64("VIEW X[xdeadline=5, other=1]", "deadline").unwrap(),
            None
        );
        assert!(parse_field_u64("VIEW X[deadline=soon]", "deadline").is_err());
        assert_eq!(snake_to_camel("receipt_until"), "receiptUntil");
        assert_eq!(snake_to_camel("deadline"), "deadline");
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
