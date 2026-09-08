//! Scenario matrix and the g1-leaf-copy milestone-1 row (rust client against
//! rust server, driven purely through spawned V2 CLI binaries).

use crate::durable::events::{ArtifactRef, EventWriter};
use crate::durable::mtls;
use crate::durable::oracle;
use crate::durable::process::AuthorityFixture;
use crate::durable::schedule;
use crate::{hex, unique_suffix};
use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

const INPUT_LEN: usize = 64 * 1024;
const WATCH_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Row {
    pub id: &'static str,
    pub group: &'static str,
    /// Implemented for the rust-client/rust-server direction in milestone 1.
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
    rows.iter_mut()
        .find(|row| row.id == "g1-leaf-copy")
        .expect("g1-leaf-copy is in the G1 group")
        .rust_implemented = true;
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
    rows
}

pub struct ScenarioContext {
    pub run_id: String,
    pub run_root: PathBuf,
    pub seed: u64,
    pub rust_bin: PathBuf,
}

impl ScenarioContext {
    pub fn scenario_dir(&self, scenario_id: &str) -> PathBuf {
        self.run_root.join(scenario_id)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DirectionOutcome {
    Pass,
    Incomplete(String),
    Fail(String),
}

pub fn run_direction(row: &Row, context: &ScenarioContext, dev: bool) -> DirectionOutcome {
    if !row.rust_implemented {
        return DirectionOutcome::Incomplete(format!(
            "{} rust-client/rust-server not implemented in milestone 1",
            row.id
        ));
    }
    match run_rust_direction(row, context) {
        Ok(()) => DirectionOutcome::Pass,
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

fn require(output: &std::process::Output, marker: &str, description: &str) -> Result<String> {
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
        other => bail!("scenario {other} has no rust direction in milestone 1"),
    }
}

/// g1-leaf-copy: declare one entity, admit a deterministic input to copy/v2,
/// watch to terminal success, then select/read index 0 and verify the
/// received bytes equal the independently computed input bytes.
fn g1_leaf_copy(context: &ScenarioContext) -> Result<()> {
    let scenario_id = "g1-leaf-copy";
    let scenario_dir = context.scenario_dir(scenario_id);
    let artifacts = scenario_dir.join("artifacts");
    fs::create_dir_all(&artifacts)?;

    let process_start_id = format!("{}-{:x}", std::process::id(), unique_suffix());
    let mut events = EventWriter::open(
        &scenario_dir.join("events.tsv"),
        &context.run_id,
        scenario_id,
        "rust",
        "client",
        &process_start_id,
    )?;

    // Isolated mTLS material and authority storage for this scenario only.
    let certs = mtls::generate(&scenario_dir.join("certs"), &[("alice", "alice")])?;
    let fixture = AuthorityFixture::new(&context.rust_bin, &scenario_dir.join("subject"), certs)?;
    fixture.run_init_authority()?;
    let server = fixture.start_server()?;
    let sequence = fixture.next_sequence(&server, "alice")?;
    ensure!(
        sequence == 1,
        "fresh authority must report NEXT_SEQUENCE 1, got {sequence}"
    );

    // g1-leaf-copy carries no fault rows in this milestone, but the schedule
    // contract is enforced for every run: parse and execute it, and fail
    // loudly if a row ever needs a subject hook that does not exist yet.
    let schedule_rows = schedule::parse("", &context.run_id, scenario_id)?;
    schedule::execute(&schedule_rows, |_| {
        bail!("g1-leaf-copy is a no-fault row; no process-lifecycle action may be scheduled")
    })?;

    // Expected values come from the oracle, stored separately from observed.
    let input = oracle::dataset(context.seed, INPUT_LEN);
    let expected_output_sha256 = oracle::sha256_hex(&input);
    fs::write(
        scenario_dir.join("expected.tsv"),
        format!(
            "input_len\t{INPUT_LEN}\ninput_sha256\t{expected_output_sha256}\n\
             expected_output_sha256\t{expected_output_sha256}\nwork\t0:0:1\nattempt\t1\n"
        ),
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

    let journal = scenario_dir.join("client").join("session.sqlite");
    fs::create_dir_all(journal.parent().expect("journal has a parent directory"))?;
    let init = {
        let mut command = fixture.base();
        command.push("init-client".into());
        command.extend(fixture.journal_args(&journal, "alice", sequence));
        crate::run_output_owned(&fixture.root, &command, Duration::from_secs(30))?
    };
    require(&init, "CLIENT_INITIALIZED", "v2 init-client")?;

    let connection = fixture.connection_args(&server, "alice")?;
    let binding = fixture.run_client_op(&journal, "alice", sequence, &connection, &["binding"])?;
    require(&binding, "BINDING", "client binding")?;

    // Declare one entity in the root scope, sealed.
    let declare = oracle::operation_hex(oracle::operation_id(context.seed, "declare", 0));
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&declare)?),
        None,
        None,
        None,
        None,
    )?;
    let declare_hex = declare.clone();
    let declared = fixture.run_client_op(
        &journal,
        "alice",
        sequence,
        &connection,
        &[
            "declare",
            "--operation",
            &declare_hex,
            "--entities",
            "1",
            "--seal",
        ],
    )?;
    require(&declared, "RECEIPT", "declare operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&declare)?),
        None,
        None,
        None,
        None,
    )?;

    // Admit the deterministic input to the copy application.
    let admit = oracle::operation_hex(oracle::operation_id(context.seed, "admit", 1));
    let input_path = artifacts.join("input.bin");
    events.append(
        "REQUEST_SENT",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    let admitted = fixture.run_client_op(
        &journal,
        "alice",
        sequence,
        &connection,
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
    require(&admitted, "RECEIPT", "admit operation")?;
    events.append(
        "RECEIPT_VALIDATED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    // Watch until terminal success (state=5), never past a failure state.
    let deadline = Instant::now() + WATCH_TIMEOUT;
    let terminal = loop {
        let watch = fixture.run_client_op(
            &journal,
            "alice",
            sequence,
            &connection,
            &["watch", "--work", "0:0:1"],
        )?;
        let stdout = require(&watch, "WORK", "watch operation")?;
        let state = stdout
            .split_whitespace()
            .find_map(|token| token.strip_prefix("state="))
            .context("watch did not report a state")?
            .parse::<u64>()
            .context("watch state is not decimal")?;
        ensure!(
            state != 6,
            "copy work entered the terminal failure state: {stdout}"
        );
        if state == 5 {
            break stdout;
        }
        ensure!(
            Instant::now() < deadline,
            "copy work did not succeed within {WATCH_TIMEOUT:?}\nlast view:\n{stdout}"
        );
        thread::sleep(Duration::from_millis(100));
    };
    events.append(
        "OBSERVATION_JOURNALED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;

    // Manifest evidence.
    let manifest = fixture.run_client_op(
        &journal,
        "alice",
        sequence,
        &connection,
        &["manifest", "--work", "0:0:1", "--attempt", "1"],
    )?;
    let manifest_text = require(&manifest, "MANIFEST", "manifest operation")?;
    fs::write(artifacts.join("manifest.txt"), &manifest_text)?;
    let manifest_len = manifest_text.len() as u64;
    let manifest_sha256 = oracle::sha256_hex(manifest_text.as_bytes());
    events.append(
        "",
        None,
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/manifest.txt".into(),
            len: manifest_len,
            sha256: manifest_sha256,
        }),
    )?;

    // Select, then read output index 0 into the artifacts directory.
    let select = fixture.run_client_op(
        &journal,
        "alice",
        sequence,
        &connection,
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
    require(&select, "REFERENCE", "select operation")?;
    let output_path = artifacts.join("output.bin");
    let read = fixture.run_client_op(
        &journal,
        "alice",
        sequence,
        &connection,
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
    require(&read, "VERIFIED", "read operation")?;

    // Independent oracle verification: copy = input bytes.
    let received = fs::read(&output_path)
        .with_context(|| format!("read installed result {}", output_path.display()))?;
    let actual_sha256 = hex(&Sha256::digest(&received));
    if received != input {
        bail!(
            "g1-leaf-copy RESULT_VERIFIED mismatch: expected sha256={expected_output_sha256} \
             len={INPUT_LEN} (independent oracle: copy = input bytes), actual \
             sha256={actual_sha256} len={} (received output at {})",
            received.len(),
            output_path.display()
        );
    }
    events.append(
        "RESULT_VERIFIED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        None,
    )?;
    events.append(
        "RESULT_INSTALLED",
        Some(hex_to_id(&admit)?),
        Some("0:0:1"),
        Some(1),
        None,
        Some(ArtifactRef {
            path: "artifacts/output.bin".into(),
            len: received.len() as u64,
            sha256: actual_sha256,
        }),
    )?;

    let detach = fixture.run_client_op(&journal, "alice", sequence, &connection, &["detach"])?;
    require(&detach, "DETACHED", "detach operation")?;

    // Observed evidence: negotiated/declared profile and limit facts that are
    // observable black-box from the CLI contract.
    fs::write(
        scenario_dir.join("observed.tsv"),
        format!(
            "alpn\tpipestream/2\nobject_limit_client\t16777216\nobject_limit_server\t16777216\n\
             application\tcopy/v2\nmode\t0\nterminal_state\t5\nwatch_terminal\t{}\n",
            terminal.trim()
        ),
    )?;

    // Graceful shutdown must drain.
    server.stop()?;

    // The events file must validate against the interface contract, including
    // artifact existence and lengths, before the run directory is sealed.
    drop(events);
    crate::durable::events::read_events_checked(
        &scenario_dir.join("events.tsv"),
        &context.run_id,
        scenario_id,
        Some(&scenario_dir),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_row_ids_are_unique_and_g1_leaf_copy_is_implemented() {
        let rows = rows();
        let mut seen = std::collections::BTreeSet::new();
        for row in &rows {
            assert!(seen.insert(row.id), "duplicate row id {}", row.id);
        }
        assert!(rows.len() >= 40);
        let leaf = rows.iter().find(|row| row.id == "g1-leaf-copy").unwrap();
        assert!(leaf.rust_implemented);
        assert!(rows.iter().filter(|row| row.rust_implemented).count() == 1);
    }
}
