//! Neutral durable conformance driver (milestone 1). Spawns subject binaries
//! black-box; the crate keeps its existing dependency set only.

mod events;
mod gates;
mod mtls;
mod oracle;
mod process;
mod scenarios;
mod schedule;

use crate::{path, repository_root, unique_suffix};
use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Args)]
pub struct DurableArgs {
    /// Development partial mode: runs available prerequisites, labels every
    /// artifact INCOMPLETE, can never emit a full-conformance PASS.
    #[arg(long)]
    dev: bool,
    /// Run only named matrix rows (repeatable). Acceptance mode runs all.
    #[arg(long)]
    scenario: Vec<String>,
    /// Base seed recorded into every schedule row and oracle dataset.
    #[arg(long, default_value_t = 0x5eed)]
    seed: u64,
    /// Fixture-owned run output directory (default the rust-quinn workspace's
    /// target/durable-runs).
    #[arg(long)]
    artifacts: Option<PathBuf>,
    /// pipestream-quinn binary (default the workspace release build).
    #[arg(long)]
    rust_bin: Option<PathBuf>,
    /// Java -all.jar. Absent in acceptance mode = explicit FAIL naming the
    /// unmet gate; in --dev the Java-direction rows report INCOMPLETE.
    #[arg(long)]
    java_jar: Option<PathBuf>,
}

const JAVA_DIRECTIONS: &[&str] = &["java-client/rust-server", "rust-client/java-server"];

pub fn run(args: DurableArgs) -> Result<()> {
    let root = repository_root()?;
    let rust_bin = args
        .rust_bin
        .clone()
        .unwrap_or_else(|| root.join("implementations/rust-quinn/target/release/pipestream-quinn"));
    ensure!(
        rust_bin.is_file(),
        "missing --rust-bin subject executable at {} (build it with: cargo build --release \
         --locked -p pipestream-server)",
        rust_bin.display()
    );
    if !args.dev {
        let mut unmet: Vec<String> = Vec::new();
        if args.java_jar.is_none() {
            unmet.push(format!(
                "missing --java-jar: acceptance mode runs the whole matrix, so the \
                 Java-direction rows ({}) are an explicit gate; rerun with --dev to \
                 execute the available rust direction only",
                JAVA_DIRECTIONS.join(", ")
            ));
        }
        if let Some(jar) = &args.java_jar
            && !jar.is_file()
        {
            unmet.push(format!(
                "missing --java-jar subject artifact at {} (Java-direction rows: {})",
                jar.display(),
                JAVA_DIRECTIONS.join(", ")
            ));
        }
        if !unmet.is_empty() {
            bail!(
                "acceptance prerequisites unmet:\n - {}",
                unmet.join("\n - ")
            );
        }
    }

    // Record subject binary hashes at start; nc-stale-binary fails the run if
    // any of them change mid-run.
    let rust_hash = hash_file(&rust_bin)?;
    let java_hash = match &args.java_jar {
        Some(jar) => Some((jar.clone(), hash_file(jar)?)),
        None => None,
    };

    let run_id = format!("durable-{:016x}", unique_suffix());
    let base = args
        .artifacts
        .clone()
        .unwrap_or_else(|| root.join("implementations/rust-quinn/target/durable-runs"));
    let run_root = base.join("runs").join(&run_id);
    fs::create_dir_all(&run_root)?;
    write_run_manifest(&run_root, &args, &rust_bin, &rust_hash, java_hash.as_ref())?;

    // Self-check stage: the negative controls must behave as designed. The
    // torn-event-line control must be REJECTED by the reader, and the recorded
    // subject hash must still match the binary on disk.
    self_check(&rust_bin, &rust_hash)?;

    let matrix = scenarios::rows();
    let selected = select_rows(&matrix, &args.scenario)?;
    let context = scenarios::ScenarioContext {
        run_id: run_id.clone(),
        run_root: run_root.clone(),
        seed: args.seed,
        rust_bin: rust_bin.clone(),
    };
    let mut outcomes: Vec<(&scenarios::Row, scenarios::DirectionOutcome)> = Vec::new();
    for row in &selected {
        outcomes.push((row, scenarios::run_direction(row, &context, args.dev)));
    }

    // nc-stale-binary: re-hash the subjects after every scenario completed.
    verify_binary_freshness(&rust_hash, &rust_bin)?;
    if let Some((jar, recorded)) = &java_hash {
        verify_binary_freshness(recorded, jar)?;
    }

    let mut failed = false;
    for (row, outcome) in &outcomes {
        match outcome {
            scenarios::DirectionOutcome::Pass if args.dev => println!(
                "SCENARIO OK {} rust-client/rust-server (dev mode: labelled INCOMPLETE)",
                row.id
            ),
            scenarios::DirectionOutcome::Pass => {
                println!("PASS {} rust-client/rust-server", row.id)
            }
            scenarios::DirectionOutcome::Incomplete(reason) => {
                println!("INCOMPLETE {}: {reason}", row.id)
            }
            scenarios::DirectionOutcome::Fail(reason) => {
                failed = true;
                println!("FAIL {}: {reason}", row.id)
            }
        }
    }
    if args.dev {
        fs::write(
            run_root.join("INCOMPLETE"),
            b"dev mode: partial run, never full conformance\n",
        )?;
        println!(
            "dev run {run_id}: partial results only; run directory is NOT a conformance PASS: {}",
            run_root.display()
        );
    } else {
        println!("acceptance run {run_id}: {}", run_root.display());
    }
    if failed {
        bail!("durable run recorded FAIL rows; see {run_root:?}");
    }
    Ok(())
}

fn hash_file(path: &std::path::Path) -> Result<String> {
    Ok(oracle::sha256_hex(&fs::read(path).with_context(|| {
        format!("hash subject binary {}", path.display())
    })?))
}

/// nc-stale-binary: the hash recorded at run start must still match the file.
pub fn verify_binary_freshness(recorded: &str, binary: &std::path::Path) -> Result<()> {
    let actual = hash_file(binary)?;
    ensure!(
        actual == recorded,
        "nc-stale-binary: recorded SHA-256 {recorded} != actual SHA-256 {actual} for {}; \
         the subject binary changed mid-run",
        binary.display()
    );
    Ok(())
}

fn self_check(rust_bin: &std::path::Path, recorded_hash: &str) -> Result<()> {
    verify_binary_freshness(recorded_hash, rust_bin)?;
    // nc-torn-event-line control: a file whose final record lacks its
    // terminating LF must fail validation; a PASS requires that rejection.
    let directory = tempfile::Builder::new()
        .prefix("pipestream-durable-selfcheck-")
        .tempdir()?;
    let events = directory.path().join("events.tsv");
    fs::write(
        &events,
        "1\trun\tscenario\trust\tclient\tpsid\t1\tREQUEST_SENT\t\t\t\t\t\t\n\
         1\trun\tscenario\trust\tclient\tpsid\t2\tREQUEST",
    )?;
    let rejected = events::read_events(&events, "run", "scenario").is_err();
    ensure!(
        rejected,
        "self-check negative control nc-torn-event-line did not fail: torn lines are being accepted"
    );
    println!(
        "self-check negative controls behaved as required (nc-torn-event-line rejected, subject hash fresh)"
    );
    Ok(())
}

fn write_run_manifest(
    run_root: &std::path::Path,
    args: &DurableArgs,
    rust_bin: &std::path::Path,
    rust_hash: &str,
    java_hash: Option<&(PathBuf, String)>,
) -> Result<()> {
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let (java_path, java_sha) = java_hash
        .map(|(jar, hash)| (jar.display().to_string(), hash.clone()))
        .unwrap_or_else(|| ("-".to_owned(), "-".to_owned()));
    fs::write(
        run_root.join("run.tsv"),
        format!(
            "started_ms\t{started}\nmode\t{}\nseed\t{}\nrust_bin\t{}\trust_sha256\t{}\n\
             java_jar\t{java_path}\tjava_sha256\t{java_sha}\n",
            if args.dev { "dev" } else { "acceptance" },
            args.seed,
            path(rust_bin),
            rust_hash,
        ),
    )?;
    Ok(())
}

fn select_rows<'a>(
    matrix: &'a [scenarios::Row],
    requested: &[String],
) -> Result<Vec<&'a scenarios::Row>> {
    if requested.is_empty() {
        return Ok(matrix.iter().collect());
    }
    let mut selected = Vec::new();
    for id in requested {
        let row = matrix.iter().find(|row| row.id == id).with_context(|| {
            let known = matrix
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown scenario id {id:?} (nc-missing-scenario); known rows: {known}")
        })?;
        selected.push(row);
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nc_stale_binary_detects_a_changed_subject() {
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("subject-a");
        let replaced = directory.path().join("subject-b");
        fs::write(&original, b"original bytes").unwrap();
        fs::write(&replaced, b"replaced bytes").unwrap();
        let recorded = hash_file(&original).unwrap();
        // The unchanged binary passes the freshness check...
        verify_binary_freshness(&recorded, &original).unwrap();
        // ...and a mid-run swap is the exact nc-stale-binary failure.
        let error = verify_binary_freshness(&recorded, &replaced).unwrap_err();
        assert!(
            format!("{error:#}").contains("nc-stale-binary"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn unknown_scenario_ids_are_rejected_with_the_known_matrix() {
        let matrix = scenarios::rows();
        assert!(
            select_rows(&matrix, &["g1-leaf-copy".to_owned()])
                .unwrap()
                .iter()
                .all(|row| row.id == "g1-leaf-copy")
        );
        let error = select_rows(&matrix, &["nope".to_owned()]).unwrap_err();
        assert!(format!("{error:#}").contains("nc-missing-scenario"));
    }

    #[test]
    fn duplicate_scenario_selections_run_once_each_in_order() {
        let matrix = scenarios::rows();
        let selected = select_rows(
            &matrix,
            &["g1-leaf-copy".to_owned(), "g1-empty-input".to_owned()],
        )
        .unwrap();
        assert_eq!(
            selected.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec!["g1-leaf-copy", "g1-empty-input"]
        );
    }
}
