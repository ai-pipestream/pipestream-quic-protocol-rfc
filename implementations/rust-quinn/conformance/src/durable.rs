//! Neutral durable conformance driver (milestone 1). Spawns subject binaries
//! black-box; the crate keeps its existing dependency set only.

mod events;
mod gates;
mod mtls;
mod oracle;
mod process;
mod rawclient;
mod resources;
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
    /// After the run, copy the run directory into <archive>/<run_id> and write
    /// a MANIFEST.sha256 over every archived file. Large files (>2 MiB) are
    /// represented by a hash-and-length note, never copied byte-for-byte.
    #[arg(long)]
    archive: Option<PathBuf>,
    /// Waive a matrix row (`--waive g7-unsafe-clock-refusal`) or one direction
    /// (`--waive g3-store-ownership:java-client/rust-server`), repeatable, in
    /// the form `ROW[:DIRECTION][=REASON]`. Waived rows/directions are
    /// recorded in run.tsv and named in the run output; in acceptance mode a
    /// waived row does not FAIL the run, and a waived direction must carry its
    /// named-gap INCOMPLETE evidence in the row directory.
    #[arg(long = "waive", value_name = "ROW[:DIRECTION][=REASON]")]
    waive: Vec<String>,
}

/// One `--waive` target: a whole row or one `<row>:<direction>` pair, with
/// the reason the waiver exists (the reason string always lands in run.tsv).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaiveTarget {
    pub row: String,
    pub direction: Option<String>,
    pub reason: Option<String>,
}

/// The direction spellings the driver uses internally (see JAVA_DIRECTIONS
/// and every direction_coverage string).
const DIRECTIONS: &[&str] = &[
    "rust-client/rust-server",
    "java-client/rust-server",
    "rust-client/java-server",
];

fn parse_waive_target(spec: &str) -> Result<WaiveTarget> {
    let (target, reason) = match spec.split_once('=') {
        Some((target, reason)) => (target, Some(reason.trim().to_owned())),
        None => (spec, None),
    };
    let (row, direction) = match target.split_once(':') {
        Some((row, direction)) => (row, Some(direction.to_owned())),
        None => (target, None),
    };
    ensure!(
        !row.is_empty(),
        "invalid --waive {spec:?}: the row id is empty"
    );
    if let Some(direction) = &direction {
        ensure!(
            DIRECTIONS.contains(&direction.as_str()),
            "invalid --waive {spec:?}: unknown direction {direction:?} (known directions: {})",
            DIRECTIONS.join(", ")
        );
    }
    Ok(WaiveTarget {
        row: row.to_owned(),
        direction,
        reason,
    })
}

/// Parse and validate the waiver specs against the matrix. Duplicate targets
/// are rejected: a waiver accepted twice with different reasons would be
/// ambiguous in run.tsv.
fn parse_waivers(specs: &[String], matrix: &[scenarios::Row]) -> Result<Vec<WaiveTarget>> {
    let mut targets: Vec<WaiveTarget> = Vec::new();
    for spec in specs {
        let target = parse_waive_target(spec)?;
        let known = matrix.iter().any(|row| row.id == target.row);
        ensure!(
            known,
            "unknown --waive row {:?} (nc-missing-scenario); the matrix rows are listed in \
             scenario-matrix-*.md",
            target.row
        );
        ensure!(
            !targets.iter().any(|existing| {
                existing.row == target.row && existing.direction == target.direction
            }),
            "duplicate --waive target {spec:?}"
        );
        targets.push(target);
    }
    Ok(targets)
}

/// The run.tsv fragment naming every waiver; the reason string always lands
/// here verbatim (tabs and newlines are flattened so the file stays TSV).
fn render_waivers_tsv(waivers: &[WaiveTarget]) -> String {
    let mut text = format!("waivers\t{}\n", waivers.len());
    for waiver in waivers {
        let direction = waiver.direction.clone().unwrap_or_else(|| "-".to_owned());
        let reason = waiver
            .reason
            .clone()
            .unwrap_or_else(|| "(no reason recorded)".to_owned())
            .replace(['\t', '\n'], " ");
        text.push_str(&format!("waive\t{}\t{direction}\t{reason}\n", waiver.row));
    }
    text
}

/// How a declared direction waiver stood against the row's evidence.
#[derive(Debug)]
enum DirectionWaiverReport {
    /// The named-gap INCOMPLETE marker exists; the waiver accepts it and
    /// the line names the evidence.
    Covered(String),
    /// The direction directory exists without a marker: the direction ran
    /// and passed, so the declared waiver was not needed (mirrors the
    /// whole-row unused rule).
    Unused(String),
}

/// A waived direction is an explicit acceptance of a NAMED gap: the
/// direction directory must carry its `INCOMPLETE` named-gap marker, unless
/// the direction ran and passed (directory present, no marker), in which
/// case the waiver is named unused. A waiver naming a direction that never
/// ran and left no evidence is itself a failure. Direction directory names
/// hyphenate the internal slash spelling.
fn check_direction_waiver(
    scenario_dir: &std::path::Path,
    target: &WaiveTarget,
) -> Result<DirectionWaiverReport> {
    let direction = target
        .direction
        .as_deref()
        .context("check_direction_waiver needs a direction waiver")?;
    let directory = scenario_dir.join(direction.replace('/', "-"));
    let marker = directory.join("INCOMPLETE");
    let reason = target.reason.as_deref().unwrap_or("(no reason recorded)");
    if marker.is_file() {
        return Ok(DirectionWaiverReport::Covered(format!(
            "waived {direction}: {reason} (named-gap evidence {})",
            marker.display()
        )));
    }
    ensure!(
        directory.is_dir(),
        "waiver names {direction}, but {} holds no named-gap INCOMPLETE evidence; a waiver \
         accepts a named gap, it never replaces one",
        marker.display()
    );
    Ok(DirectionWaiverReport::Unused(format!(
        "unused direction waiver (the direction passed without a named gap): {reason}"
    )))
}

/// Every direction-level INCOMPLETE marker under the row directory, as
/// (hyphenated direction directory name, marker path, content). Row-level
/// outcomes are reported through `DirectionOutcome`; only direction
/// subdirectories carry these markers (scenarios.rs writes them under
/// `<scenario_dir>/<direction-hyphenated>/`).
fn direction_incomplete_markers(
    scenario_dir: &std::path::Path,
) -> Result<Vec<(String, PathBuf, String)>> {
    let mut markers = Vec::new();
    for path in walk_sorted(scenario_dir)? {
        if path.file_name() == Some(std::ffi::OsStr::new("INCOMPLETE"))
            && path.parent() != Some(scenario_dir)
        {
            let direction = path
                .parent()
                .and_then(|parent| parent.file_name())
                .map(|name| name.to_string_lossy().into_owned())
                .context("a marker file has no parent directory name")?;
            let content = fs::read_to_string(&path)
                .with_context(|| format!("read marker {}", path.display()))?;
            markers.push((direction, path, content));
        }
    }
    Ok(markers)
}

/// Every `row_status` value in observed.tsv files under the row directory
/// (the R rows and the missing-capability rows record theirs there).
fn row_status_values(scenario_dir: &std::path::Path) -> Result<Vec<(PathBuf, String)>> {
    let mut values = Vec::new();
    for path in walk_sorted(scenario_dir)? {
        if path.file_name() == Some(std::ffi::OsStr::new("observed.tsv")) {
            for line in fs::read_to_string(&path)
                .with_context(|| format!("read {}", path.display()))?
                .lines()
            {
                if let Some(value) = line.strip_prefix("row_status\t") {
                    values.push((path.clone(), value.to_owned()));
                }
            }
        }
    }
    Ok(values)
}

/// PARTIAL is acceptable in acceptance mode only with its named unmeasured
/// scopes intact: the value must be `PARTIAL: <scopes>` with a non-empty
/// reason. A bare or empty PARTIAL is a stripped marker and never passes.
/// Returns the relative evidence paths carrying a PARTIAL marker.
fn check_partial_markers(values: &[(PathBuf, String)]) -> Result<Vec<PathBuf>> {
    let mut partial = Vec::new();
    for (path, value) in values {
        if value.starts_with("PARTIAL") {
            let reason = value.strip_prefix("PARTIAL:").map(str::trim);
            ensure!(
                matches!(reason, Some(reason) if !reason.is_empty()),
                "row_status PARTIAL without named scopes in {}: {value:?}; the marker is \
                 accepted only with the unmeasured scopes named",
                path.display()
            );
            partial.push(path.clone());
        }
    }
    Ok(partial)
}

/// How one row's outcome is reported: the line, its PASS-line annotations
/// (PARTIAL markers, waivers), and whether it fails the run.
#[derive(Debug)]
struct RowReport {
    line: String,
    annotations: Vec<String>,
    failed: bool,
}

/// Convert a failing row outcome under a whole-row waiver (the Fail and
/// Incomplete arms share this rule, so the two can never drift): the waiver
/// absorbs ONLY a failure whose reason names the row's missing capability;
/// anything else stays FAILED and says the waiver did not match.
fn waive_row_failure(row_id: &str, reason: &str, waiver: Option<&WaiveTarget>) -> RowReport {
    match waiver {
        None => RowReport {
            line: format!("FAIL {row_id}: {reason}"),
            annotations: Vec::new(),
            failed: true,
        },
        Some(waiver) if reason.contains(scenarios::MISSING_CAPABILITY_MARKER) => RowReport {
            line: format!(
                "WAIVED {row_id}: {reason} (waiver: {})",
                waiver.reason.as_deref().unwrap_or("(no reason recorded)")
            ),
            annotations: Vec::new(),
            failed: false,
        },
        Some(_) => RowReport {
            line: format!(
                "FAIL {row_id}: {reason} (waiver did not match: the waiver for this row \
                 accepts its named missing capability, and this failure is not one)"
            ),
            annotations: Vec::new(),
            failed: true,
        },
    }
}

/// The driver's canonical slash spelling for a hyphenated direction
/// directory name. Direction components contain hyphens themselves
/// ("java-client", "rust-server"), so only the known direction set can be
/// reverse-mapped; anything else is echoed verbatim.
fn slash_direction(hyphenated: &str) -> String {
    DIRECTIONS
        .iter()
        .find(|direction| direction.replace('/', "-") == hyphenated)
        .map(|direction| (*direction).to_owned())
        .unwrap_or_else(|| hyphenated.to_owned())
}

/// Report one row's outcome, applying waivers (acceptance mode only; dev
/// keeps its INCOMPLETE labelling untouched) and the PARTIAL named-scope
/// check. Kept free of process state so the rules are unit-testable.
fn report_row_outcome(
    row: &scenarios::Row,
    outcome: &scenarios::DirectionOutcome,
    scenario_dir: &std::path::Path,
    waivers: &[WaiveTarget],
    dev: bool,
) -> RowReport {
    let whole_row_waiver = waivers
        .iter()
        .find(|target| target.row == row.id && target.direction.is_none());
    let direction_waivers: Vec<&WaiveTarget> = waivers
        .iter()
        .filter(|target| target.row == row.id && target.direction.is_some())
        .collect();
    match outcome {
        scenarios::DirectionOutcome::Pass(directions) => {
            let partials = match row_status_values(scenario_dir)
                .and_then(|values| check_partial_markers(&values))
            {
                Ok(partials) => partials,
                Err(error) => {
                    return RowReport {
                        line: format!("FAIL {}: {error:#}", row.id),
                        annotations: Vec::new(),
                        failed: true,
                    };
                }
            };
            let mut annotations: Vec<String> = partials
                .iter()
                .map(|path| {
                    format!(
                        "row_status PARTIAL in {}: unmeasured scopes named",
                        path.display()
                    )
                })
                .collect();
            if let Some(waiver) = whole_row_waiver {
                annotations.push(format!(
                    "unused whole-row waiver (the row passed): {}",
                    waiver.reason.as_deref().unwrap_or("(no reason recorded)")
                ));
            }
            if !dev {
                for target in &direction_waivers {
                    match check_direction_waiver(scenario_dir, target) {
                        Ok(DirectionWaiverReport::Covered(line))
                        | Ok(DirectionWaiverReport::Unused(line)) => annotations.push(line),
                        Err(error) => {
                            return RowReport {
                                line: format!("FAIL {}: {error:#}", row.id),
                                annotations: Vec::new(),
                                failed: true,
                            };
                        }
                    }
                }
                // Every direction-level INCOMPLETE marker must be covered by
                // a declared direction waiver; an unwaived marker FAILs the
                // row, naming the direction and quoting the marker's reason
                // (acceptance must never pass an incomplete direction
                // silently - the pre-landing review's exact finding).
                let declared: Vec<String> = direction_waivers
                    .iter()
                    .filter_map(|target| target.direction.clone())
                    .collect();
                let markers = match direction_incomplete_markers(scenario_dir) {
                    Ok(markers) => markers,
                    Err(error) => {
                        return RowReport {
                            line: format!("FAIL {}: {error:#}", row.id),
                            annotations: Vec::new(),
                            failed: true,
                        };
                    }
                };
                for (direction, marker, content) in markers {
                    let covered = declared
                        .iter()
                        .any(|spelling| spelling.replace('/', "-") == direction);
                    if !covered {
                        return RowReport {
                            line: format!(
                                "FAIL {}: direction {} recorded an INCOMPLETE marker that no \
                                 direction waiver covers: {}:\n{}",
                                row.id,
                                slash_direction(&direction),
                                marker.display(),
                                content.trim_end()
                            ),
                            annotations: Vec::new(),
                            failed: true,
                        };
                    }
                }
            }
            let suffix = if annotations.is_empty() {
                String::new()
            } else {
                format!(" ({})", annotations.join("; "))
            };
            RowReport {
                line: format!("PASS {} {directions}{suffix}", row.id),
                annotations,
                failed: false,
            }
        }
        scenarios::DirectionOutcome::Incomplete(reason) => {
            if dev {
                // Dev labelling is correct and stays byte-for-byte
                // unchanged: an Incomplete row reports INCOMPLETE and never
                // fails the dev run.
                RowReport {
                    line: format!("INCOMPLETE {}: {reason}", row.id),
                    annotations: Vec::new(),
                    failed: false,
                }
            } else {
                let mut report = waive_row_failure(row.id, reason, whole_row_waiver);
                if report.failed && whole_row_waiver.is_none() {
                    report.line = format!(
                        "{} (acceptance requires an explicit whole-row waiver naming this \
                         row's missing capability, and none is declared)",
                        report.line
                    );
                }
                report
            }
        }
        scenarios::DirectionOutcome::Fail(reason) => {
            if dev {
                RowReport {
                    line: format!("FAIL {}: {reason}", row.id),
                    annotations: Vec::new(),
                    failed: true,
                }
            } else {
                waive_row_failure(row.id, reason, whole_row_waiver)
            }
        }
    }
}

const JAVA_DIRECTIONS: &[&str] = &["java-client/rust-server", "rust-client/java-server"];

/// Validate a subject artifact, then canonicalize it. Subjects are spawned
/// with a scenario-owned working directory, so a relative --rust-bin or
/// --java-jar is resolved by the child against ITS cwd, not the launch cwd
/// (the acceptance smoke test of the run_all.sh block shape caught exactly
/// this: the binary existed but `start ./target/release/pipestream-quinn`
/// failed with ENOENT from the scenario cwd). Canonicalizing after the
/// is_file check makes every recorded and spawned path cwd-independent.
fn canonical_subject_path(path: &std::path::Path, flag: &str) -> Result<PathBuf> {
    ensure!(
        path.is_file(),
        "missing {flag} subject artifact at {} (build it with: cargo build --release --locked)",
        path.display()
    );
    fs::canonicalize(path).with_context(|| format!("canonicalize {flag} path {}", path.display()))
}

/// Store roots may not exist yet, so they cannot be canonicalized; join a
/// relative path onto the launch cwd instead, keeping symlink structure
/// intact. Every path the driver hands a spawned subject ends up absolute.
fn absolutize(path: PathBuf) -> Result<PathBuf> {
    if path.is_relative() {
        Ok(std::env::current_dir()
            .context("resolve the launch cwd for a relative store path")?
            .join(path))
    } else {
        Ok(path)
    }
}

pub fn run(args: DurableArgs) -> Result<()> {
    let root = repository_root()?;
    let rust_bin = args
        .rust_bin
        .clone()
        .unwrap_or_else(|| root.join("implementations/rust-quinn/target/release/pipestream-quinn"));
    let rust_bin = canonical_subject_path(&rust_bin, "--rust-bin")?;
    let java_jar = match &args.java_jar {
        Some(jar) => Some(canonical_subject_path(jar, "--java-jar")?),
        None => None,
    };
    if !args.dev {
        let mut unmet: Vec<String> = Vec::new();
        if java_jar.is_none() {
            unmet.push(format!(
                "missing --java-jar: acceptance mode runs the whole matrix, so the \
                 Java-direction rows ({}) are an explicit gate; rerun with --dev to \
                 execute the available rust direction only",
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
    let java_hash = match &java_jar {
        Some(jar) => Some((jar.clone(), hash_file(jar)?)),
        None => None,
    };

    let run_id = format!("durable-{:016x}", unique_suffix());
    let base = match &args.artifacts {
        Some(artifacts) => absolutize(artifacts.clone())?,
        None => root.join("implementations/rust-quinn/target/durable-runs"),
    };
    let run_root = base.join("runs").join(&run_id);
    fs::create_dir_all(&run_root)?;
    let archive = match &args.archive {
        Some(archive) => Some(absolutize(archive.clone())?),
        None => None,
    };

    // Waivers are validated before anything runs so a typo'd --waive fails
    // fast instead of surfacing as a FAIL at the end of an hour-long run.
    let matrix = scenarios::rows();
    let waivers = parse_waivers(&args.waive, &matrix)?;
    write_run_manifest(
        &run_root,
        &args,
        &rust_bin,
        &rust_hash,
        java_hash.as_ref(),
        &waivers,
    )?;

    // Self-check stage: the negative controls must behave as designed. The
    // torn-event-line control must be REJECTED by the reader, and the recorded
    // subject hash must still match the binary on disk.
    self_check(&rust_bin, &rust_hash)?;

    let selected = select_rows(&matrix, &args.scenario)?;
    let context = scenarios::ScenarioContext {
        run_id: run_id.clone(),
        run_root: run_root.clone(),
        seed: args.seed,
        rust_bin: rust_bin.clone(),
        java_jar: java_jar.clone(),
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
        let report = report_row_outcome(
            row,
            outcome,
            &context.scenario_dir(row.id),
            &waivers,
            args.dev,
        );
        failed |= report.failed;
        match outcome {
            scenarios::DirectionOutcome::Pass(directions) if args.dev => {
                let suffix = if report.annotations.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", report.annotations.join("; "))
                };
                println!(
                    "SCENARIO OK {} {directions}{suffix} (dev mode: labelled INCOMPLETE)",
                    row.id
                );
            }
            _ => println!("{}", report.line),
        }
    }
    if !waivers.is_empty() {
        println!(
            "waivers recorded: {} (run.tsv; dev mode does not neutralise rows)",
            waivers.len()
        );
        for waiver in &waivers {
            match &waiver.direction {
                Some(direction) => println!(
                    "  WAIVED {}:{direction}: {}",
                    waiver.row,
                    waiver.reason.as_deref().unwrap_or("(no reason recorded)")
                ),
                None => println!(
                    "  WAIVED {}: {}",
                    waiver.row,
                    waiver.reason.as_deref().unwrap_or("(no reason recorded)")
                ),
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
    if let Some(archive) = &archive {
        archive_run(&run_root, archive, &run_id)?;
        println!("archived run {run_id}: {}", archive.join(&run_id).display());
    }
    Ok(())
}

/// Copy one run directory into <archive>/<run_id>. Files larger than 2 MiB
/// are not copied: a `<name>.LARGE.txt` note records the sha256, length and
/// the fact the bytes were elided. A MANIFEST.sha256 listing every archived
/// file (sha256, sorted relative paths, the manifest itself excluded) is
/// written last.
fn archive_run(
    run_root: &std::path::Path,
    archive_dir: &std::path::Path,
    run_id: &str,
) -> Result<()> {
    const LARGE_LIMIT: u64 = 2 * 1024 * 1024;
    let destination = archive_dir.join(run_id);
    fs::create_dir_all(&destination)?;
    let mut copied: Vec<PathBuf> = Vec::new();
    for entry in walk_sorted(run_root)? {
        let relative = entry
            .strip_prefix(run_root)
            .expect("walk root is a prefix of every walked path")
            .to_path_buf();
        let target = destination.join(&relative);
        let length = fs::metadata(&entry)?.len();
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        if length > LARGE_LIMIT {
            let digest = oracle::sha256_hex(
                &fs::read(&entry)
                    .with_context(|| format!("hash large run file {}", entry.display()))?,
            );
            let note = format!(
                "sha256={digest}\nlen={length}\nThis file exceeded the {LARGE_LIMIT}-byte archive \
                 limit; its bytes were not archived.\n"
            );
            let note_path = destination.join(large_note_name(&relative));
            fs::write(&note_path, note)?;
            copied.push(note_path);
        } else {
            fs::copy(&entry, &target)
                .with_context(|| format!("archive {} -> {}", entry.display(), target.display()))?;
            copied.push(target);
        }
    }
    let mut manifest = String::new();
    copied.sort();
    for file in &copied {
        let relative = file
            .strip_prefix(&destination)
            .expect("archive root is a prefix of every archived path");
        let digest = oracle::sha256_hex(
            &fs::read(file).with_context(|| format!("hash archived file {}", file.display()))?,
        );
        manifest.push_str(&format!("{digest}\t{}\n", path(relative)));
    }
    fs::write(destination.join("MANIFEST.sha256"), manifest)?;
    Ok(())
}

/// `<relative>.LARGE.txt`, with the path rendered the way the manifest
/// renders it (forward slashes on every platform).
fn large_note_name(relative: &std::path::Path) -> PathBuf {
    let mut text = path(relative);
    text.push_str(".LARGE.txt");
    PathBuf::from(text)
}

/// Every file under `root`, sorted so the archive is deterministic.
fn walk_sorted(root: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("read run directory {}", directory.display()))?
        {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                directories.push(entry_path);
            } else {
                files.push(entry_path);
            }
        }
    }
    files.sort();
    Ok(files)
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
    waivers: &[WaiveTarget],
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
             java_jar\t{java_path}\tjava_sha256\t{java_sha}\njava_memory_flags\t{}\n{waivers}",
            if args.dev { "dev" } else { "acceptance" },
            args.seed,
            path(rust_bin),
            rust_hash,
            process::java_memory_flags_text(),
            waivers = render_waivers_tsv(waivers),
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

    #[test]
    fn waive_specs_parse_row_direction_and_reason() {
        assert_eq!(
            parse_waive_target("g7-unsafe-clock-refusal").unwrap(),
            WaiveTarget {
                row: "g7-unsafe-clock-refusal".into(),
                direction: None,
                reason: None,
            }
        );
        assert_eq!(
            parse_waive_target("g3-store-ownership:java-client/rust-server").unwrap(),
            WaiveTarget {
                row: "g3-store-ownership".into(),
                direction: Some("java-client/rust-server".into()),
                reason: None,
            }
        );
        // The reason keeps everything after the first '=' and may itself
        // contain '=' and ':' (only the target side is split on ':').
        let target = parse_waive_target(
            "g4-revocation-vs-publication:rust-client/java-server=no Java \
                                operator revoke command: DurableHost.revoke is host-internal",
        )
        .unwrap();
        assert_eq!(target.row, "g4-revocation-vs-publication");
        assert_eq!(target.direction.as_deref(), Some("rust-client/java-server"));
        assert!(
            target
                .reason
                .as_deref()
                .unwrap()
                .contains("DurableHost.revoke is host-internal")
        );
        let error = parse_waive_target("g3-store-ownership:sideways/client").unwrap_err();
        assert!(
            format!("{error:#}").contains("unknown direction"),
            "{error:#}"
        );
        let error = parse_waive_target(":java-client/rust-server").unwrap_err();
        assert!(
            format!("{error:#}").contains("row id is empty"),
            "{error:#}"
        );
    }

    #[test]
    fn waive_specs_validate_against_the_matrix() {
        let matrix = scenarios::rows();
        let waivers = parse_waivers(
            &[
                "g7-unsafe-clock-refusal=no fixture clock on either subject".to_owned(),
                "g3-store-ownership:java-client/rust-server=the client subject never owns the \
                 store"
                    .to_owned(),
            ],
            &matrix,
        )
        .unwrap();
        assert_eq!(waivers.len(), 2);
        let error =
            parse_waivers(&["g2-drop-reply-publication=x".to_owned()], &matrix).unwrap_err();
        assert!(
            format!("{error:#}").contains("unknown --waive row"),
            "{error:#}"
        );
        let error = parse_waivers(
            &[
                "g7-unsafe-clock-refusal=a".to_owned(),
                "g7-unsafe-clock-refusal=b".to_owned(),
            ],
            &matrix,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("duplicate --waive"),
            "{error:#}"
        );
    }

    #[test]
    fn waivers_render_into_run_tsv_with_their_reasons() {
        let text = render_waivers_tsv(&[
            WaiveTarget {
                row: "g7-unsafe-clock-refusal".into(),
                direction: None,
                reason: Some("no fixture clock on either subject".into()),
            },
            WaiveTarget {
                row: "g3-store-ownership".into(),
                direction: Some("java-client/rust-server".into()),
                reason: Some("the client subject never owns the store".into()),
            },
        ]);
        assert!(text.starts_with("waivers\t2\n"), "{text}");
        assert!(
            text.contains(
                "waive\tg7-unsafe-clock-refusal\t-\tno fixture clock on either subject\n"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "waive\tg3-store-ownership\tjava-client/rust-server\tthe client subject never \
                 owns the store\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_direction_waiver_requires_the_named_gap_marker() {
        let directory = tempfile::tempdir().unwrap();
        let row_dir = directory.path().join("g3-store-ownership");
        let target = WaiveTarget {
            row: "g3-store-ownership".into(),
            direction: Some("java-client/rust-server".into()),
            reason: Some("the client subject never owns the store".into()),
        };
        // No direction directory at all: the direction never ran and left
        // no evidence, so the waiver itself fails (a waiver accepts a named
        // gap, it never replaces one).
        let error = check_direction_waiver(&row_dir, &target).unwrap_err();
        assert!(
            format!("{error:#}").contains("no named-gap INCOMPLETE evidence"),
            "{error:#}"
        );
        // The named-gap marker makes the waiver an explicit acceptance.
        let direction_dir = row_dir.join("java-client-rust-server");
        fs::create_dir_all(&direction_dir).unwrap();
        fs::write(direction_dir.join("INCOMPLETE"), b"named gap\n").unwrap();
        let report = check_direction_waiver(&row_dir, &target).unwrap();
        let line = match report {
            DirectionWaiverReport::Covered(line) => line,
            other => panic!("expected the marker to cover the waiver, got {other:?}"),
        };
        assert!(line.contains("waived java-client/rust-server"), "{line}");
        assert!(line.contains("never owns the store"), "{line}");
        // The direction ran and passed (directory, evidence, no marker):
        // the waiver is named unused, mirroring the whole-row rule.
        fs::remove_file(direction_dir.join("INCOMPLETE")).unwrap();
        fs::write(direction_dir.join("observed.tsv"), "row_status\tfull\n").unwrap();
        let report = check_direction_waiver(&row_dir, &target).unwrap();
        let line = match report {
            DirectionWaiverReport::Unused(line) => line,
            other => {
                panic!("expected the passed direction to make the waiver unused, got {other:?}")
            }
        };
        assert!(line.contains("unused direction waiver"), "{line}");
    }

    /// Acceptance scans the row directory for direction-level INCOMPLETE
    /// markers: every marker must be covered by a declared direction
    /// waiver, and an unwaived marker FAILs the row naming the direction
    /// and quoting the marker's named reason (the pre-landing review's
    /// "incomplete directions can pass without an explicit waiver" hole).
    #[test]
    fn unwaived_direction_marker_fails_acceptance() {
        let row = scenarios::Row {
            id: "g3-store-ownership",
            group: "G3",
            rust_implemented: true,
        };
        let directory = tempfile::tempdir().unwrap();
        let row_dir = directory.path().join("g3-store-ownership");
        let direction_dir = row_dir.join("java-client-rust-server");
        fs::create_dir_all(&direction_dir).unwrap();
        fs::write(
            direction_dir.join("INCOMPLETE"),
            b"named gap: the client subject never owns the store\n",
        )
        .unwrap();
        let pass = scenarios::DirectionOutcome::Pass("rust-client/rust-server".into());
        // No waiver declared: the marker FAILs the row in acceptance...
        let report = report_row_outcome(&row, &pass, &row_dir, &[], false);
        assert!(report.failed, "{report:?}");
        assert!(
            report.line.starts_with("FAIL g3-store-ownership"),
            "{report:?}"
        );
        assert!(
            report.line.contains("java-client/rust-server"),
            "{report:?}"
        );
        assert!(
            report
                .line
                .contains("named gap: the client subject never owns the store"),
            "{report:?}"
        );
        // ...and the declared waiver covering it converts to a PASS.
        let waivers = vec![WaiveTarget {
            row: "g3-store-ownership".into(),
            direction: Some("java-client/rust-server".into()),
            reason: Some("the client subject never owns the store".into()),
        }];
        let report = report_row_outcome(&row, &pass, &row_dir, &waivers, false);
        assert!(!report.failed, "{report:?}");
        assert!(
            report.line.contains(
                "waived java-client/rust-server: the client subject never owns the store"
            ),
            "{report:?}"
        );
        // A waiver for one direction does not cover another direction's
        // marker: hyphenated spelling is compared exactly.
        let other_dir = row_dir.join("rust-client-java-server");
        fs::create_dir_all(&other_dir).unwrap();
        fs::write(other_dir.join("INCOMPLETE"), b"another named gap\n").unwrap();
        let report = report_row_outcome(&row, &pass, &row_dir, &waivers, false);
        assert!(report.failed, "{report:?}");
        assert!(
            report.line.contains("rust-client/java-server"),
            "{report:?}"
        );
    }

    /// A row-level Incomplete in acceptance FAILs unless a whole-row waiver
    /// covers it, and covers it only when the reason names the row's
    /// missing capability - the same rule as the Fail arm.
    #[test]
    fn row_level_incomplete_needs_a_matching_waiver_in_acceptance() {
        let row = scenarios::Row {
            id: "g7-unsafe-clock-refusal",
            group: "G7",
            rust_implemented: true,
        };
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("g7-unsafe-clock-refusal")).unwrap();
        let waivers = vec![WaiveTarget {
            row: "g7-unsafe-clock-refusal".into(),
            direction: None,
            reason: Some("no fixture clock on either subject".into()),
        }];
        let missing = format!(
            "g7-unsafe-clock-refusal INCOMPLETE: {}no subject fixture clock: ...",
            scenarios::MISSING_CAPABILITY_MARKER
        );
        // Without a waiver: acceptance requires an explicit one, never
        // silent passage.
        let incomplete = scenarios::DirectionOutcome::Incomplete(missing.clone());
        let report = report_row_outcome(&row, &incomplete, directory.path(), &[], false);
        assert!(report.failed, "{report:?}");
        assert!(
            report.line.starts_with("FAIL g7-unsafe-clock-refusal"),
            "{report:?}"
        );
        assert!(
            report.line.contains("acceptance requires an explicit"),
            "{report:?}"
        );
        // The matching whole-row waiver converts it, like the Fail arm.
        let report = report_row_outcome(&row, &incomplete, directory.path(), &waivers, false);
        assert!(!report.failed, "{report:?}");
        assert!(
            report.line.starts_with("WAIVED g7-unsafe-clock-refusal"),
            "{report:?}"
        );
        // A non-matching Incomplete stays FAILED even under a waiver.
        let other = scenarios::DirectionOutcome::Incomplete(
            "g7-unsafe-clock-refusal: process timed out".into(),
        );
        let report = report_row_outcome(&row, &other, directory.path(), &waivers, false);
        assert!(report.failed, "{report:?}");
        assert!(report.line.contains("waiver did not match"), "{report:?}");
        // Dev mode is untouched: Incomplete stays an INCOMPLETE report.
        let report = report_row_outcome(&row, &incomplete, directory.path(), &[], true);
        assert!(!report.failed, "{report:?}");
        assert!(
            report
                .line
                .starts_with("INCOMPLETE g7-unsafe-clock-refusal"),
            "{report:?}"
        );
    }

    #[test]
    fn partial_row_status_is_acceptable_only_with_named_scopes() {
        let directory = tempfile::tempdir().unwrap();
        let row_dir = directory.path().join("r-network-bytes");
        fs::create_dir_all(&row_dir).unwrap();
        fs::write(
            row_dir.join("observed.tsv"),
            "server_subject\trust\nrow_status\tPARTIAL: this host grants NEITHER a network \
             namespace NOR packet capture; named reason, never a skip\n",
        )
        .unwrap();
        let values = row_status_values(&row_dir).unwrap();
        let partials = check_partial_markers(&values).unwrap();
        assert_eq!(partials.len(), 1);
        // A full status line is not a PARTIAL marker.
        fs::write(
            row_dir.join("observed.tsv"),
            "row_status\tfull on this host\n",
        )
        .unwrap();
        let values = row_status_values(&row_dir).unwrap();
        assert!(check_partial_markers(&values).unwrap().is_empty());
        // A stripped bare marker is never acceptable.
        fs::write(row_dir.join("observed.tsv"), "row_status\tPARTIAL\n").unwrap();
        let values = row_status_values(&row_dir).unwrap();
        let error = check_partial_markers(&values).unwrap_err();
        assert!(
            format!("{error:#}").contains("PARTIAL without named scopes"),
            "{error:#}"
        );
    }

    #[test]
    fn waived_rows_do_not_fail_acceptance_but_dev_reporting_is_untouched() {
        let row = scenarios::Row {
            id: "g7-unsafe-clock-refusal",
            group: "G7",
            rust_implemented: true,
        };
        let waivers = vec![WaiveTarget {
            row: "g7-unsafe-clock-refusal".into(),
            direction: None,
            reason: Some("no fixture clock on either subject".into()),
        }];
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("g7-unsafe-clock-refusal")).unwrap();
        // The failure the waiver exists for: the row's named missing
        // capability (run_direction prefixes it "INCOMPLETE: " and
        // MissingCapability displays "missing subject capability: ...").
        let missing = scenarios::DirectionOutcome::Fail(format!(
            "g7-unsafe-clock-refusal INCOMPLETE: {}no subject fixture clock: ...",
            scenarios::MISSING_CAPABILITY_MARKER
        ));
        let report = report_row_outcome(&row, &missing, directory.path(), &waivers, false);
        assert!(!report.failed, "{report:?}");
        assert!(
            report.line.starts_with("WAIVED g7-unsafe-clock-refusal"),
            "{report:?}"
        );
        // A waiver over ANY OTHER failure must not absorb it: the row stays
        // FAILED and the line says the waiver did not match (a waiver
        // accepts a named gap, it never hides a defect).
        let spawn_error = scenarios::DirectionOutcome::Fail(
            "start ./target/release/pipestream-quinn v2 init-authority: No such file or \
             directory (os error 2)"
                .into(),
        );
        let report = report_row_outcome(&row, &spawn_error, directory.path(), &waivers, false);
        assert!(report.failed, "{report:?}");
        assert!(
            report.line.starts_with("FAIL g7-unsafe-clock-refusal"),
            "{report:?}"
        );
        assert!(report.line.contains("waiver did not match"), "{report:?}");
        // Without a waiver the same failure fails the run.
        let report = report_row_outcome(&row, &missing, directory.path(), &[], false);
        assert!(report.failed);
        assert!(
            report.line.starts_with("FAIL g7-unsafe-clock-refusal"),
            "{report:?}"
        );
        // Dev mode never neutralises a row: the INCOMPLETE labelling stands
        // and even the matched failure still reports as a failure to fix.
        let report = report_row_outcome(&row, &missing, directory.path(), &waivers, true);
        assert!(report.failed);
        assert!(
            report.line.starts_with("FAIL g7-unsafe-clock-refusal"),
            "{report:?}"
        );
    }

    #[test]
    fn a_waiver_over_a_passing_row_is_named_unused() {
        let row = scenarios::Row {
            id: "g1-leaf-copy",
            group: "G1",
            rust_implemented: true,
        };
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("g1-leaf-copy")).unwrap();
        fs::write(
            directory.path().join("g1-leaf-copy").join("observed.tsv"),
            "row_status\tfull on this host\n",
        )
        .unwrap();
        let waivers = vec![WaiveTarget {
            row: "g1-leaf-copy".into(),
            direction: None,
            reason: Some("should-not-apply".into()),
        }];
        let pass = scenarios::DirectionOutcome::Pass("rust-client/rust-server".into());
        let report = report_row_outcome(&row, &pass, directory.path(), &waivers, false);
        assert!(!report.failed, "{report:?}");
        assert!(
            report
                .line
                .contains("unused whole-row waiver (the row passed)"),
            "{report:?}"
        );
    }

    #[test]
    fn relative_subject_and_store_paths_are_made_cwd_independent() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("subject-bin");
        fs::write(&binary, b"subject").unwrap();
        // An existing subject path is validated and canonicalized (absolute)
        // so a child spawned from a scenario-owned cwd resolves it no
        // matter what cwd the driver was launched from.
        let canonical = canonical_subject_path(&binary, "--rust-bin").unwrap();
        assert!(canonical.is_absolute(), "{canonical:?}");
        let error =
            canonical_subject_path(&directory.path().join("absent"), "--rust-bin").unwrap_err();
        assert!(
            format!("{error:#}").contains("missing --rust-bin"),
            "{error:#}"
        );
        // Store roots may not exist yet: absolutize joins a relative path
        // onto the launch cwd without requiring existence.
        let absolute = absolutize(PathBuf::from("scratch/store")).unwrap();
        assert!(absolute.is_absolute(), "{absolute:?}");
        assert!(
            absolute.starts_with(std::env::current_dir().unwrap()),
            "{absolute:?}"
        );
        let already = absolutize(directory.path().to_path_buf()).unwrap();
        assert_eq!(already, directory.path());
    }

    #[test]
    fn pass_with_a_direction_waiver_names_the_waiver_and_its_evidence() {
        let row = scenarios::Row {
            id: "g3-store-ownership",
            group: "G3",
            rust_implemented: true,
        };
        let directory = tempfile::tempdir().unwrap();
        let direction_dir = directory
            .path()
            .join("g3-store-ownership")
            .join("java-client-rust-server");
        fs::create_dir_all(&direction_dir).unwrap();
        fs::write(direction_dir.join("INCOMPLETE"), b"named gap\n").unwrap();
        let waivers = vec![WaiveTarget {
            row: "g3-store-ownership".into(),
            direction: Some("java-client/rust-server".into()),
            reason: Some("the client subject never owns the store".into()),
        }];
        let pass = scenarios::DirectionOutcome::Pass("rust-client/rust-server".into());
        let row_dir = directory.path().join("g3-store-ownership");
        let report = report_row_outcome(&row, &pass, &row_dir, &waivers, false);
        assert!(!report.failed, "{report:?}");
        assert!(
            report.line.starts_with(
                "PASS g3-store-ownership rust-client/rust-server (waived \
                              java-client/rust-server: the client subject never owns the store"
            ),
            "{report:?}"
        );
        assert!(report.line.contains("named-gap evidence"), "{report:?}");
        // Without the marker but with the direction directory (the
        // direction ran and passed), the waiver is named unused instead.
        fs::remove_file(direction_dir.join("INCOMPLETE")).unwrap();
        let report = report_row_outcome(&row, &pass, &row_dir, &waivers, false);
        assert!(!report.failed, "{report:?}");
        assert!(
            report.line.contains("unused direction waiver"),
            "{report:?}"
        );
        // Without even the direction directory the waiver fails the row:
        // it names a direction that never ran and left no evidence.
        fs::remove_dir_all(&direction_dir).unwrap();
        let report = report_row_outcome(&row, &pass, &row_dir, &waivers, false);
        assert!(report.failed, "{report:?}");
        assert!(
            report.line.contains("no named-gap INCOMPLETE evidence"),
            "{report:?}"
        );
    }
}
