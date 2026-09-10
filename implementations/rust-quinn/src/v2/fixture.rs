//! Test-only fixture hooks for the neutral conformance driver (interface v1).
//!
//! Arming surface: the `pipestream-server v2 serve --fixture-*` flags only,
//! installed once per process through [`arm`]. When no configuration is
//! installed every hook here is a cheap no-op. The hooks report, hold, withhold
//! or hard-exit at boundaries the production code actually reached; they never
//! forge commits, receipts or protocol results, and they never pause inside a
//! SQLite transaction.

use super::{Control, Drain, Scope, Session, Work};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Schema version of the event record and the fault schedule (interface v1).
const VERSION: &str = "1";
/// Maximum event records one process may publish for one run (interface v1 §1).
const MAX_RECORDS: u64 = 65_536;
/// Interface-v1 §2.1 server boundaries this subject can reach, in lifecycle
/// order. Boundaries the subject never reaches (for example INPUT_INSTALLED or
/// SHUTDOWN_DRAINED) are rejected at schedule parse time instead of failing a
/// run by never arriving.
pub const BOUNDARIES: &[&str] = &[
    "LISTENING",
    "CONNECTION_AUTHENTICATED",
    "SESSION_COMMITTED",
    "SESSION_RESPONSE_SENT",
    "DECLARATION_COMMITTED",
    "DECLARATION_RESPONSE_SENT",
    "ADMISSION_COMMITTED",
    "ADMISSION_RESPONSE_SENT",
    "EXECUTION_CLAIMED",
    "PUBLICATION_COMMITTED",
    "RETRY_COMMITTED",
    "FENCE_COMMITTED",
    "CLOSURE_COMMITTED",
    "COMPLETE_RESPONSE_SENT",
    "DETACH_ACKNOWLEDGED",
    "REFUSAL_SENT",
];
/// Committed boundaries whose reply is written on the same connection: the only
/// boundaries where `pause`, `drop-reply` or `disconnect` can act pre-queue.
/// This is the "three reply pairs" of the peer-agreed placement addendum.
const REPLY_PAIRS: &[&str] = &[
    "SESSION_COMMITTED",
    "DECLARATION_COMMITTED",
    "ADMISSION_COMMITTED",
];

static FIXTURE: OnceLock<Fixture> = OnceLock::new();
static SEQ: AtomicU64 = AtomicU64::new(0);

/// A parsed, target-filtered fault-schedule row that arms one boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arm {
    pub boundary: &'static str,
    pub action: Action,
    /// Hard bound for the hold, from the schedule row's `deadline_ms`.
    pub deadline: Duration,
}

/// A schedule action this subject can perform at a reached boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Hold the pending reply until a release file appears or the deadline
    /// elapses. The boundary's commit is untouched.
    Pause,
    /// Withhold the pending reply and reset the connection (lost-ACK shape).
    DropReply,
    /// Close the connection at the boundary without process death.
    Disconnect,
    /// Hard process exit (code 86, no destructors) at the boundary.
    Kill,
}

struct Fixture {
    run_id: String,
    scenario_id: String,
    process_start_id: String,
    /// Directory holding `events.tsv` and the release files a paused boundary
    /// polls for (`release-<BOUNDARY>` or `release-all`).
    directory: PathBuf,
    events: Mutex<File>,
    /// Armed schedule rows keyed by interface-v1 boundary label.
    armed: HashMap<&'static str, Vec<Arm>>,
    /// Per-boundary count of commits that actually fired in this process, so
    /// the reply gate only acts on a freshly committed operation and never on
    /// a replayed receipt committed by an earlier process.
    reached: Mutex<HashMap<&'static str, u64>>,
}

/// Installs the process-wide fixture configuration. Called once by
/// `pipestream-server v2 serve` when any `--fixture-*` flag is present.
/// Returns an error when the events file cannot be opened or the process is
/// already armed.
pub fn arm(
    events_path: &Path,
    run_id: &str,
    scenario_id: &str,
    arms: Vec<Arm>,
) -> Result<(), String> {
    let events = OpenOptions::new()
        .create(true)
        .append(true)
        .open(events_path)
        .map_err(|error| {
            format!(
                "cannot open fixture events {}: {error}",
                events_path.display()
            )
        })?;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let mut armed: HashMap<&'static str, Vec<Arm>> = HashMap::new();
    for arm in arms {
        armed.entry(arm.boundary).or_default().push(arm);
    }
    let fixture = Fixture {
        run_id: run_id.to_string(),
        scenario_id: scenario_id.to_string(),
        process_start_id: format!("{}-{:x}", std::process::id(), started.as_nanos()),
        directory: events_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default(),
        events: Mutex::new(events),
        armed,
        reached: Mutex::new(HashMap::new()),
    };
    FIXTURE
        .set(fixture)
        .map_err(|_| "fixture hooks are already armed in this process".to_string())
}

fn fixture() -> Option<&'static Fixture> {
    FIXTURE.get()
}

fn escape(label: &str) -> String {
    let mut escaped = String::with_capacity(label.len());
    for character in label.chars() {
        match character {
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\\' => escaped.push_str("\\\\"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn unescape(label: &str) -> Result<String, String> {
    let mut unescaped = String::with_capacity(label.len());
    let mut characters = label.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            unescaped.push(character);
            continue;
        }
        match characters.next() {
            Some('t') => unescaped.push('\t'),
            Some('n') => unescaped.push('\n'),
            Some('r') => unescaped.push('\r'),
            Some('\\') => unescaped.push('\\'),
            other => {
                return Err(format!("invalid escape in label {label:?}: {other:?}"));
            }
        }
    }
    Ok(unescaped)
}

/// Appends one complete interface-v1 §2 event record and fsyncs it, so a
/// reached-boundary claim is subject evidence rather than driver inference.
fn emit(fixture: &Fixture, boundary: &str) {
    let seq = SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    if seq > MAX_RECORDS {
        // The run bound is exceeded; missing evidence fails the run driver-side.
        return;
    }
    let mut record = String::new();
    let _ = writeln!(
        record,
        "{VERSION}\t{}\t{}\trust\tserver\t{}\t{seq}\t{}\t\t\t\t\t\t\t",
        escape(&fixture.run_id),
        escape(&fixture.scenario_id),
        fixture.process_start_id,
        escape(boundary),
    );
    let mut events = fixture
        .events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Err(error) = events
        .write_all(record.as_bytes())
        .and_then(|()| events.sync_data())
    {
        eprintln!("fixture event append failed: {error}");
    }
}

/// Hard process kill for a scheduled `kill` row: exit code 86 without running
/// destructors, abandoning SQLite state exactly like process death. The event
/// record for the boundary is appended and fsynced before the exit.
fn kill() -> ! {
    std::process::exit(86)
}

/// Boundary hook for a label the subject emits directly: emits the event
/// record when armed, then performs a scheduled kill at the boundary.
fn boundary_hook(boundary: &'static str) {
    let Some(fixture) = fixture() else { return };
    if let Some(arms) = fixture.armed.get(boundary) {
        emit(fixture, boundary);
        if arms.iter().any(|arm| arm.action == Action::Kill) {
            kill();
        }
    }
}

/// Maps the stable commit-funnel key ([`crate::v2::authority`]) to its
/// interface-v1 boundary label. Supplementary storage probes (`prepare-input`,
/// `worker-renew`, `worker-expansion`, `settlement-work`, `session-revoke`,
/// `result-read`, `retention-*`, `retirement-*`) have no §2.1 label: they
/// cannot be armed through a schedule and never emit an event record.
pub fn commit_label(key: &str) -> Option<&'static str> {
    Some(match key {
        "create" => "SESSION_COMMITTED",
        "declare" => "DECLARATION_COMMITTED",
        "admit-input" => "ADMISSION_COMMITTED",
        "worker-claim" => "EXECUTION_CLAIMED",
        "worker-publish" => "PUBLICATION_COMMITTED",
        "worker-retry" => "RETRY_COMMITTED",
        "work-fence" | "scope-fence" => "FENCE_COMMITTED",
        "settlement-scope" => "CLOSURE_COMMITTED",
        _ => return None,
    })
}

/// Commit-funnel hook, called from `commit()` only after `tx.commit()`
/// returned (never inside the transaction, so no SQLite write lock is held).
/// Emits the mapped event record when the boundary is armed, then performs a
/// scheduled kill. A kill is scheduled only in the `:after` position; there is
/// no armed `:before` arm, matching interface-v1 `kill` semantics
/// ("after the boundary's commit, before any further reply").
pub fn commit_boundary(key: &str) {
    let Some(fixture) = fixture() else { return };
    let Some(boundary) = commit_label(key) else {
        return;
    };
    if let Some(arms) = fixture.armed.get(boundary) {
        emit(fixture, boundary);
        fixture
            .reached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(boundary)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        if arms.iter().any(|arm| arm.action == Action::Kill) {
            kill();
        }
    }
}

/// How many commits at this boundary actually fired in this process. The
/// pre-queue reply gate compares this count around `Pending::run` so pause and
/// drop-reply act only on a freshly committed operation; a replayed receipt
/// (session attach, idempotent operation replay, admission replay) committed
/// by an earlier process passes without gating.
pub fn reached(boundary: &str) -> u64 {
    fixture().map_or(0, |fixture| {
        fixture
            .reached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(boundary)
            .copied()
            .unwrap_or(0)
    })
}

/// Classifies a control reply the writer just sent into its interface-v1
/// `*_SENT` boundary, if it has one.
pub fn sent_label(control: &Control) -> Option<&'static str> {
    Some(match control {
        Control::Session(Session::Binding { .. }) => "SESSION_RESPONSE_SENT",
        Control::Scope(Scope::Declared { .. }) => "DECLARATION_RESPONSE_SENT",
        Control::Work(Work::Admitted { .. }) => "ADMISSION_RESPONSE_SENT",
        Control::Drain(Drain::Completed { .. }) => "COMPLETE_RESPONSE_SENT",
        Control::Drain(Drain::Detached { .. }) => "DETACH_ACKNOWLEDGED",
        Control::Refusal(_) => "REFUSAL_SENT",
        _ => return None,
    })
}

/// Writer-task hook, called after the transport accepted the control write
/// (never from the job after `queue()`). Emits the `*_SENT` event record when
/// armed, then performs a scheduled kill at the SENT boundary.
pub fn sent_boundary(control: &Control) {
    let Some(fixture) = fixture() else { return };
    let Some(boundary) = sent_label(control) else {
        return;
    };
    if let Some(arms) = fixture.armed.get(boundary) {
        emit(fixture, boundary);
        if arms.iter().any(|arm| arm.action == Action::Kill) {
            kill();
        }
    }
}

/// Emits CONNECTION_AUTHENTICATED once per authenticated connection when the
/// boundary is armed, then performs a scheduled kill.
pub fn connection_authenticated() {
    boundary_hook("CONNECTION_AUTHENTICATED");
}

/// Emits LISTENING at serve startup when the boundary is armed, then performs
/// a scheduled kill.
pub fn listening() {
    boundary_hook("LISTENING");
}

/// What the pre-queue reply gate must do at a committed, reply-bearing
/// boundary, per the armed schedule.
#[derive(Debug, PartialEq, Eq)]
pub enum Gate {
    /// Queue the reply normally.
    Pass,
    /// Hold the pending reply until the release file appears or the deadline
    /// elapses, then queue it. The commit stands untouched.
    Hold {
        release: PathBuf,
        deadline: Duration,
    },
    /// Withhold the reply and reset the connection (lost-ACK shape).
    Drop,
}

/// Decides the pre-queue action for a committed, reply-bearing boundary.
/// Unarmed boundaries and supplementary keys always pass.
pub fn reply_gate(boundary: &str) -> Gate {
    let Some(fixture) = fixture() else {
        return Gate::Pass;
    };
    let Some(arms) = fixture.armed.get(boundary) else {
        return Gate::Pass;
    };
    if arms
        .iter()
        .any(|arm| matches!(arm.action, Action::DropReply | Action::Disconnect))
    {
        return Gate::Drop;
    }
    if let Some(pause) = arms.iter().find(|arm| arm.action == Action::Pause) {
        return Gate::Hold {
            release: fixture.directory.join(format!("release-{boundary}")),
            deadline: pause.deadline,
        };
    }
    Gate::Pass
}

fn parse_boundary(field: &str, path: &Path, row: usize) -> Result<&'static str, String> {
    BOUNDARIES
        .iter()
        .copied()
        .find(|boundary| *boundary == field)
        .ok_or_else(|| format!("{}:{row}: unknown boundary {field:?}", path.display()))
}

fn parse_deadline(field: &str, path: &Path, row: usize) -> Result<Duration, String> {
    let millis: u64 = field
        .parse()
        .map_err(|_| format!("{}:{row}: invalid deadline_ms {field:?}", path.display()))?;
    Ok(Duration::from_millis(millis))
}

/// Parses the interface-v1 §3 fault schedule (8-column TSV) into the rows that
/// arm this subject. Rows for other targets and `release` rows belong to the
/// driver and are dropped. Rejects unknown versions, columns, boundaries and
/// actions; rejects `pause`/`drop-reply`/`disconnect` outside the three reply
/// pairs; rejects driver-side actions (`stop`, `restart`, `clock-set`) on the
/// server target. `run_id`/`scenario_id` must match the armed run.
pub fn parse_schedule(path: &Path, run_id: &str, scenario_id: &str) -> Result<Vec<Arm>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read fixture schedule {}: {error}", path.display()))?;
    let mut arms = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let row = index + 1;
        let columns: Vec<&str> = line.split('\t').collect();
        if columns.len() != 8 {
            return Err(format!(
                "{}:{row}: expected 8 schedule columns, found {}",
                path.display(),
                columns.len()
            ));
        }
        if columns[0] != VERSION {
            return Err(format!(
                "{}:{row}: unknown schedule version {:?}",
                path.display(),
                columns[0]
            ));
        }
        if unescape(columns[1])? != run_id {
            return Err(format!(
                "{}:{row}: run_id does not match --fixture-run {run_id:?}",
                path.display()
            ));
        }
        if unescape(columns[2])? != scenario_id {
            return Err(format!(
                "{}:{row}: scenario_id does not match --fixture-scenario {scenario_id:?}",
                path.display()
            ));
        }
        if columns[3] != "server" {
            // Other targets (client, worker-N) are the driver's business.
            continue;
        }
        let boundary = parse_boundary(&unescape(columns[4])?, path, row)?;
        let deadline = parse_deadline(columns[7], path, row)?;
        let action = match columns[5] {
            "release" => continue,
            "stop" | "restart" | "clock-set" => {
                return Err(format!(
                    "{}:{row}: action {:?} is driver-side; the Rust subject cannot perform it",
                    path.display(),
                    columns[5]
                ));
            }
            "pause" => {
                if !REPLY_PAIRS.contains(&boundary) {
                    return Err(format!(
                        "{}:{row}: pause requires a committed reply-pair boundary, not {boundary:?}",
                        path.display()
                    ));
                }
                Action::Pause
            }
            "drop-reply" => {
                if !REPLY_PAIRS.contains(&boundary) {
                    return Err(format!(
                        "{}:{row}: drop-reply requires a committed reply-pair boundary, not {boundary:?}",
                        path.display()
                    ));
                }
                Action::DropReply
            }
            "disconnect" => {
                if !REPLY_PAIRS.contains(&boundary) {
                    return Err(format!(
                        "{}:{row}: disconnect requires a committed reply-pair boundary, not {boundary:?}",
                        path.display()
                    ));
                }
                Action::Disconnect
            }
            "kill" => Action::Kill,
            other => {
                return Err(format!(
                    "{}:{row}: unknown schedule action {other:?}",
                    path.display()
                ));
            }
        };
        arms.push(Arm {
            boundary,
            action,
            deadline,
        });
    }
    Ok(arms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn schedule(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        file
    }

    #[test]
    fn schedule_parse_keeps_server_rows_and_validates_actions() {
        let file = schedule(
            "1\trun-a\tscen-1\tserver\tSESSION_COMMITTED\tkill\t0\t1000\n\
             1\trun-a\tscen-1\tserver\tADMISSION_COMMITTED\tpause\t0\t2000\n\
             1\trun-a\tscen-1\tserver\tDECLARATION_COMMITTED\tdrop-reply\t0\t3000\n\
             1\trun-a\tscen-1\tclient\tREQUEST_SENT\tkill\t0\t1000\n\
             1\trun-a\tscen-1\tserver\tADMISSION_COMMITTED\trelease\t0\t0\n",
        );
        let arms = parse_schedule(file.path(), "run-a", "scen-1").unwrap();
        assert_eq!(arms.len(), 3);
        assert_eq!(arms[0].boundary, "SESSION_COMMITTED");
        assert_eq!(arms[0].action, Action::Kill);
        assert_eq!(arms[1].action, Action::Pause);
        assert_eq!(arms[1].deadline, Duration::from_millis(2000));
        assert_eq!(arms[2].action, Action::DropReply);
    }

    #[test]
    fn schedule_parse_rejects_invalid_rows() {
        let cases: &[&str] = &[
            "1\trun-a\tscen-1\tserver\tSESSION_COMMITTED\tkill\t0\t1000\textra",
            "2\trun-a\tscen-1\tserver\tSESSION_COMMITTED\tkill\t0\t1000",
            "1\trun-b\tscen-1\tserver\tSESSION_COMMITTED\tkill\t0\t1000",
            "1\trun-a\tscen-1\tserver\tNOT_A_BOUNDARY\tkill\t0\t1000",
            "1\trun-a\tscen-1\tserver\tSESSION_COMMITTED\texplode\t0\t1000",
            "1\trun-a\tscen-1\tserver\tFENCE_COMMITTED\tdrop-reply\t0\t1000",
            "1\trun-a\tscen-1\tserver\tSESSION_RESPONSE_SENT\tpause\t0\t1000",
            "1\trun-a\tscen-1\tserver\tSESSION_COMMITTED\tstop\t0\t1000",
            "1\trun-a\tscen-1\tserver\tSESSION_COMMITTED\tkill\t0\tsoon",
        ];
        for case in cases {
            let file = schedule(case);
            assert!(
                parse_schedule(file.path(), "run-a", "scen-1").is_err(),
                "schedule row must be rejected: {case:?}"
            );
        }
    }

    #[test]
    fn commit_label_maps_only_durable_boundaries() {
        assert_eq!(commit_label("create"), Some("SESSION_COMMITTED"));
        assert_eq!(commit_label("declare"), Some("DECLARATION_COMMITTED"));
        assert_eq!(commit_label("admit-input"), Some("ADMISSION_COMMITTED"));
        assert_eq!(commit_label("worker-claim"), Some("EXECUTION_CLAIMED"));
        assert_eq!(
            commit_label("worker-publish"),
            Some("PUBLICATION_COMMITTED")
        );
        assert_eq!(commit_label("worker-retry"), Some("RETRY_COMMITTED"));
        assert_eq!(commit_label("work-fence"), Some("FENCE_COMMITTED"));
        assert_eq!(commit_label("scope-fence"), Some("FENCE_COMMITTED"));
        assert_eq!(commit_label("settlement-scope"), Some("CLOSURE_COMMITTED"));
        // Supplementary storage probes have no interface-v1 boundary label.
        for key in [
            "prepare-input",
            "worker-renew",
            "worker-expansion",
            "settlement-work",
            "session-revoke",
            "result-read",
            "retention-intent",
            "retirement-finish",
        ] {
            assert_eq!(commit_label(key), None, "{key} must stay supplementary");
        }
    }

    #[test]
    fn armed_fixture_emits_interface_v1_records_and_gates() {
        let directory = tempfile::tempdir().unwrap();
        let events = directory.path().join("events.tsv");
        // No Kill arm here: the configuration is process-global, and a armed
        // kill would exit(86) every later commit in this test process. The
        // kill path is covered by the release-binary smoke run instead.
        let arms = vec![
            Arm {
                boundary: "SESSION_COMMITTED",
                action: Action::Pause,
                deadline: Duration::from_millis(1000),
            },
            Arm {
                boundary: "ADMISSION_COMMITTED",
                action: Action::Pause,
                deadline: Duration::from_millis(2000),
            },
            Arm {
                boundary: "DECLARATION_COMMITTED",
                action: Action::DropReply,
                deadline: Duration::from_millis(1000),
            },
        ];
        arm(&events, "run-1", "scen-9", arms).unwrap();
        assert!(arm(&events, "run-1", "scen-9", Vec::new()).is_err());

        let fixture = fixture().unwrap();
        emit(fixture, "SESSION_COMMITTED");
        emit(fixture, "ADMISSION_COMMITTED");
        // Tests committing concurrently in this process append records too;
        // every record must be well-formed and carry this run and scenario.
        let mut text = String::new();
        File::open(&events)
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines.len() >= 2, "expected at least our two records");
        let mut boundaries = Vec::new();
        for line in &lines {
            let columns: Vec<&str> = line.split('\t').collect();
            assert_eq!(columns.len(), 15, "record must have 15 columns: {line:?}");
            assert_eq!(columns[0], "1");
            assert_eq!(columns[1], "run-1");
            assert_eq!(columns[2], "scen-9");
            assert_eq!(columns[3], "rust");
            assert_eq!(columns[4], "server");
            assert!(
                columns[6].parse::<u64>().is_ok(),
                "seq must be decimal: {line:?}"
            );
            assert!(columns[8..].iter().all(|column| column.is_empty()));
            boundaries.push(columns[7]);
        }
        assert!(boundaries.contains(&"SESSION_COMMITTED"));
        assert!(boundaries.contains(&"ADMISSION_COMMITTED"));
        assert_eq!(reached("NOT_A_BOUNDARY"), 0);

        match reply_gate("SESSION_COMMITTED") {
            Gate::Hold { deadline, .. } => {
                assert_eq!(deadline, Duration::from_millis(1000));
            }
            other => panic!("expected Hold for armed pause, got {other:?}"),
        }
        match reply_gate("ADMISSION_COMMITTED") {
            Gate::Hold { release, deadline } => {
                assert_eq!(
                    release,
                    directory.path().join("release-ADMISSION_COMMITTED")
                );
                assert_eq!(deadline, Duration::from_millis(2000));
            }
            other => panic!("expected Hold for armed pause, got {other:?}"),
        }
        assert_eq!(reply_gate("DECLARATION_COMMITTED"), Gate::Drop);
        match reply_gate("FENCE_COMMITTED") {
            Gate::Pass => {}
            other => panic!("unarmed boundary must pass, got {other:?}"),
        }
    }

    #[test]
    fn label_escaping_round_trips() {
        assert_eq!(escape("a\tb\nc\\d\re"), "a\\tb\\nc\\\\d\\re");
        assert_eq!(unescape("a\\tb\\nc\\\\d\\re").unwrap(), "a\tb\nc\\d\re");
        assert!(unescape("bad\\x").is_err());
    }
}
