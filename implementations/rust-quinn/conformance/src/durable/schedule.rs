//! interface-v1 fault schedule parser, validator, and milestone-1 executor.
//!
//! Eight columns per row: version, run_id, scenario_id, target, boundary,
//! action, seed, deadline_ms. Rows apply in file order. Actions that require
//! subject hooks (`pause`, `drop-reply`) report UNSUPPORTED in this milestone
//! rather than being silently skipped.

use crate::durable::events::{escape_label, unescape_label};
use anyhow::{Context, Result, bail, ensure};
use std::collections::BTreeSet;

pub const SCHEDULE_VERSION: &str = "1";

/// §2.1 boundary labels, used to validate schedule arming points.
pub const BOUNDARIES: &[&str] = &[
    "LISTENING",
    "CONNECTION_AUTHENTICATED",
    "SESSION_COMMITTED",
    "SESSION_RESPONSE_SENT",
    "DECLARATION_COMMITTED",
    "DECLARATION_RESPONSE_SENT",
    "INPUT_INSTALLED",
    "ADMISSION_COMMITTED",
    "ADMISSION_RESPONSE_SENT",
    "EXECUTION_CLAIMED",
    "OUTPUT_INSTALLED",
    "PUBLICATION_COMMITTED",
    "RETRY_COMMITTED",
    "FENCE_COMMITTED",
    "CLOSURE_COMMITTED",
    "RESULT_HEADER_SENT",
    "RESULT_FIN_SENT",
    "COMPLETE_RESPONSE_SENT",
    "DETACH_ACKNOWLEDGED",
    "REFUSAL_SENT",
    "SHUTDOWN_DRAINED",
    "INTENT_JOURNALED",
    "REQUEST_SENT",
    "RECEIPT_VALIDATED",
    "RECEIPT_JOURNALED",
    "OBSERVATION_JOURNALED",
    "RESULT_VERIFIED",
    "RESULT_INSTALLED",
    "REFUSAL_RECEIVED",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Pause,
    Release,
    Disconnect,
    DropReply,
    Stop,
    Kill,
    Restart,
    ClockSet,
}

impl Action {
    pub fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "pause" => Self::Pause,
            "release" => Self::Release,
            "disconnect" => Self::Disconnect,
            "drop-reply" => Self::DropReply,
            "stop" => Self::Stop,
            "kill" => Self::Kill,
            "restart" => Self::Restart,
            "clock-set" => Self::ClockSet,
            other => bail!("unknown schedule action: {other:?}"),
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Release => "release",
            Self::Disconnect => "disconnect",
            Self::DropReply => "drop-reply",
            Self::Stop => "stop",
            Self::Kill => "kill",
            Self::Restart => "restart",
            Self::ClockSet => "clock-set",
        }
    }

    /// Whether the action acts through a subject fixture hook (pause,
    /// drop-reply) rather than the fixture's own process lifecycle.
    /// Retained from the pre-hook milestone; no current caller filters on it.
    #[allow(dead_code)]
    pub fn requires_hook(self) -> bool {
        matches!(self, Self::Pause | Self::DropReply)
    }
}

// Row fields beyond the action/boundary/target triple are validated at parse
// time and are consumed by the milestone-2 fault executor.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRow {
    pub run_id: String,
    pub scenario_id: String,
    pub target: String,
    pub boundary: String,
    pub action: Action,
    pub seed: u64,
    pub deadline_ms: u64,
}

fn decimal(name: &str, value: &str, location: &str) -> Result<u64> {
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        "{location}: {name} must be a decimal integer, got {value:?}"
    );
    value
        .parse()
        .with_context(|| format!("{location}: {name} does not fit a 64-bit decimal integer"))
}

pub fn parse(text: &str, expected_run: &str, expected_scenario: &str) -> Result<Vec<ScheduleRow>> {
    let mut rows = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let location = format!("schedule line {}", index + 1);
        let fields = raw.split('\t').collect::<Vec<_>>();
        ensure!(
            fields.len() == 8,
            "{location}: expected 8 columns, got {}",
            fields.len()
        );
        ensure!(
            fields[0] == SCHEDULE_VERSION,
            "{location}: unknown schedule version {}",
            fields[0]
        );
        let run_id = unescape_label(fields[1])?;
        let scenario_id = unescape_label(fields[2])?;
        ensure!(
            run_id == expected_run,
            "{location}: run_id {run_id:?} does not match the run directory {expected_run:?}"
        );
        ensure!(
            scenario_id == expected_scenario,
            "{location}: scenario_id {scenario_id:?} does not match scenario {expected_scenario:?}"
        );
        let target = unescape_label(fields[3])?;
        ensure!(!target.is_empty(), "{location}: target must not be empty");
        let boundary = unescape_label(fields[4])?;
        ensure!(
            BOUNDARIES.contains(&boundary.as_str()),
            "{location}: unknown boundary {boundary:?}"
        );
        let action = Action::parse(&unescape_label(fields[5])?)
            .with_context(|| format!("{location}: invalid action"))?;
        let seed = decimal("seed", fields[6], &location)?;
        let deadline_ms = decimal("deadline_ms", fields[7], &location)?;
        rows.push(ScheduleRow {
            run_id,
            scenario_id,
            target,
            boundary,
            action,
            seed,
            deadline_ms,
        });
    }
    validate(&rows)?;
    Ok(rows)
}

/// Render rows back into schedule TSV. Used by the writer side once the
/// first fault-injection rows land.
#[allow(dead_code)]
pub fn render(rows: &[ScheduleRow]) -> Result<String> {
    let mut text = String::new();
    for row in rows {
        text.push_str(SCHEDULE_VERSION);
        let action = row.action.as_str().to_owned();
        for field in [
            row.run_id.as_str(),
            row.scenario_id.as_str(),
            row.target.as_str(),
            row.boundary.as_str(),
            action.as_str(),
        ] {
            text.push('\t');
            text.push_str(&escape_label(field));
        }
        text.push_str(&format!("\t{}\t{}\n", row.seed, row.deadline_ms));
    }
    Ok(text)
}

/// Ordering rules: a `release` needs a prior `pause` on the same
/// target/boundary; a `restart` is valid only after a `stop`/`kill` on that
/// target. A boundary that never arrives is a run failure at execution time,
/// never a silent skip.
pub fn validate(rows: &[ScheduleRow]) -> Result<()> {
    let mut paused: BTreeSet<(&str, &str)> = BTreeSet::new();
    let mut terminated: BTreeSet<&str> = BTreeSet::new();
    let mut restarted: BTreeSet<&str> = BTreeSet::new();
    for (index, row) in rows.iter().enumerate() {
        let location = format!("schedule line {}", index + 1);
        match row.action {
            Action::Pause => {
                ensure!(
                    paused.insert((row.target.as_str(), row.boundary.as_str())),
                    "{location}: duplicate pause on {} at {}",
                    row.target,
                    row.boundary
                );
            }
            Action::Release => {
                ensure!(
                    paused.remove(&(row.target.as_str(), row.boundary.as_str())),
                    "{location}: release without a prior pause on {} at {}",
                    row.target,
                    row.boundary
                );
            }
            Action::Restart => {
                ensure!(
                    terminated.contains(row.target.as_str())
                        && !restarted.contains(row.target.as_str()),
                    "{location}: restart of {} requires one prior stop/kill and a fresh target",
                    row.target
                );
                restarted.insert(row.target.as_str());
            }
            Action::Stop | Action::Kill => {
                terminated.insert(row.target.as_str());
                restarted.remove(row.target.as_str());
            }
            Action::Disconnect | Action::DropReply | Action::ClockSet => {}
        }
    }
    Ok(())
}

/// Execute a schedule by delivering every row, in file order, to
/// `on_process_action`. The handler owns the semantics: process-lifecycle
/// actions (stop/kill/restart) drive the spawned process, and hook-dependent
/// actions act through the milestone-5 subject hooks — `pause` rows are
/// released by writing `<events dir>/release-<BOUNDARY>` at the scenario's
/// chosen moment, `drop-reply` rows proceed once the client observes
/// connection loss, and a scheduled server `kill` exits the subject itself
/// (code 86) at the armed boundary, which the handler observes as process
/// exit plus the subject's event record before restarting the same roots.
/// Targets whose subject has no published hook (the Java server until
/// Claude's FixtureMain lands) are the handler's job: those directions
/// report INCOMPLETE with the named gate, never skip-pass.
pub fn execute(
    rows: &[ScheduleRow],
    mut on_process_action: impl FnMut(&ScheduleRow) -> Result<()>,
) -> Result<()> {
    for row in rows {
        on_process_action(row)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(action: &str) -> String {
        format!("1\trun-a\tg1-leaf-copy\tserver\tREQUEST_SENT\t{action}\t7\t1000\n")
    }

    #[test]
    fn parses_all_actions() {
        for action in [
            "pause",
            "disconnect",
            "drop-reply",
            "stop",
            "kill",
            "clock-set",
        ] {
            let text = format!("1\trun-a\ts\tserver\tREQUEST_SENT\t{action}\t7\t1000\n");
            let parsed = parse(&text, "run-a", "s").unwrap();
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].action, Action::parse(action).unwrap());
        }
        let text = "1\trun-a\ts\tserver\tREQUEST_SENT\tpause\t7\t1000\n\
                    1\trun-a\ts\tserver\tREQUEST_SENT\trelease\t7\t1000\n\
                    1\trun-a\ts\tserver\tREQUEST_SENT\tkill\t7\t1000\n\
                    1\trun-a\ts\tserver\tREQUEST_SENT\trestart\t7\t1000\n";
        let parsed = parse(text, "run-a", "s").unwrap();
        assert_eq!(
            parsed.iter().map(|row| row.action).collect::<Vec<_>>(),
            vec![
                Action::Pause,
                Action::Release,
                Action::Kill,
                Action::Restart
            ]
        );
    }

    #[test]
    fn rejects_unknown_action() {
        let error = parse(&row("explode"), "run-a", "g1-leaf-copy").unwrap_err();
        assert!(format!("{error:#}").contains("unknown schedule action"));
    }

    #[test]
    fn rejects_wrong_column_count() {
        for text in [
            "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\n",
            "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\t7\t1000\textra\n",
        ] {
            let error = parse(text, "run-a", "s").unwrap_err();
            assert!(error.to_string().contains("expected 8 columns"));
        }
    }

    #[test]
    fn rejects_malformed_seed_and_deadline() {
        for text in [
            "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\t-1\t1000\n",
            "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\tabc\t1000\n",
            "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\t7\t1.5\n",
            "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\t7\t\n",
            "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\t18446744073709551616\t1000\n",
        ] {
            assert!(parse(text, "run-a", "s").is_err(), "accepted {text:?}");
        }
    }

    #[test]
    fn rejects_wrong_version_and_run_scenario_mismatch() {
        assert!(
            parse(
                "2\trun-a\ts\tserver\tREQUEST_SENT\tstop\t7\t1000\n",
                "run-a",
                "s"
            )
            .is_err()
        );
        let error = parse(&row("stop"), "run-b", "g1-leaf-copy").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match the run directory")
        );
        let error = parse(&row("stop"), "run-a", "other").unwrap_err();
        assert!(error.to_string().contains("does not match scenario"));
    }

    #[test]
    fn rejects_unknown_boundary() {
        let error = parse(
            "1\trun-a\ts\tserver\tNOT_A_BOUNDARY\tstop\t7\t1000\n",
            "run-a",
            "s",
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown boundary"));
    }

    #[test]
    fn rejects_release_without_pause_and_restart_without_kill() {
        let error = parse(&row("release"), "run-a", "g1-leaf-copy").unwrap_err();
        assert!(error.to_string().contains("release without a prior pause"));
        let error = parse(&row("restart"), "run-a", "g1-leaf-copy").unwrap_err();
        assert!(error.to_string().contains("restart of server requires"));
    }

    #[test]
    fn hook_actions_are_delivered_to_the_handler() {
        // Milestone 5 landed the subject hooks; hook rows now reach the
        // handler, which implements the driver-side semantics (release file,
        // connection-loss observation, self-kill wait + restart).
        let rows = parse(
            "1\trun-a\ts\tserver\tADMISSION_COMMITTED\tpause\t7\t1000\n\
             1\trun-a\ts\tserver\tPUBLICATION_COMMITTED\tdrop-reply\t7\t1000\n",
            "run-a",
            "s",
        )
        .unwrap();
        let mut seen = Vec::new();
        execute(&rows, |row| {
            seen.push(row.action);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![Action::Pause, Action::DropReply]);
    }

    #[test]
    fn lifecycle_actions_reach_the_process_delegate() {
        let text = "1\trun-a\ts\tserver\tREQUEST_SENT\tstop\t7\t1000\n\
                    1\trun-a\ts\tserver\tREQUEST_SENT\trestart\t7\t1000\n";
        let rows = parse(text, "run-a", "s").unwrap();
        let mut seen = Vec::new();
        execute(&rows, |row| {
            seen.push(row.action);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![Action::Stop, Action::Restart]);
    }

    #[test]
    fn render_parse_round_trip() {
        let rows = parse(
            "1\trun-a\ts\tworker-2\tREQUEST_SENT\tkill\t42\t250\n",
            "run-a",
            "s",
        )
        .unwrap();
        let text = render(&rows).unwrap();
        assert_eq!(parse(&text, "run-a", "s").unwrap(), rows);
    }
}
