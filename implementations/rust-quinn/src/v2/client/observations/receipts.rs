//! Validate commitments without inventing an observation revision or treating
//! arrival order as commit order. Both records have already passed wire checks.
use super::*;

pub(super) fn target(intent: &Intent) -> Option<&WorkKey> {
    match &intent.mutation {
        Mutation::Admit(p) => Some(&p.work),
        Mutation::Retry { work, .. } | Mutation::Cancel { work } | Mutation::Skip { work } => {
            Some(work)
        }
        _ => None,
    }
}

// An accepted fence promises an eventual outcome, not an already stopped worker.
fn fence(receipt: &Outcome) -> Option<(State, Number, Disposition)> {
    let (expected, time, disposition, state) = match receipt {
        Outcome::Cancelled {
            accepted_at,
            disposition,
            state_at_commit,
            ..
        } => (
            State::CANCELLED,
            *accepted_at,
            *disposition,
            *state_at_commit,
        ),
        Outcome::Skipped {
            accepted_at,
            disposition,
            state_at_commit,
            ..
        } => (State::SKIPPED, *accepted_at, *disposition, *state_at_commit),
        _ => return None,
    };
    Some((
        if disposition.0 == 0 { expected } else { state },
        time,
        disposition,
    ))
}

fn terminal(receipt: &Outcome, state: State, attempt: Number, committed: Number) -> Result<()> {
    if let Some((expected, accepted, disposition)) = fence(receipt) {
        check(
            state == expected,
            "terminal outcome contradicts accepted fence receipt",
        )?;
        check(
            if disposition.0 == 0 {
                committed >= accepted
            } else {
                committed <= accepted
            },
            "terminal time contradicts fence disposition",
        )?;
    }
    if let Outcome::Retried {
        replacement_attempt,
        accepted_at,
        ..
    } = receipt
    {
        check(
            attempt.0 >= replacement_attempt.0 && committed >= *accepted_at,
            "terminal outcome predates accepted replacement attempt",
        )?;
    }
    Ok(())
}

pub(super) fn manifest(intent: &Intent, receipt: &Outcome, manifest: &Manifest) -> Result<()> {
    if let Mutation::Admit(p) = &intent.mutation {
        let Outcome::Admitted {
            admitted_at,
            deadline,
            ..
        } = receipt
        else {
            return Err(JournalError::Corrupt("not an admission receipt"));
        };
        check(
            p.input.sha256 == manifest.input_sha256,
            "manifest input commitment changed",
        )?;
        check(
            manifest.outputs.len() as u64 <= p.outputs.count.0
                && manifest.outputs.iter().map(|o| o.length.0).sum::<u64>()
                    <= p.outputs.total_bytes.0,
            "manifest exceeds known output budget",
        )?;
        check(
            manifest.committed_at >= *admitted_at && manifest.committed_at < *deadline,
            "manifest commit outside known execution interval",
        )?;
    }
    terminal(
        receipt,
        State::SUCCEEDED,
        Number(manifest.attempt.0),
        manifest.committed_at,
    )
}

pub(super) fn view(intent: &Intent, receipt: &Outcome, view: &WorkView) -> Result<()> {
    if let Mutation::Admit(p) = &intent.mutation
        && (view.admitted_at.is_some() || view.state.is_terminal())
    {
        admission_matches_view(p, receipt, view)?;
    }
    if let Some(admitted) = view.admitted_at {
        if let Outcome::Retried { accepted_at, .. } = receipt {
            check(
                *accepted_at >= admitted && *accepted_at < view.deadline.expect("validated view"),
                "retry outside observed execution interval",
            )?;
        }
        if let Some((_, accepted, Disposition(0))) = fence(receipt) {
            check(
                admitted <= accepted,
                "admission follows accepted work fence",
            )?;
        }
    }
    if let Some(committed) = view.terminal_at {
        terminal(receipt, view.state, view.attempt, committed)?;
    }
    Ok(())
}

pub(super) fn pair(
    old: &Intent,
    prior: &OperationReceipt,
    new: &Intent,
    received: &OperationReceipt,
) -> Result<()> {
    check(target(old) == target(new), "receipt target index changed")?;
    if let (Mutation::Admit(a), Mutation::Admit(b)) = (&old.mutation, &new.mutation) {
        check(
            a == b && prior == received,
            "admission disagrees with known input",
        )?;
    }
    // Run directional checks both ways: delayed acknowledgments have no ordering guarantee.
    compatible_outcomes(&prior.body, &received.body)?;
    compatible_outcomes(&received.body, &prior.body)?;
    if let (
        Outcome::Retried {
            expected_attempt: a,
            accepted_at: ta,
            ..
        },
        Outcome::Retried {
            expected_attempt: b,
            accepted_at: tb,
            ..
        },
    ) = (&prior.body, &received.body)
    {
        check(
            if a == b {
                prior == received
            } else if a < b {
                ta <= tb
            } else {
                tb <= ta
            },
            "replacement attempt has conflicting operation or commit time",
        )?;
    }
    Ok(())
}

fn compatible_outcomes(first: &Outcome, second: &Outcome) -> Result<()> {
    if let Outcome::Admitted {
        admitted_at,
        deadline,
        ..
    } = first
    {
        if let Outcome::Retried { accepted_at, .. } = second {
            check(
                *accepted_at >= *admitted_at && *accepted_at < *deadline,
                "retry outside retained admission interval",
            )?;
        }
        if let Some((_, accepted, _)) = fence(second) {
            check(
                *admitted_at <= accepted,
                "admission follows terminal or accepted fence",
            )?;
        }
    }
    if let Some((outcome, accepted, disposition)) = fence(first) {
        if let Some((other, later, other_disposition)) = fence(second) {
            check(
                outcome == other,
                "work fences promise conflicting terminal outcomes",
            )?;
            if disposition.0 == 0 && other_disposition.0 == 1 {
                check(
                    accepted <= later,
                    "accepted fence follows reported terminal outcome",
                )?;
            }
        }
        if let Outcome::Retried { accepted_at, .. } = second {
            check(
                *accepted_at <= accepted,
                "replacement accepted after known exclusion fence",
            )?;
        }
    }
    Ok(())
}
