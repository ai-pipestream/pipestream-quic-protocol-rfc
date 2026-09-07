use super::*;

/// Only client request variants can enter the writer. Replacing the local
/// correlation number does not touch any durable operation commitment.
pub(super) fn number(control: &mut Control, id: Id) -> Result<()> {
    let target = match control {
        Control::Session(
            Session::Create { request, .. }
            | Session::Attach { request, .. }
            | Session::NextSequence { request },
        )
        | Control::Scope(
            Scope::Declare { request, .. }
            | Scope::Page { request, .. }
            | Scope::Checkpoint { request, .. }
            | Scope::Cancel { request, .. },
        )
        | Control::Work(
            Work::Operation { request, .. }
            | Work::Watch { request, .. }
            | Work::Retry { request, .. }
            | Work::Cancel { request, .. }
            | Work::Skip { request, .. },
        )
        | Control::Result(
            ResultMessage::Read { request, .. } | ResultMessage::GetManifest { request, .. },
        )
        | Control::Drain(Drain::Complete { request, .. } | Drain::Detach { request }) => request,
        _ => return Err(error(ErrorCode::FrameError, "not a client control request")),
    };
    if id.0 > MAX_NUMBER {
        return Err(error(
            ErrorCode::LimitExceeded,
            "request IDs exhausted; reconnect",
        ));
    }
    *target = id;
    Ok(())
}
