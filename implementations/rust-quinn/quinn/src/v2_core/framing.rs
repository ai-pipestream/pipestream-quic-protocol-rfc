//! Bounded control I/O. Cancelling either operation abandons this control stream;
//! a caller must close the connection, never resume a partially consumed frame.

use super::failure;
use anyhow::Result;
use pipestream_core::v2::{Control, ErrorCode, control_body_length};
use tokio::time::Instant;

pub(crate) enum Frame {
    Control(Control),
    Ignored,
    Fin,
}

async fn read(
    recv: &mut quinn::RecvStream,
    bytes: &mut [u8],
    deadline: Instant,
) -> Result<Option<usize>> {
    if Instant::now() >= deadline {
        return Err(failure(ErrorCode::LimitExceeded, "control receive deadline").into());
    }
    let result = tokio::time::timeout_at(deadline, recv.read(bytes))
        .await
        .map_err(|_| failure(ErrorCode::LimitExceeded, "control receive deadline"))?;
    if Instant::now() >= deadline {
        return Err(failure(ErrorCode::LimitExceeded, "control receive deadline").into());
    }
    match result {
        Ok(n) => Ok(n),
        Err(quinn::ReadError::Reset(_)) => {
            Err(failure(ErrorCode::ControlReset, "control reset").into())
        }
        Err(error) => Err(error.into()),
    }
}

async fn fill(recv: &mut quinn::RecvStream, bytes: &mut [u8], deadline: Instant) -> Result<()> {
    let mut used = 0;
    while used < bytes.len() {
        let n = read(recv, &mut bytes[used..], deadline)
            .await?
            .ok_or_else(|| failure(ErrorCode::FrameError, "truncated control frame"))?;
        used += n;
    }
    Ok(())
}

pub(crate) async fn receive(
    recv: &mut quinn::RecvStream,
    limit: Option<usize>,
    deadline: Instant,
) -> Result<Frame> {
    let mut prefix = [0; 5];
    let Some(n) = read(recv, &mut prefix, deadline).await? else {
        return Ok(Frame::Fin);
    };
    remainder(recv, prefix, n, limit, deadline).await
}

/// Between negotiated frames a healthy connection may have only data or a
/// long-running request in flight. Start the frame deadline at its first byte,
/// not while waiting for a new frame. Cancellation abandons the control stream.
pub(crate) async fn receive_next(
    recv: &mut quinn::RecvStream,
    limit: usize,
    timeout: std::time::Duration,
) -> Result<Frame> {
    let mut prefix = [0; 5];
    let n = match recv.read(&mut prefix).await {
        Ok(Some(n)) => n,
        Ok(None) => return Ok(Frame::Fin),
        Err(quinn::ReadError::Reset(_)) => {
            return Err(failure(ErrorCode::ControlReset, "control reset").into());
        }
        Err(error) => return Err(error.into()),
    };
    remainder(recv, prefix, n, Some(limit), Instant::now() + timeout).await
}

async fn remainder(
    recv: &mut quinn::RecvStream,
    mut prefix: [u8; 5],
    n: usize,
    limit: Option<usize>,
    deadline: Instant,
) -> Result<Frame> {
    fill(recv, &mut prefix[n..], deadline).await?;
    let length = control_body_length(prefix, limit)?;
    match prefix[0] {
        1..=7 => {
            // Only a validated known type/length can allocate a body buffer.
            let mut bytes = vec![0; 5 + length];
            bytes[..5].copy_from_slice(&prefix);
            fill(recv, &mut bytes[5..], deadline).await?;
            Ok(Frame::Control(Control::decode(
                &bytes,
                limit.unwrap_or(4096),
            )?))
        }
        0x80..=0xbf => {
            let mut scratch = [0; 4096];
            let mut remaining = length;
            while remaining != 0 {
                let count = remaining.min(scratch.len());
                fill(recv, &mut scratch[..count], deadline).await?;
                remaining -= count;
            }
            Ok(Frame::Ignored)
        }
        0xc0..=0xff => Err(failure(
            ErrorCode::ExtensionUnsupported,
            "private frame not activated",
        )
        .into()),
        _ => Err(failure(ErrorCode::FrameError, "unknown required control type").into()),
    }
}

pub(super) async fn send(
    send: &mut quinn::SendStream,
    message: &Control,
    limit: usize,
    deadline: Instant,
) -> Result<()> {
    let bytes = message.encode(limit)?;
    if Instant::now() >= deadline {
        return Err(failure(ErrorCode::LimitExceeded, "control send deadline").into());
    }
    let result = tokio::time::timeout_at(deadline, send.write_all(&bytes))
        .await
        .map_err(|_| failure(ErrorCode::LimitExceeded, "control send deadline"))?;
    if Instant::now() >= deadline {
        return Err(failure(ErrorCode::LimitExceeded, "control send deadline").into());
    }
    match result {
        Ok(()) => Ok(()),
        Err(quinn::WriteError::Stopped(_)) => {
            Err(failure(ErrorCode::ControlReset, "control stopped").into())
        }
        Err(error) => Err(error.into()),
    }
}
