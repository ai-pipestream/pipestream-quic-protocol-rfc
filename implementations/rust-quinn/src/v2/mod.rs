//! Version-2 typed wire contract. This module does not activate an endpoint profile.
//!
//! Decoding checks representation and context-free invariants. Session identity,
//! authorization, negotiated budgets and retained-state commitments still require
//! validation by the authority/client before a message can affect durable state.

mod codec;
mod commitments;
mod correlation;
mod messages;
mod negotiation;
mod records;
mod transfer;
mod uri;

pub use commitments::*;
pub use correlation::*;
pub use messages::*;
pub use negotiation::*;
pub use records::*;
pub use transfer::*;
pub use uri::{ResultLocator, ResultTarget};

pub const ALPN: &[u8] = b"pipestream/2";
pub const DURABLE_WORK: u16 = 65284;
pub const RESULT_DELIVERY: u16 = 65285;
pub const MAX_NUMBER: u64 = i64::MAX as u64;
pub const MAX_DURATION: u64 = 31_536_000_000;
pub const INITIAL_CONTROL_LIMIT: usize = 4096;
pub const MAX_CONTROL_LIMIT: usize = 1_048_576;
pub const MAX_HEADER: usize = 4096;

/// Section 12.2 refusal codes, distinct from the version-1 error namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    FrameError = 1,
    ExtensionUnsupported,
    Unauthorized,
    LimitExceeded,
    NotFound,
    Expired,
    Conflict,
    IntegrityError,
    NotReady,
    WaitTimeout,
    DeadlineExceeded,
    Cancelled,
    ApplicationUnsupported,
    ControlReset,
    InternalError,
    OutputUnavailable,
    ClockUnsafe,
    AlreadyTerminal,
}

impl ErrorCode {
    pub fn name(self) -> &'static str {
        match self {
            Self::FrameError => "FRAME_ERROR",
            Self::ExtensionUnsupported => "EXTENSION_UNSUPPORTED",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::NotFound => "NOT_FOUND",
            Self::Expired => "EXPIRED",
            Self::Conflict => "CONFLICT",
            Self::IntegrityError => "INTEGRITY_ERROR",
            Self::NotReady => "NOT_READY",
            Self::WaitTimeout => "WAIT_TIMEOUT",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::Cancelled => "CANCELLED",
            Self::ApplicationUnsupported => "APPLICATION_UNSUPPORTED",
            Self::ControlReset => "CONTROL_RESET",
            Self::InternalError => "INTERNAL_ERROR",
            Self::OutputUnavailable => "OUTPUT_UNAVAILABLE",
            Self::ClockUnsafe => "CLOCK_UNSAFE",
            Self::AlreadyTerminal => "ALREADY_TERMINAL",
        }
    }

    pub fn quic_error(self) -> u64 {
        0x200 + self as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    pub detail: &'static str,
}

impl Error {
    pub(crate) fn new(code: ErrorCode, detail: &'static str) -> Self {
        Self { code, detail }
    }

    pub(crate) fn frame(detail: &'static str) -> Self {
        Self::new(ErrorCode::FrameError, detail)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.name(), self.detail)
    }
}

impl std::error::Error for Error {}

pub(crate) fn require(condition: bool, detail: &'static str) -> Result<(), Error> {
    if condition {
        Ok(())
    } else {
        Err(Error::frame(detail))
    }
}

#[cfg(test)]
mod tests;
