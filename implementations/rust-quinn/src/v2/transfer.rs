use super::{codec::Wire, *};
use sha2::{Digest as _, Sha256};
use std::time::{Duration as Elapsed, Instant};

/// Evidence from a completed validation, not a buffer or a durable publication.
/// A receiver may persist the bytes incrementally but cannot construct this
/// evidence until it has checked length, digest, FIN and stream deadlines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPayload {
    length: Number,
    sha256: Digest,
}

impl VerifiedPayload {
    pub fn length(&self) -> Number {
        self.length
    }
    pub fn sha256(&self) -> Digest {
        self.sha256
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Receiving,
    Verified,
    Failed,
}

/// Constant-memory validation over borrowed receive chunks. It neither buffers
/// the object nor invokes an application callback. Stream abort/durable input
/// installation and result visibility remain the endpoint's responsibility.
pub struct PayloadReceiver {
    length: Number,
    expected: Digest,
    received: u64,
    hash: Sha256,
    started: Instant,
    last_progress: Instant,
    idle: Elapsed,
    lifetime: Elapsed,
    phase: Phase,
}

impl PayloadReceiver {
    pub fn new(
        length: Number,
        expected: Digest,
        limits: &Capabilities,
        now: Instant,
    ) -> Result<Self, Error> {
        length.check()?;
        limits.check()?;
        if length.0 > limits.object_limit.0 {
            return Err(Error::new(
                ErrorCode::LimitExceeded,
                "payload exceeds negotiated object limit",
            ));
        }
        Ok(Self {
            length,
            expected,
            received: 0,
            hash: Sha256::new(),
            started: now,
            last_progress: now,
            idle: Elapsed::from_millis(limits.stream_idle_ms.0),
            lifetime: Elapsed::from_millis(limits.stream_lifetime_ms.0),
            phase: Phase::Receiving,
        })
    }

    fn fail<T>(&mut self, code: ErrorCode, detail: &'static str) -> Result<T, Error> {
        self.phase = Phase::Failed;
        Err(Error::new(code, detail))
    }

    /// Must also be driven by an endpoint timer when no payload arrives. An
    /// empty chunk does not extend idle time; progress never extends lifetime.
    pub fn check_deadline(&mut self, now: Instant) -> Result<(), Error> {
        if self.phase != Phase::Receiving {
            return Err(Error::frame("payload receiver is no longer receiving"));
        }
        let Some(total) = now.checked_duration_since(self.started) else {
            return self.fail(ErrorCode::ClockUnsafe, "stream monotonic clock regressed");
        };
        let Some(idle) = now.checked_duration_since(self.last_progress) else {
            return self.fail(ErrorCode::ClockUnsafe, "stream monotonic clock regressed");
        };
        if total >= self.lifetime || idle >= self.idle {
            return self.fail(ErrorCode::LimitExceeded, "payload stream deadline reached");
        }
        Ok(())
    }

    pub fn receive(&mut self, chunk: &[u8], now: Instant) -> Result<(), Error> {
        self.check_deadline(now)?;
        if chunk.len() as u64 > self.length.0 - self.received {
            return self.fail(
                ErrorCode::IntegrityError,
                "payload exceeds committed length",
            );
        }
        self.hash.update(chunk);
        self.received += chunk.len() as u64;
        if !chunk.is_empty() {
            self.last_progress = now;
        }
        Ok(())
    }

    /// Call only for a successful QUIC FIN, never for EOF caused by reset or
    /// connection loss. A premature/late FIN is not a verified empty result.
    pub fn finish(&mut self, now: Instant) -> Result<VerifiedPayload, Error> {
        self.check_deadline(now)?;
        if self.received != self.length.0 {
            return self.fail(ErrorCode::IntegrityError, "truncated payload");
        }
        let actual = Digest(self.hash.clone().finalize().into());
        if actual != self.expected {
            return self.fail(ErrorCode::IntegrityError, "payload digest mismatch");
        }
        self.phase = Phase::Verified;
        Ok(VerifiedPayload {
            length: self.length,
            sha256: actual,
        })
    }
}
