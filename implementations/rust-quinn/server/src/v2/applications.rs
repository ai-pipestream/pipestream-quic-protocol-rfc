//! Explicit pure processing contracts used by the runnable reference server.
//! These are applications, not additional wire profiles or arbitrary fallbacks.
use super::*;
use authority::{
    StoreError,
    execution::{
        Application, ApplicationOutcome, CopyApplication, Expansion, ExpansionContext,
        ExpansionOutcome, WorkContext,
    },
    ingress::{Applications, InputPreparation, InputReception, RestartSafety},
};
use sha2::{Digest as _, Sha256};
use std::{sync::Arc, time::Instant};
type AppResult<T> = std::result::Result<T, StoreError>;

pub fn registry() -> Result<Arc<Applications>> {
    let mut apps = Applications::default();
    for (name, modes, implementation) in [
        (
            "copy/v2",
            vec![Mode(0)],
            Arc::new(CopyApplication) as Arc<dyn Application>,
        ),
        ("consume/v2", vec![Mode(0)], Arc::new(Consume)),
        ("retry-copy/v2", vec![Mode(0)], Arc::new(RetryCopy)),
        ("reassemble/v2", vec![Mode(1)], Arc::new(Reassemble)),
        ("chunk-copy/v2", vec![Mode(2)], Arc::new(Chunks)),
    ] {
        apps.register(
            ApplicationLabel(name.into()),
            modes,
            RestartSafety::Pure,
            implementation,
        )?;
    }
    Ok(Arc::new(apps))
}
struct Consume;
/// An explicit exercise contract: attempt 1 requests an authorized retry;
/// subsequent attempts copy the original input. It never advances its own attempt.
struct RetryCopy;
impl Application for RetryCopy {
    fn execute(&self, context: &mut WorkContext) -> AppResult<ApplicationOutcome> {
        if context.attempt().0 == 1 {
            return Ok(ApplicationOutcome::Retryable(Diagnostic {
                code: DiagnosticCode(1),
                detail: Detail("retry-copy/v2 requires one caller-authorized retry".into()),
            }));
        }
        CopyApplication.execute(context)
    }
}
impl Application for Consume {
    fn execute(&self, context: &mut WorkContext) -> AppResult<ApplicationOutcome> {
        let mut bytes = [0; 8192];
        let limit = bytes.len().min(context.buffer_limit());
        while context.read_input(&mut bytes[..limit])? != 0 {
            context.renew()?;
        }
        Ok(ApplicationOutcome::Succeeded)
    }
}
struct Reassemble;
impl Application for Reassemble {
    fn execute(&self, context: &mut WorkContext) -> AppResult<ApplicationOutcome> {
        let expected = context.input_descriptor().clone();
        // The parent input is the expected complete object, not a textual recipe.
        Consume.execute(context)?;
        context.begin_output(expected.length, expected.content_type)?;
        let mut bytes = [0; 8192];
        let limit = bytes.len().min(context.buffer_limit());
        let mut after = Number(0);
        let mut count = 0u64;
        let mut hash = Sha256::new();
        loop {
            let children = context.children(after, PageLimit(256))?;
            for work in children.members {
                context.begin_child_output(work.entity, OutputIndex(0))?;
                loop {
                    let n = context.read_child_output(&mut bytes[..limit])?;
                    if n == 0 {
                        break;
                    }
                    context.write_output(&bytes[..n])?;
                    count = count.checked_add(n as u64).ok_or(Error {
                        code: ErrorCode::LimitExceeded,
                        detail: "reassembly length overflow",
                    })?;
                    hash.update(&bytes[..n]);
                }
                context.finish_child_output()?;
                context.renew()?;
                after = Number(work.entity.0);
            }
            if !children.more {
                break;
            }
        }
        if count != expected.length.0 || Digest(hash.finalize().into()) != expected.sha256 {
            return Err(Error {
                code: ErrorCode::IntegrityError,
                detail: "child outputs do not reconstruct the parent input",
            }
            .into());
        }
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}
const CHUNK: usize = 65536;
const MAX_CHUNKS: u64 = 256;
struct Chunks;
impl Application for Chunks {
    fn execute(&self, context: &mut WorkContext) -> AppResult<ApplicationOutcome> {
        Reassemble.execute(context)
    }
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
}
impl Expansion for Chunks {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> AppResult<ExpansionOutcome> {
        match expand(context) {
            // Previously committed declarations/children remain intact. Yield
            // makes room for accepted child jobs; recovery replays the same IDs.
            Err(StoreError::Protocol(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })) => Ok(ExpansionOutcome::Yield),
            result => result,
        }
    }
}
fn expand(context: &mut ExpansionContext<'_>) -> AppResult<ExpansionOutcome> {
    let length = context.input_descriptor().length.0;
    let chunks = length.div_ceil(CHUNK as u64);
    if chunks > MAX_CHUNKS {
        return Ok(ExpansionOutcome::Failed(Diagnostic {
            code: DiagnosticCode(ErrorCode::LimitExceeded as u64),
            detail: Detail("chunk-copy/v2 supports at most 256 chunks of 65536 bytes".into()),
        }));
    }
    context.declare(
        context.operation(Id(1))?,
        &(1..=chunks).map(Id).collect::<Vec<_>>(),
        true,
    )?;
    let mut bytes = [0; CHUNK];
    let mut consumed = 0;
    for entity in 1..=chunks {
        context.renew()?;
        let expected = (length - consumed).min(CHUNK as u64) as usize;
        let mut filled = 0;
        while filled < expected {
            let end = expected.min(filled + context.buffer_limit());
            let n = context.read_input(&mut bytes[filled..end])?;
            if n == 0 {
                return Err(Error {
                    code: ErrorCode::IntegrityError,
                    detail: "parent input ended before its commitment",
                }
                .into());
            }
            filled += n;
        }
        consumed += expected as u64;
        let parameters = AdmitParameters {
            work: WorkKey {
                scope: Number(context.child_scope().0),
                producer: Producer(1),
                entity: Id(entity),
            },
            input: Input {
                length: Number(expected as u64),
                sha256: Digest(Sha256::digest(&bytes[..expected]).into()),
                content_type: context.input_descriptor().content_type.clone(),
            },
            application: ApplicationLabel("copy/v2".into()),
            mode: Mode(0),
            execution_ms: context.execution_duration(),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(expected as u64),
            },
        };
        match context.receive_input(
            context.operation(Id(entity + 1))?,
            parameters,
            Instant::now(),
        )? {
            InputReception::Replay(_) => {}
            InputReception::Receiving(mut input) => {
                for bytes in bytes[..expected].chunks(context.buffer_limit()) {
                    input.receive(bytes, Instant::now())?;
                }
                match context.prepare_input(input.finish(Instant::now())?)? {
                    InputPreparation::Replay(_) => {}
                    InputPreparation::Ready(input) => {
                        context.admit_input(*input)?;
                    }
                }
            }
        }
    }
    if context.read_input(&mut bytes[..1])? != 0 {
        return Err(Error {
            code: ErrorCode::IntegrityError,
            detail: "parent input exceeds its commitment",
        }
        .into());
    }
    Ok(ExpansionOutcome::Complete)
}
