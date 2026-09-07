use super::*;
use session::{Client, files::FileInput};

pub fn hex<const N: usize>(text: &str) -> Result<[u8; N]> {
    if text.len() != N * 2 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("expected {} hexadecimal digits", N * 2);
    }
    let mut bytes = [0; N];
    for (byte, pair) in bytes.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(bytes)
}
fn operation_id(text: &str) -> std::result::Result<OperationId, String> {
    let id = OperationId(hex(text).map_err(|e| e.to_string())?);
    if id.0 == [0; 16] {
        return Err("operation ID cannot be zero".into());
    }
    Ok(id)
}
fn digest(text: &str) -> std::result::Result<Digest, String> {
    Ok(Digest(hex(text).map_err(|e| e.to_string())?))
}
fn work(text: &str) -> std::result::Result<WorkKey, String> {
    let parts: Vec<_> = text.split(':').collect();
    if parts.len() != 3 {
        return Err("work must be scope:producer:entity".into());
    }
    let parse = |value: &str| -> std::result::Result<u64, String> {
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err("work IDs must be decimal integers".into());
        }
        value.parse::<u64>().map_err(|e| e.to_string())
    };
    let key = WorkKey {
        scope: Number(parse(parts[0])?),
        producer: Producer(parse(parts[1])?),
        entity: Id(parse(parts[2])?),
    };
    if key.scope.0 > MAX_NUMBER
        || key.entity.0 == 0
        || key.entity.0 > MAX_NUMBER
        || key.producer.0 > 1
    {
        return Err("work key is outside the protocol range".into());
    }
    Ok(key)
}

#[derive(Debug, Subcommand)]
pub enum Operation {
    /// Display the validated, durably saved session binding.
    Binding,
    Declare {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
        #[arg(long, default_value_t = 0)]
        scope: u64,
        #[arg(long, value_delimiter=',', num_args=1..=256)]
        entities: Vec<u64>,
        #[arg(long)]
        seal: bool,
    },
    Admit {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
        #[arg(long, value_parser=operation_id)]
        declaration: OperationId,
        #[arg(long, value_parser=work)]
        work: WorkKey,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        application: String,
        #[arg(long, default_value_t = 0)]
        mode: u64,
        #[arg(long, default_value_t = 60000)]
        execution_ms: u64,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        #[arg(long, default_value_t = 1)]
        output_count: u64,
        /// Exact budget requested at admission; default is the input length.
        #[arg(long)]
        output_bytes: Option<u64>,
    },
    /// Resend the exact journaled operation, never a new identity.
    Replay {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
        /// Required only for an original admission operation.
        #[arg(long)]
        input: Option<PathBuf>,
        #[arg(long, value_parser=operation_id)]
        declaration: Option<OperationId>,
    },
    Lookup {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
    },
    Watch {
        #[arg(long, value_parser=work)]
        work: WorkKey,
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long, default_value_t = 0)]
        wait_ms: u64,
    },
    Page {
        #[arg(long, default_value_t = 0)]
        scope: u64,
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long, default_value_t = 256)]
        limit: u64,
    },
    Checkpoint {
        #[arg(long, default_value_t = 0)]
        scope: u64,
        #[arg(long, value_parser=digest)]
        seal: Digest,
        #[arg(long, default_value_t = 0)]
        wait_ms: u64,
    },
    Manifest {
        #[arg(long, value_parser=work)]
        work: WorkKey,
        #[arg(long)]
        attempt: u64,
    },
    Select {
        #[arg(long, value_parser=work)]
        work: WorkKey,
        #[arg(long)]
        attempt: u64,
        #[arg(long)]
        index: u64,
    },
    Read {
        #[arg(long, value_parser=work)]
        work: WorkKey,
        #[arg(long)]
        attempt: u64,
        #[arg(long)]
        index: u64,
        #[arg(long)]
        output: PathBuf,
    },
    Retry {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
        #[arg(long, value_parser=work)]
        work: WorkKey,
        #[arg(long)]
        expected_attempt: u64,
    },
    Cancel {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
        #[arg(long, value_parser=work)]
        work: WorkKey,
    },
    Skip {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
        #[arg(long, value_parser=work)]
        work: WorkKey,
    },
    CancelScope {
        #[arg(long, value_parser=operation_id)]
        operation: OperationId,
        #[arg(long, default_value_t = 0)]
        scope: u64,
    },
    Unresolved {
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long, default_value_t = 256)]
        limit: u64,
    },
    /// Uses exact previously saved root coverage; does not invent missing evidence.
    Complete,
    Detach,
}

fn show(name: &str, value: impl std::fmt::Debug) {
    println!("{name} {value:?}");
}
pub async fn run(client: &Client, command: Operation, maximum: u64) -> Result<()> {
    use journal::Intent;
    match command {
        Operation::Binding => show("BINDING", client.binding()),
        Operation::Declare {
            operation,
            scope,
            entities,
            seal,
        } => show(
            "RECEIPT",
            client
                .mutate(Intent {
                    operation,
                    mutation: Mutation::Declare {
                        scope: Number(scope),
                        entity_ids: entities.into_iter().map(Id).collect(),
                        seal,
                    },
                })
                .await?,
        ),
        Operation::Admit {
            operation,
            declaration,
            work,
            input,
            application,
            mode,
            execution_ms,
            content_type,
            output_count,
            output_bytes,
        } => {
            let file = FileInput::open(input, maximum).await?;
            let intent = Intent {
                operation,
                mutation: Mutation::Admit(AdmitParameters {
                    work,
                    input: Input {
                        length: file.length(),
                        sha256: file.sha256(),
                        content_type: ApplicationLabel(content_type),
                    },
                    application: ApplicationLabel(application),
                    mode: Mode(mode),
                    execution_ms: Duration(execution_ms),
                    outputs: OutputBudget {
                        count: BatchCount(output_count),
                        total_bytes: Number(output_bytes.unwrap_or(if output_count == 0 {
                            0
                        } else {
                            file.length().0
                        })),
                    },
                }),
            };
            show(
                "RECEIPT",
                file.send(client.clone(), intent, declaration).await?,
            );
        }
        Operation::Replay {
            operation,
            input,
            declaration,
        } => {
            let intent = client.intent(operation).await?;
            let receipt = if matches!(intent.mutation, Mutation::Admit(_)) {
                let input = input.ok_or_else(|| {
                    anyhow::anyhow!("admission replay requires --input and --declaration")
                })?;
                let declaration = declaration
                    .ok_or_else(|| anyhow::anyhow!("admission replay requires --declaration"))?;
                FileInput::open(input, maximum)
                    .await?
                    .send(client.clone(), intent, declaration)
                    .await?
            } else {
                if input.is_some() || declaration.is_some() {
                    bail!("only admission replay accepts input/declaration");
                }
                client.mutate(intent).await?
            };
            show("RECEIPT", receipt);
        }
        Operation::Lookup { operation } => {
            show("RECEIPT", client.recover_operation(operation).await?)
        }
        Operation::Watch {
            work,
            after,
            wait_ms,
        } => {
            let observed = client.watch(work, Number(after), WaitMs(wait_ms)).await?;
            println!(
                "WORK revision={} state={} attempt={} child={}",
                observed.revision.0,
                observed.view.state.0,
                observed.view.attempt.0,
                observed
                    .view
                    .child
                    .as_ref()
                    .map(|c| format!("{}:{}", c.scope.0, c.producer.0))
                    .unwrap_or_else(|| "none".into())
            );
            show("VIEW", observed.view);
        }
        Operation::Page {
            scope,
            after,
            limit,
        } => {
            let observed = client
                .scope_page(Number(scope), Number(after), PageLimit(limit))
                .await?;
            let seal = observed
                .seal
                .map(|d| d.0.iter().map(|b| format!("{b:02x}")).collect::<String>())
                .unwrap_or_else(|| "none".into());
            println!(
                "SCOPE scope={} producer={} declared={} membership_verified={} seal={}",
                observed.scope.0,
                observed.producer.0,
                observed.declared.0,
                observed.membership_verified,
                seal
            );
            show(
                "MEMBERS",
                client
                    .scope_members(Number(scope), Number(after), PageLimit(limit))
                    .await?,
            );
        }
        Operation::Checkpoint {
            scope,
            seal,
            wait_ms,
        } => show(
            "COVERAGE",
            client
                .checkpoint(Number(scope), seal, WaitMs(wait_ms))
                .await?,
        ),
        Operation::Manifest { work, attempt } => {
            show("MANIFEST", client.manifest(work, Id(attempt)).await?)
        }
        Operation::Select {
            work,
            attempt,
            index,
        } => show(
            "REFERENCE",
            client
                .select_output(work, Id(attempt), OutputIndex(index))
                .await?,
        ),
        Operation::Read {
            work,
            attempt,
            index,
            output,
        } => {
            let saved = client
                .read_output(work, Id(attempt), OutputIndex(index))
                .await?
                .save_to(output, maximum)
                .await?;
            show("VERIFIED", saved.verification.header());
        }
        Operation::Retry {
            operation,
            work,
            expected_attempt,
        } => show(
            "RECEIPT",
            client
                .mutate(Intent {
                    operation,
                    mutation: Mutation::Retry {
                        work,
                        expected_attempt: Id(expected_attempt),
                    },
                })
                .await?,
        ),
        Operation::Cancel { operation, work } => show(
            "RECEIPT",
            client
                .mutate(Intent {
                    operation,
                    mutation: Mutation::Cancel { work },
                })
                .await?,
        ),
        Operation::Skip { operation, work } => show(
            "RECEIPT",
            client
                .mutate(Intent {
                    operation,
                    mutation: Mutation::Skip { work },
                })
                .await?,
        ),
        Operation::CancelScope { operation, scope } => show(
            "RECEIPT",
            client
                .mutate(Intent {
                    operation,
                    mutation: Mutation::ScopeCancel {
                        scope: Number(scope),
                    },
                })
                .await?,
        ),
        Operation::Unresolved { after, limit } => show(
            "UNRESOLVED",
            client.unresolved(Number(after), PageLimit(limit)).await?,
        ),
        Operation::Complete => show("COMPLETED", client.complete().await?),
        Operation::Detach => {
            client.detach().await?;
            println!("DETACHED");
        }
    }
    Ok(())
}
