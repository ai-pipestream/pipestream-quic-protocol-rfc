//! Durable-index coordinator.
//!
//! Drives one index-build run on public `v2_client` APIs only: generates the
//! seeded corpus, declares/admits one `index-file/v1` parent per file on
//! authority A, collects parent reference outputs, admits one
//! `index-merge/v1` unit on authority B, fetches the index and byte-compares
//! it against the single-process reference from index-core. Supports
//! subtree cancellation (--cancel-file) and kill/resume (--kill-at +
//! --resume) with frozen operation identities.
use anyhow::{Context, Result, bail};
use clap::Parser;
use index_core::{
    FILE_LABEL, MERGE_LABEL, corpus_file, digest_hex, parse_refs, reference_index,
};
use pipestream_quic::{
    persistence::PhysicalLimits,
    v2::*,
    v2_client::{
        journal::{self, Journal},
        session::{self, Client},
        transport,
    },
};
use rustls::pki_types::pem::PemObject;
use sha2::{Digest as _, Sha256};
use std::{
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Instant,
};

const OBJECT_LIMIT: u64 = 16 * 1024 * 1024;
const TRANSMIT_CHUNK: usize = 8192;

#[derive(Debug, Parser)]
#[command(about = "Durable-index coordinator: corpus, file units, merge, verify")]
struct Run {
    #[command(flatten)]
    tls: Tls,
    #[arg(long)]
    owner: String,
    #[arg(long, default_value = "index-a")]
    authority_a: String,
    #[arg(long, default_value = "index-b")]
    authority_b: String,
    #[arg(long)]
    connect_a: SocketAddr,
    #[arg(long)]
    connect_b: SocketAddr,
    #[arg(long, default_value_t = 1)]
    creation_a: u64,
    /// Creation sequences are per-authority: both sessions start at 1.
    #[arg(long, default_value_t = 1)]
    creation_b: u64,
    #[arg(long)]
    journal_a: PathBuf,
    #[arg(long)]
    journal_b: PathBuf,
    #[arg(long, default_value_t = 6)]
    seed: u64,
    #[arg(long, default_value_t = 8)]
    files: u64,
    #[arg(long, default_value_t = 2048)]
    words_per_file: u64,
    /// Cancel this file's subtree after admission (demo); none by default.
    #[arg(long)]
    cancel_file: Option<u64>,
    /// Exit mid-run for the resume demo: "stage2" (after file admits) or
    /// "stage3" (after the merge admit). One-shot via a sentinel file.
    #[arg(long)]
    kill_at: Option<String>,
    #[arg(long, default_value_t = false)]
    resume: bool,
    #[arg(long)]
    staging: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    events: PathBuf,
    /// Session and work execution budget. The Java authority caps policy at
    /// 60 s; longer reads renew the lease instead of raising the budget.
    #[arg(long, default_value_t = 60000)]
    execution_ms: u64,
}

#[derive(Debug, Parser)]
struct Tls {
    #[arg(long)]
    ca: PathBuf,
    #[arg(long)]
    cert: PathBuf,
    #[arg(long)]
    key: PathBuf,
    #[arg(long, default_value = "localhost")]
    server_name: String,
}

#[derive(Clone)]
struct Session {
    client: Client,
    events: PathBuf,
    started: Instant,
}

impl Session {
    fn log(&self, event: &str, ordinal: i64, detail: &str) {
        let line = format!(
            "{}\t{}\t{}\n",
            self.started.elapsed().as_millis(),
            event,
            detail.replace('\t', " ")
        );
        let _ = ordinal;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.events)
            .expect("events file");
        file.write_all(line.as_bytes()).expect("events write");
    }
}

fn op_id(seed: u64, side: &str, kind: &str, n: u64) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"index-op-v1");
    hash.update(seed.to_le_bytes());
    hash.update(side.as_bytes());
    hash.update(kind.as_bytes());
    hash.update(n.to_le_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let mut id: [u8; 16] = digest[..16].try_into().unwrap();
    if id == [0; 16] {
        id[15] = 1;
    }
    OperationId(id)
}

fn endpoint(tls: &Tls, connect: SocketAddr) -> Result<session::Endpoint> {
    let mut options = transport::Options::default();
    options.offer.object_limit = Number(OBJECT_LIMIT);
    let bytes = std::fs::read(&tls.ca)?;
    let certs: Vec<_> =
        rustls::pki_types::CertificateDer::pem_slice_iter(&bytes)
            .collect::<std::result::Result<_, _>>()?;
    let mut roots = rustls::RootCertStore::empty();
    for c in certs {
        roots.add(c)?;
    }
    let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(&std::fs::read(&tls.cert)?)
        .collect::<std::result::Result<_, _>>()?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(&std::fs::read(&tls.key)?)?;
    Ok(session::Endpoint {
        local: if connect.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?,
        remote: connect,
        server_name: tls.server_name.clone(),
        security: transport::Security::new(roots, Some((certs, key)))?,
        transport: options,
    })
}

async fn open_session(
    run: &Run,
    authority: &str,
    connect: SocketAddr,
    journal_path: &Path,
    creation_sequence: u64,
    execution_ms: u64,
    require_existing: bool,
) -> Result<Session> {
    let started = Instant::now();
    let creation = journal::Creation {
        authority: IdentityLabel(authority.into()),
        owner: IdentityLabel(run.owner.clone()),
        creation_sequence: Id(creation_sequence),
        policy: Policy {
            execution_limit_ms: Duration(execution_ms),
            output_retention_ms: Duration(3_600_000),
            receipt_retention_ms: Duration(86_400_000),
        },
        results: true,
    };
    creation.request(Id(1))?;
    if !run.resume && journal_path.exists() {
        bail!("journal {} exists; rerun with --resume", journal_path.display());
    }
    if run.resume && require_existing && !journal_path.exists() {
        bail!("journal {} missing; nothing to resume", journal_path.display());
    }
    // Lenient resume: a journal that was never created (side B after a
    // stage-2 kill) starts fresh; an existing one reopens.
    let journal = if journal_path.exists() {
        Journal::open(
            journal_path.to_path_buf(),
            creation,
            journal::JournalLimits::default(),
            PhysicalLimits::default(),
            journal::Options::default(),
        )
        .await?
    } else {
        Journal::initialize(
            journal_path.to_path_buf(),
            creation,
            journal::JournalLimits::default(),
            PhysicalLimits::default(),
            journal::Options::default(),
        )
        .await?
    };
    // One attempt only: a failed bind shuts the journal down, so retries
    // must reopen it. The runner waits on authority ready files instead.
    let client = Client::connect(endpoint(&run.tls, connect)?, journal.clone(), session::Options::default())
        .await
        .with_context(|| format!("connect to {connect}"))?;
    Ok(Session {
        client,
        events: run.events.clone(),
        started,
    })
}

fn file_bytes(run: &Run, file: u64) -> Vec<u8> {
    corpus_file(run.seed, file, run.words_per_file)
}

fn file_path(staging: &Path, file: u64) -> PathBuf {
    staging.join(format!("doc-{file:04}.txt"))
}

/// Transmit one admission intent with its body file; the server dedups a
/// previously committed identical operation into a receipt.
async fn transmit(
    session: &Session,
    intent: journal::Intent,
    declaration: OperationId,
    body: &[u8],
) -> Result<()> {
    let mut input = session.client.input(intent, declaration).await.context("client.input")?;
    let mut count = 0usize;
    while count < body.len() {
        let end = (count + TRANSMIT_CHUNK).min(body.len());
        input.write(&body[count..end]).await.context("input.write")?;
        count = end;
    }
    input.finish().await.context("input.finish")?;
    let _ = input.receipt().await.context("input.receipt")?;
    Ok(())
}

fn admit_intent(
    seed: u64,
    side: &str,
    entity: u64,
    label: &str,
    mode: u64,
    body: &[u8],
    output_cap: u64,
    execution_ms: u64,
) -> journal::Intent {
    let digest: [u8; 32] = Sha256::digest(body).into();
    journal::Intent {
        operation: op_id(seed, side, "admit", entity),
        mutation: Mutation::Admit(AdmitParameters {
            work: WorkKey {
                scope: Number(0),
                producer: Producer(0),
                entity: Id(entity),
            },
            input: Input {
                length: Number(body.len() as u64),
                sha256: Digest(digest),
                content_type: ApplicationLabel("text/corpus".into()),
            },
            application: ApplicationLabel(label.into()),
            mode: Mode(mode),
            execution_ms: Duration(execution_ms),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(output_cap),
            },
        }),
    }
}

/// Watch one unit to SUCCEEDED; returns its attempt. Anything else is a
/// hard failure (the example has no retryable-error taxonomy).
async fn watch_success(session: &Session, key: WorkKey, what: &str) -> Result<Id> {
    let mut after = Number(0);
    loop {
        let observed = session.client.watch(key.clone(), after, WaitMs(30_000)).await?;
        after = Number(observed.revision.0);
        let state = observed.view.state.0;
        if (5..=8).contains(&state) {
            if state != 5 {
                bail!(
                    "{what} terminal state {state}, need SUCCEEDED(5): diagnostic {:?}",
                    observed.view.diagnostic
                );
            }
            session.log("succeeded", -1, &format!("{what} attempt {}", observed.view.attempt.0));
            return Ok(Id(observed.view.attempt.0));
        }
    }
}

/// Replay journaled-but-unconfirmed intents with original identities.
/// Admit bodies are re-materialized: corpus files (deterministic regen) on
/// side A, the rebuilt merge input on side B.
async fn replay(session: &Session, run: &Run, side: &str, bodies: &dyn Fn(u64) -> Result<Vec<u8>>) -> Result<()> {
    let pending = session.client.unresolved(Number(0), PageLimit(256)).await?;
    for (_, intent) in pending {
        match &intent.mutation {
            Mutation::Admit(params) => {
                let entity = params.work.entity.0;
                let body = bodies(entity)?;
                let decl = op_id(run.seed, side, "declare", 0);
                transmit(session, intent, decl, &body).await?;
                session.log("replayed-admit", -1, &format!("{side} entity {entity}"));
            }
            _ => {
                session.client.mutate(intent).await?;
                session.log("replayed-mutation", -1, side.into());
            }
        }
    }
    Ok(())
}

fn parent_key(file: u64) -> WorkKey {
    WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(file + 1),
    }
}

/// Fetch one output, reusing a staged file when it parses (resume
/// shortcut). Reuse is end-to-end safe: TF bytes are digest-checked by the
/// merge reader and the final index is byte-compared to the reference, so a
/// corrupt staged file fails loudly instead of silently.
async fn fetch_output(
    session: &Session,
    key: WorkKey,
    attempt: Id,
    path: &Path,
    what: &str,
    validate: &dyn Fn(&[u8]) -> Result<()>,
) -> Result<Vec<u8>> {
    if path.exists() {
        let bytes = std::fs::read(path)?;
        if validate(&bytes).is_ok() {
            session.log("staged-reuse", -1, &format!("{what} {} bytes", bytes.len()));
            return Ok(bytes);
        }
        std::fs::remove_file(path)?;
    }
    session.client.select_output(key.clone(), attempt, OutputIndex(0)).await?;
    session
        .client
        .read_output(key, attempt, OutputIndex(0))
        .await
        .map_err(anyhow::Error::from)?
        .save_to(path.to_path_buf(), OBJECT_LIMIT)
        .await?;
    let bytes = std::fs::read(path)?;
    session.log("fetched", -1, &format!("{what} {} bytes", bytes.len()));
    Ok(bytes)
}

fn sentinel(staging: &Path) -> PathBuf {
    staging.join("killed.once")
}

async fn run_all(run: &Run) -> Result<()> {
    std::fs::create_dir_all(&run.staging)?;
    // Corpus first (deterministic): replay needs the same bytes back.
    for file in 0..run.files {
        let bytes = file_bytes(run, file);
        std::fs::write(file_path(&run.staging, file), &bytes)?;
    }
    let skip: Vec<u64> = run.cancel_file.into_iter().collect();
    let expected = reference_index(run.seed, run.files, run.words_per_file, &skip);
    std::fs::write(run.staging.join("reference.bin"), &expected)?;

    // Side A: file parents.
    let sa = open_session(run, &run.authority_a, run.connect_a, &run.journal_a, run.creation_a, run.execution_ms, true).await?;
    if run.resume {
        replay(&sa, run, "a", &|entity| Ok(file_bytes(run, entity - 1))).await?;
    } else {
        let ids: Vec<Id> = (1..=run.files).map(Id).collect();
        sa.client
            .mutate(journal::Intent {
                operation: op_id(run.seed, "a", "declare", 0),
                mutation: Mutation::Declare {
                    scope: Number(0),
                    entity_ids: ids,
                    seal: true,
                },
            })
            .await?;
        sa.log("declared", -1, &format!("scope 0: {} files sealed", run.files));
    }
    for file in 0..run.files {
        // Resume shortcut: confirmed terminal work is never re-transmitted
        // (the client refuses to overwrite its staged transfer).
        if run.resume {
            if let Some(o) = sa.client.observed_work(parent_key(file)).await? {
                if (5..=8).contains(&o.view.state.0) {
                    sa.log("already-terminal", -1, &format!("file {file} state {}", o.view.state.0));
                    continue;
                }
            }
        }
        let body = file_bytes(run, file);
        let intent = admit_intent(run.seed, "a", file + 1, FILE_LABEL, 2, &body, 8192, run.execution_ms);
        transmit(&sa, intent, op_id(run.seed, "a", "declare", 0), &body).await?;
        sa.log("admitted", -1, &format!("file {file}"));
    }

    // Subtree cancellation demo: cancel the armed file's child scope, then
    // record closure counts at the end. The cancel waits for the first
    // admitted member first: sent immediately, it can seal the scope before
    // authority-side expansion admits anything (empty scope, vacuous
    // close). Waiting for one member means the expansion ran, so the
    // cancel lands on admitted work; earlier files keep the authority busy,
    // so that work is still queued and settles CANCELLED.
    let mut cancelled_scope: Option<u64> = None;
    if let Some(file) = run.cancel_file {
        // The journal-local view lags the authority; one live watch yields
        // a fresh admission view carrying the allocated child scope.
        let observed = sa
            .client
            .watch(parent_key(file), Number(0), WaitMs(15_000))
            .await?;
        let scope = observed
            .view
            .child
            .context("parent view shows no child scope after admit")?
            .scope
            .0;
        for _ in 0..300 {
            sa.client
                .scope_page(Number(scope), Number(0), PageLimit(256))
                .await?;
            let members = sa
                .client
                .scope_members(Number(scope), Number(0), PageLimit(256))
                .await?;
            if !members.is_empty() {
                sa.log(
                    "cancel-target-ready",
                    -1,
                    &format!("scope {scope} has {} admitted members", members.len()),
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        sa.client
            .mutate(journal::Intent {
                operation: op_id(run.seed, "a", "cancel", scope),
                mutation: Mutation::ScopeCancel {
                    scope: Number(scope),
                },
            })
            .await?;
        sa.log("scope-cancelled", -1, &format!("file {file} child scope {scope}"));
        cancelled_scope = Some(scope);
    }

    if run.kill_at.as_deref() == Some("stage2") && !sentinel(&run.staging).exists() {
        std::fs::write(sentinel(&run.staging), "stage2")?;
        sa.log("killed", -1, "stage2 self-kill for resume demo");
        println!("KILLED stage2 (restart with --resume)");
        std::process::exit(0);
    }

    // Collect parent reference outputs (skip the cancelled file).
    let mut merge_body = String::new();
    let mut child_scopes = Vec::new();
    for file in 0..run.files {
        if Some(file) == run.cancel_file {
            continue;
        }
        let key = parent_key(file);
        let attempt = watch_success(&sa, key.clone(), &format!("file {file}")).await?;
        let scope = sa
            .client
            .observed_work(key.clone())
            .await?
            .context("parent observation missing after success")?
            .view
            .child
            .context("parent view shows no child scope after success")?
            .scope
            .0;
        child_scopes.push(scope);
        let path = run.staging.join(format!("parent-{file:04}.refs"));
        let bytes = fetch_output(&sa, key, attempt, &path, &format!("file {file} refs"), &|b| {
            let text = std::str::from_utf8(b).map_err(|e| anyhow::anyhow!(e))?;
            parse_refs(text).map(|_| ()).map_err(|e| anyhow::anyhow!(e))
        })
        .await?;
        let text = std::str::from_utf8(&bytes).context("parent refs are UTF-8")?;
        merge_body.push_str(text);
        if !merge_body.ends_with('\n') {
            merge_body.push('\n');
        }
    }
    // Parent outputs must parse as reference lines (they carry no header).
    parse_refs(&merge_body).map_err(|e| anyhow::anyhow!("parent refs unparsable: {e}"))?;
    let merge_input = format!(
        "v1 authority={} creation={}\n{merge_body}",
        run.authority_a, run.creation_a
    );
    std::fs::write(run.staging.join("merge-input.txt"), merge_input.as_bytes())?;

    // Side B: the merge unit. Coverage is per-side: after a stage-2
    // kill (or a failed first resume) the B journal may exist without the
    // declare, so declare whenever scope 0 is uncovered.
    let sb = open_session(run, &run.authority_b, run.connect_b, &run.journal_b, run.creation_b, run.execution_ms, false).await?;
    if sb.client.covered_scope(Number(0)).await?.is_none() {
        let staged = std::fs::read(run.staging.join("merge-input.txt")).unwrap_or_default();
        replay(&sb, run, "b", &|_| Ok(staged.clone())).await?;
    }
    if sb.client.covered_scope(Number(0)).await?.is_none() {
        sb.client
            .mutate(journal::Intent {
                operation: op_id(run.seed, "b", "declare", 0),
                mutation: Mutation::Declare {
                    scope: Number(0),
                    entity_ids: vec![Id(1)],
                    seal: true,
                },
            })
            .await?;
        sb.log("declared", -1, "scope 0: merge sealed");
    }
    if !run.resume || !merge_admitted(&sb).await? {
        let intent = admit_intent(run.seed, "b", 1, MERGE_LABEL, 0, merge_input.as_bytes(), 2 * 1024 * 1024, run.execution_ms);
        transmit(&sb, intent, op_id(run.seed, "b", "declare", 0), merge_input.as_bytes()).await?;
        sb.log("admitted", -1, "merge unit");
    }

    if run.kill_at.as_deref() == Some("stage3") && !sentinel(&run.staging).exists() {
        std::fs::write(sentinel(&run.staging), "stage3")?;
        sb.log("killed", -1, "stage3 self-kill for resume demo");
        println!("KILLED stage3 (restart with --resume)");
        std::process::exit(0);
    }

    let merge_key = WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(1),
    };
    let attempt = watch_success(&sb, merge_key.clone(), "merge").await?;
    let index = fetch_output(&sb, merge_key, attempt, &run.output, "index", &|b| {
        if b.is_empty() || std::str::from_utf8(b).is_err() {
            anyhow::bail!("staged index unusable");
        }
        Ok(())
    })
    .await?;

    // Closure for the cancellation demo: page the cancelled scope and
    // require every admitted member to settle CANCELLED(7). Membership is
    // read from the authority (never assumed): when the cancel seals the
    // scope before expansion admits anything, the scope closes with zero
    // members and that outcome is logged, not failed. A member already
    // SUCCEEDED(5) necessarily finished before the cancel applied, so it
    // is logged and allowed; anything else terminal is a hard failure.
    if let Some(scope) = cancelled_scope {
        let mut settled = false;
        for _ in 0..300 {
            let page = sa
                .client
                .scope_page(Number(scope), Number(0), PageLimit(256))
                .await?;
            let members = sa
                .client
                .scope_members(Number(scope), Number(0), PageLimit(256))
                .await?;
            let mut pending = 0u64;
            for member in &members {
                let state = member.terminal.map(|s| s.0);
                match state {
                    None => pending += 1,
                    Some(7) => sa.log(
                        "cancelled-child",
                        -1,
                        &format!("scope {scope} entity {} terminal state 7", member.work.entity.0),
                    ),
                    Some(5) => sa.log(
                        "cancelled-child",
                        -1,
                        &format!(
                            "scope {scope} entity {} terminal state 5 (finished before cancel)",
                            member.work.entity.0
                        ),
                    ),
                    Some(other) => {
                        bail!(
                            "cancelled child scope {scope} entity {} settled state {other}, need CANCELLED(7)",
                            member.work.entity.0
                        )
                    }
                }
            }
            if pending == 0 && page.seal.is_some() {
                sa.log(
                    "cancelled-scope-settled",
                    -1,
                    &format!("scope {scope} sealed with {} admitted members", members.len()),
                );
                settled = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        if !settled {
            bail!("cancelled child scope {scope} never settled");
        }
        // Journal one terminal work view per admitted member: the
        // checkpoint below needs member coverage, and page observations
        // alone do not establish it. Members are already terminal, so
        // each watch returns immediately.
        let members = sa
            .client
            .scope_members(Number(scope), Number(0), PageLimit(256))
            .await?;
        for member in &members {
            let observed = sa
                .client
                .watch(member.work.clone(), Number(0), WaitMs(30_000))
                .await?;
            sa.log(
                "cancelled-child",
                -1,
                &format!(
                    "scope {scope} entity {} terminal state {}",
                    member.work.entity.0, observed.view.state.0
                ),
            );
        }
        record_counts(&sa, Number(scope), &format!("cancelled child scope {scope}")).await;
        // The cancelled parent cannot assemble its references: its execute
        // phase reads the cancelled children. Watch it to terminal so the
        // scope-0 checkpoint below has full member coverage. Any terminal
        // state is accepted here and logged (the demo asserts exclusion
        // from the index, not the parent's failure code).
        if let Some(file) = run.cancel_file {
            let key = parent_key(file);
            let mut after = Number(0);
            for _ in 0..300 {
                let observed = sa.client.watch(key.clone(), after, WaitMs(30_000)).await?;
                after = Number(observed.revision.0);
                let state = observed.view.state.0;
                if (5..=8).contains(&state) {
                    sa.log(
                        "cancelled-parent",
                        -1,
                        &format!("file {file} terminal state {state}"),
                    );
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
    // Checkpoint every surviving file's child scope first: the scope-0
    // checkpoint needs descendant coverage, and that means a checkpoint
    // per child scope with member terminal views journaled. TF children
    // are already terminal, so each watch returns immediately; they must
    // all be SUCCEEDED(5).
    for scope in &child_scopes {
        sa.client
            .scope_page(Number(*scope), Number(0), PageLimit(256))
            .await?;
        let members = sa
            .client
            .scope_members(Number(*scope), Number(0), PageLimit(256))
            .await?;
        for member in &members {
            let observed = sa
                .client
                .watch(member.work.clone(), Number(0), WaitMs(30_000))
                .await?;
            let state = observed.view.state.0;
            if state != 5 {
                bail!(
                    "child scope {scope} entity {} terminal state {state}, need SUCCEEDED(5)",
                    member.work.entity.0
                );
            }
        }
        record_counts(&sa, Number(*scope), &format!("child scope {scope}")).await;
    }
    record_counts(&sa, Number(0), "scope 0").await;

    if index == expected {
        sa.log("verified", -1, &format!("index byte-exact, digest {}", digest_hex(&index)));
        println!("VERIFIED index byte-exact, digest {}", digest_hex(&index));
        // Detach both sessions on clean exit: every lingering attachment
        // holds a server connection slot (default ceiling is 16) until it
        // times out, and rapid successive runs trip the ceiling. The kill
        // demos exit without detach on purpose (crash simulation).
        sa.client.detach().await?;
        sb.client.detach().await?;
        Ok(())
    } else {
        std::fs::write(run.staging.join("index-mismatch.bin"), &index)?;
        bail!("index differs from single-process reference");
    }
}

/// True when the merge unit already has a confirmed admission (resume path
/// must not re-admit: same op id would dedup anyway, but the check keeps
/// the event stream honest).
async fn merge_admitted(sb: &Session) -> Result<bool> {
    match sb.client.observed_work(WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(1),
    }).await?
    {
        Some(o) => Ok(o.view.admitted_at.is_some()),
        None => Ok(false),
    }
}

async fn record_counts(session: &Session, scope: Number, what: &str) {
    // The seal arrives via scope paging once the scope is sealed; the
    // checkpoint then returns the closure counts.
    let mut seal = None;
    for _ in 0..150 {
        match session.client.scope_page(scope, Number(0), PageLimit(256)).await {
            Ok(page) => {
                if page.seal.is_some() {
                    seal = page.seal;
                    break;
                }
            }
            Err(e) => {
                session.log("counts-unavailable", -1, &format!("{what}: page failed: {e:?}"));
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let Some(seal) = seal else {
        session.log("counts-unavailable", -1, &format!("{what}: seal never published"));
        return;
    };
    match session.client.checkpoint(scope, seal, WaitMs(30_000)).await {
        Ok(s) => session.log(
            "scope-counts",
            -1,
            &format!(
                "{what}: declared={} success={} failure={} cancelled={} skipped={}",
                s.declared.0, s.counts.success.0, s.counts.failure.0, s.counts.cancelled.0, s.counts.skipped.0
            ),
        ),
        Err(e) => session.log("counts-unavailable", -1, &format!("{what}: checkpoint failed: {e:?}")),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let run = Run::parse();
    if let Some(k) = &run.kill_at {
        if k != "stage2" && k != "stage3" {
            bail!("--kill-at takes stage2 or stage3");
        }
    }
    if let Some(f) = run.cancel_file {
        if f >= run.files {
            bail!("--cancel-file names a file below --files");
        }
    }
    if run.files == 0 || run.files > 64 {
        bail!("--files in 1..=64");
    }
    run_all(&run).await
}
