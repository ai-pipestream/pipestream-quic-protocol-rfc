//! Protocol-neutral process orchestration and immutable-corpus verification.

mod extensions;
mod receipts;
mod schema;
mod scope_model;
mod v2_vectors;
mod work_model;

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Parser)]
#[command(name = "pipestream-conformance")]
struct Cli {
    #[command(subcommand)]
    command: Task,
}

#[derive(Debug, Subcommand)]
enum Task {
    Verify,
    Interop,
    Recursive,
    Examples,
    Extensions,
    /// Explore the independent, bounded durable-work lifecycle model.
    Modelcheck {
        #[arg(long, default_value_t = 32)]
        depth: usize,
        #[arg(long, default_value_t = 1_000_000)]
        max_states: usize,
    },
}

#[derive(Debug, Clone)]
struct Program {
    name: &'static str,
    command: Vec<String>,
}

struct Server {
    name: &'static str,
    child: Option<Child>,
    address: String,
    output: PathBuf,
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Task::Verify => verify(),
        Task::Interop => interop(),
        Task::Recursive => recursive(),
        Task::Examples => examples(),
        Task::Extensions => extensions::run(),
        Task::Modelcheck { depth, max_states } => {
            work_model::run(depth, max_states)?;
            scope_model::run(depth, max_states)
        }
    }
}

fn repository_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .context("resolve repository root")
}

fn verify() -> Result<()> {
    let root = repository_root()?;
    verify_no_python_sources(&root)?;
    verify_vector_index(&root)?;
    verify_recursive_index(&root)?;
    validate_cddl(&root)?;
    v2_vectors::verify(&root)?;
    println!("immutable vector corpus and normative CDDL passed");
    Ok(())
}

fn verify_recursive_index(root: &Path) -> Result<()> {
    let index = fs::read_to_string(root.join("test-vectors/recursive/index.tsv"))?;
    let mut lines = index.lines();
    ensure!(
        lines.next() == Some("name\tkind\tlayer\texpectation\terror\thex"),
        "unexpected recursive vector index header"
    );
    let mut names = BTreeSet::new();
    let mut count = 0usize;
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(fields.len() == 6, "malformed recursive vector row: {line}");
        ensure!(names.insert(fields[0]), "duplicate recursive vector name");
        ensure!(matches!(fields[2], "0" | "1" | "2"), "invalid vector layer");
        ensure!(
            matches!(fields[3], "valid" | "invalid"),
            "invalid vector expectation"
        );
        ensure!(
            (fields[3] == "valid" && fields[4] == "-")
                || (fields[3] == "invalid" && fields[4].starts_with("PIPESTREAM_")),
            "invalid vector error metadata"
        );
        ensure!(!decode_hex(fields[5])?.is_empty(), "empty recursive vector");
        count += 1;
    }
    ensure!(count > 0, "recursive vector corpus is empty");
    println!("verified {count} immutable recursive and recovery vectors");
    Ok(())
}

fn verify_no_python_sources(root: &Path) -> Result<()> {
    let mut python = Vec::new();
    visit_files(root, &mut |path| {
        if matches!(
            path.extension().and_then(OsStr::to_str),
            Some("py" | "pyi" | "pyx")
        ) {
            python.push(path.strip_prefix(root).unwrap_or(path).to_path_buf());
        }
        Ok(())
    })?;
    ensure!(
        python.is_empty(),
        "Python sources are not part of the reference suite: {python:?}"
    );
    Ok(())
}

fn visit_files(root: &Path, visitor: &mut impl FnMut(&Path) -> Result<()>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            if matches!(
                name.to_str(),
                Some(".git" | ".refcache" | "build" | "target" | "vendor")
            ) {
                continue;
            }
            visit_files(&path, visitor)?;
        } else {
            visitor(&path)?;
        }
    }
    Ok(())
}

fn verify_vector_index(root: &Path) -> Result<()> {
    let vectors = root.join("test-vectors");
    let index = fs::read_to_string(vectors.join("index.tsv"))?;
    let mut indexed = BTreeSet::new();
    let mut lines = index.lines();
    ensure!(
        lines.next() == Some("name\tkind\texpectation\terror\tsha256\toctets"),
        "unexpected vector index header"
    );
    let mut count = 0usize;
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(fields.len() == 6, "malformed vector index row: {line}");
        let relative = PathBuf::from(fields[2]).join(format!("{}.bin", fields[0]));
        let data = fs::read(vectors.join(&relative))
            .with_context(|| format!("read vector {}", relative.display()))?;
        let digest = hex(&Sha256::digest(&data));
        ensure!(digest == fields[4], "{}: SHA-256 differs", fields[0]);
        ensure!(
            data.len() == fields[5].parse::<usize>()?,
            "{}: octet count differs",
            fields[0]
        );
        ensure!(
            indexed.insert(relative),
            "duplicate vector row {}",
            fields[0]
        );
        count += 1;
    }
    let mut present = BTreeSet::new();
    for expectation in ["valid", "invalid"] {
        for entry in fs::read_dir(vectors.join(expectation))? {
            let entry = entry?;
            if entry.path().extension() == Some(OsStr::new("bin")) {
                present.insert(PathBuf::from(expectation).join(entry.file_name()));
            }
        }
    }
    ensure!(indexed == present, "vector index and binary corpus differ");
    println!("verified {count} immutable language-neutral vectors");
    Ok(())
}

fn validate_cddl(root: &Path) -> Result<()> {
    let bundle = executable(&["bundle", "bundle3.3"])?;
    let schema = root.join("cddl/pipestream-layer0.cddl");
    schema::synchronized(
        &fs::read_to_string(&schema)?,
        &fs::read_to_string(root.join("sections-src/appendix-c.md"))?,
    )?;
    run_checked_owned(
        root,
        &[
            bundle.clone(),
            "exec".to_owned(),
            "cddl".to_owned(),
            path(&schema),
            "generate".to_owned(),
            "1".to_owned(),
        ],
        Duration::from_secs(30),
    )?;
    let temporary = tempfile::Builder::new()
        .prefix("pipestream-cddl-")
        .tempdir()?;
    let mut accepted_paths = Vec::new();
    let mut refused_paths = Vec::new();
    let mut index = fs::read_to_string(root.join("test-vectors/cddl/index.tsv"))?;
    for fixture in [
        "extensions.tsv",
        "work-sets.tsv",
        "authenticated-recovery.tsv",
    ] {
        let additional = fs::read_to_string(root.join("test-vectors/cddl").join(fixture))?;
        ensure!(
            additional.lines().next() == Some("name\texpectation\thex"),
            "unexpected CDDL fixture header: {fixture}"
        );
        for row in additional.lines().skip(1) {
            index.push_str(row);
            index.push('\n');
        }
    }
    let mut lines = index.lines();
    ensure!(
        lines.next() == Some("name\texpectation\thex"),
        "unexpected CDDL fixture index header"
    );
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(fields.len() == 3, "malformed CDDL fixture row: {line}");
        let name = fields[0];
        let expectation = fields[1];
        ensure!(
            name.bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid CDDL fixture name: {name}"
        );
        let destination = temporary.path().join(format!("{expectation}-{name}.cbor"));
        fs::write(&destination, decode_hex(fields[2])?)?;
        match expectation {
            "valid" => accepted_paths.push(destination),
            "invalid" => refused_paths.push((name, destination)),
            _ => bail!("unknown CDDL fixture expectation: {expectation}"),
        }
    }
    ensure!(!accepted_paths.is_empty(), "no valid CDDL fixtures");
    ensure!(!refused_paths.is_empty(), "no invalid CDDL fixtures");
    let mut command = vec![
        bundle.clone(),
        "exec".to_owned(),
        "cddl".to_owned(),
        path(&schema),
        "validate".to_owned(),
    ];
    command.extend(accepted_paths.iter().map(|value| path(value)));
    run_checked_owned(root, &command, Duration::from_secs(30))?;
    for (name, destination) in refused_paths {
        let output = run_output(
            root,
            &[
                &bundle,
                "exec",
                "cddl",
                &path(&schema),
                "validate",
                &path(&destination),
            ],
            Duration::from_secs(30),
        )?;
        ensure!(
            !output.status.success(),
            "normative CDDL accepted invalid vector {name}"
        );
    }
    Ok(())
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.len().is_multiple_of(2),
        "odd-length hexadecimal fixture"
    );
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let digits = std::str::from_utf8(pair)?;
        bytes.push(
            u8::from_str_radix(digits, 16)
                .with_context(|| format!("invalid hexadecimal fixture octet: {digits}"))?,
        );
    }
    Ok(bytes)
}

fn interop() -> Result<()> {
    let root = repository_root()?;
    let implementations = implementations(&root)?;
    let temporary = tempfile::Builder::new()
        .prefix("pipestream-interop-")
        .tempdir()?;
    let certs = temporary.path().join("certs");
    certificates(&certs)?;
    let payload = temporary.path().join("payload.bin");
    let mut bytes = b"PipeStream interop\0".to_vec();
    for _ in 0..17 {
        bytes.extend(0u8..=255);
    }
    fs::write(&payload, bytes)?;
    let mut entity_id = 100u32;
    for server in &implementations {
        for client in &implementations {
            one_pair(
                &root,
                server,
                client,
                temporary.path(),
                &certs,
                &payload,
                entity_id,
            )?;
            entity_id += 1;
        }
    }
    println!(
        "all {} black-box pairs passed",
        implementations.len().pow(2)
    );
    Ok(())
}

fn implementations(root: &Path) -> Result<Vec<Program>> {
    let java = single_match(
        &root.join("implementations/java-netty/target"),
        "-all.jar",
        "Java implementation JAR",
    )?;
    let values = vec![
        Program {
            name: "java-netty",
            command: vec![
                "java".to_owned(),
                "--enable-native-access=ALL-UNNAMED".to_owned(),
                "-jar".to_owned(),
                path(&java),
            ],
        },
        Program {
            name: "rust-quinn",
            command: vec![path(
                &root.join("implementations/rust-quinn/target/release/pipestream-quinn"),
            )],
        },
        Program {
            name: "cpp-msquic",
            command: vec![path(
                &root.join("implementations/cpp-msquic/build/pipestream-msquic"),
            )],
        },
    ];
    for program in &values {
        if program.command[0].contains(std::path::MAIN_SEPARATOR)
            || program.command[0].starts_with('/')
        {
            ensure!(
                Path::new(&program.command[0]).is_file(),
                "missing {} executable; run the build gates first",
                program.name
            );
        }
    }
    Ok(values)
}

fn one_pair(
    root: &Path,
    server_program: &Program,
    client_program: &Program,
    temporary: &Path,
    certs: &Path,
    payload: &Path,
    entity_id: u32,
) -> Result<()> {
    let pair = temporary.join(format!(
        "{}-to-{}",
        client_program.name, server_program.name
    ));
    let mut server = start_server(root, server_program, &pair, certs)?;
    let mut command = client_program.command.clone();
    command.extend([
        "send".to_owned(),
        "--connect".to_owned(),
        server.address.clone(),
        "--ca".to_owned(),
        path(&certs.join("ca.crt")),
        "--server-name".to_owned(),
        "localhost".to_owned(),
        "--entity-id".to_owned(),
        entity_id.to_string(),
        "--input".to_owned(),
        path(payload),
        "--content-type".to_owned(),
        "application/octet-stream".to_owned(),
        "--parent-id".to_owned(),
        "42".to_owned(),
    ]);
    let client_result = run_output_owned(root, &command, Duration::from_secs(30))?;
    ensure_success(
        &client_result,
        &format!(
            "{} client against {}",
            client_program.name, server_program.name
        ),
    )?;
    finish_server(&mut server)?;
    ensure!(
        fs::read(server.output.join(format!("{entity_id}.bin")))? == fs::read(payload)?,
        "payload mismatch for {} -> {}",
        client_program.name,
        server_program.name
    );
    ensure!(
        fs::read_to_string(server.output.join(format!("{entity_id}.parent")))?.trim() == "42",
        "parent mismatch for {} -> {}",
        client_program.name,
        server_program.name
    );
    println!("PASS {} -> {}", client_program.name, server_program.name);
    Ok(())
}

fn examples() -> Result<()> {
    let root = repository_root()?;
    java_to_rust(&root)?;
    rust_to_cpp_recovery(&root)?;
    three_node_scatter(&root)?;
    Ok(())
}

fn recursive() -> Result<()> {
    let root = repository_root()?;
    let temporary = tempfile::Builder::new()
        .prefix("pipestream-recursive-")
        .tempdir()?;
    let certs = temporary.path().join("certs");
    certificates(&certs)?;
    let executable = root.join("implementations/rust-quinn/target/release/pipestream-quinn");
    ensure!(
        executable.is_file(),
        "missing Rust server executable; run the release build first"
    );
    let program = Program {
        name: "rust-recursive",
        command: vec![path(&executable)],
    };

    let layer1 = temporary.path().join("layer1");
    let mut server = start_recursive_server(&root, &program, &layer1, &certs)?;
    let session_id = "black-box-recursive-1";
    let result = run_checked_owned(
        &root,
        &with_connection(
            &program.command,
            "recursive-scenario",
            &server.address,
            &certs,
            &["--session-id", session_id],
        ),
        Duration::from_secs(30),
    )?;
    finish_server(&mut server)?;
    ensure!(
        String::from_utf8_lossy(&result.stdout).contains("RECURSIVE_OK black-box-recursive-1 2 3"),
        "recursive CLI did not report the verified tree"
    );
    ensure!(
        fs::read(server.output.join(session_id).join("lineage.sha256"))?
            == receipts::recursive(session_id),
        "recursive CLI lineage differs from independent expected receipt"
    );
    println!("PASS Rust Layer 1 recursive CLI and durable lineage");

    let layer2 = temporary.path().join("layer2");
    let recovery_session = "black-box-recovery-1";
    let mut first = start_recursive_server(&root, &program, &layer2, &certs)?;
    let yielded = run_checked_owned(
        &root,
        &with_connection(
            &program.command,
            "begin-yield",
            &first.address,
            &certs,
            &["--session-id", recovery_session],
        ),
        Duration::from_secs(30),
    )?;
    finish_server(&mut first)?;
    let claim = String::from_utf8(yielded.stdout)?;
    let claim = claim.split_whitespace().collect::<Vec<_>>();
    ensure!(
        claim.len() == 4 && claim[0] == "CLAIM" && claim[1] == recovery_session,
        "begin-yield returned an invalid claim contract"
    );
    ensure!(
        claim[2].parse::<u64>().is_ok() && claim[3].len() == 64,
        "begin-yield returned invalid claim values"
    );

    let mut second = start_recursive_server(&root, &program, &layer2, &certs)?;
    let redemption = with_connection(
        &program.command,
        "redeem",
        &second.address,
        &certs,
        &[
            "--session-id",
            recovery_session,
            "--claim-id",
            claim[2],
            "--state-checksum",
            claim[3],
        ],
    );
    let redeemed = run_checked_owned(&root, &redemption, Duration::from_secs(30))?;
    finish_server(&mut second)?;
    ensure!(
        String::from_utf8_lossy(&redeemed.stdout).contains(&format!("REDEEMED {}", claim[2])),
        "redeem CLI did not acknowledge the claim"
    );
    ensure!(
        fs::read(second.output.join(recovery_session).join("lineage.sha256"))?
            == receipts::recovery(recovery_session),
        "claim redemption lineage differs from independent expected receipt"
    );

    let mut replay_server = start_recursive_server(&root, &program, &layer2, &certs)?;
    let replay = with_connection(
        &program.command,
        "redeem",
        &replay_server.address,
        &certs,
        &[
            "--session-id",
            recovery_session,
            "--claim-id",
            claim[2],
            "--state-checksum",
            claim[3],
        ],
    );
    let replay_result = run_output_owned(&root, &replay, Duration::from_secs(30))?;
    ensure!(
        !replay_result.status.success()
            && String::from_utf8_lossy(&replay_result.stderr)
                .contains("PIPESTREAM_CLAIM_NOT_FOUND"),
        "claim replay did not produce the named client refusal"
    );
    finish_server_with_refusal(&mut replay_server, "PIPESTREAM_CLAIM_NOT_FOUND")?;
    println!("PASS Rust Layer 2 cross-server redemption and replay refusal");
    Ok(())
}

fn with_connection(
    base: &[String],
    command: &str,
    address: &str,
    certs: &Path,
    arguments: &[&str],
) -> Vec<String> {
    let mut result = base.to_vec();
    result.extend([
        command.to_owned(),
        "--connect".to_owned(),
        address.to_owned(),
        "--ca".to_owned(),
        path(&certs.join("ca.crt")),
        "--server-name".to_owned(),
        "localhost".to_owned(),
    ]);
    result.extend(arguments.iter().map(|value| (*value).to_owned()));
    result
}

fn java_to_rust(root: &Path) -> Result<()> {
    let temporary = tempfile::Builder::new()
        .prefix("pipestream-java-example-")
        .tempdir()?;
    let certs = temporary.path().join("certs");
    certificates(&certs)?;
    let payload = temporary.path().join("input.bin");
    let mut bytes = b"Java API example\0".to_vec();
    for _ in 0..3 {
        bytes.extend(0u8..=255);
    }
    fs::write(&payload, bytes)?;
    let rust = implementations(root)?
        .into_iter()
        .find(|value| value.name == "rust-quinn")
        .unwrap();
    let mut server = start_server(root, &rust, temporary.path(), &certs)?;
    let jar = single_match(
        &root.join("examples/java-to-rust/target"),
        "-all.jar",
        "Java-to-Rust example JAR",
    )?;
    let result = run_checked_owned(
        root,
        &[
            "java".to_owned(),
            "--enable-native-access=ALL-UNNAMED".to_owned(),
            "-jar".to_owned(),
            path(&jar),
            "--connect".to_owned(),
            server.address.clone(),
            "--ca".to_owned(),
            path(&certs.join("ca.crt")),
            "--server-name".to_owned(),
            "localhost".to_owned(),
            "--entity-id".to_owned(),
            "101".to_owned(),
            "--input".to_owned(),
            path(&payload),
        ],
        Duration::from_secs(30),
    )?;
    finish_server(&mut server)?;
    ensure!(
        result
            .stdout
            .windows(32)
            .any(|value| value == b"JAVA EXAMPLE COMPLETE entity=101"),
        "Java example did not report completion"
    );
    ensure!(fs::read(server.output.join("101.bin"))? == fs::read(payload)?);
    println!("PASS Java source -> Rust/Quinn server");
    Ok(())
}

fn rust_to_cpp_recovery(root: &Path) -> Result<()> {
    let temporary = tempfile::Builder::new()
        .prefix("pipestream-rust-recovery-")
        .tempdir()?;
    let certs = temporary.path().join("certs");
    certificates(&certs)?;
    let payload = temporary.path().join("input.bin");
    let mut bytes = b"Rust durable recovery example\0".to_vec();
    bytes.extend((0u8..=255).rev());
    fs::write(&payload, bytes)?;
    let journal = temporary.path().join("sender.journal");
    let executable = root.join("examples/rust-to-cpp-recovery/target/release/rust-to-cpp-recovery");
    let base = vec![path(&executable)];
    run_checked_owned(
        root,
        &with(
            &base,
            &[
                "prepare",
                "--journal",
                &path(&journal),
                "--input",
                &path(&payload),
                "--entity-id",
                "201",
            ],
        ),
        Duration::from_secs(30),
    )?;
    let cpp = implementations(root)?
        .into_iter()
        .find(|value| value.name == "cpp-msquic")
        .unwrap();
    let mut server = start_server(root, &cpp, temporary.path(), &certs)?;
    let recover = with(
        &base,
        &[
            "recover",
            "--journal",
            &path(&journal),
            "--input",
            &path(&payload),
            "--connect",
            &server.address,
            "--ca",
            &path(&certs.join("ca.crt")),
        ],
    );
    let result = run_checked_owned(root, &recover, Duration::from_secs(30))?;
    finish_server(&mut server)?;
    ensure!(
        result
            .stdout
            .windows(33)
            .any(|value| value == b"RUST RECOVERY COMPLETE entity=201"),
        "Rust recovery example did not report completion"
    );
    ensure!(fs::read(server.output.join("201.bin"))? == fs::read(payload)?);
    let replay = run_output_owned(root, &recover, Duration::from_secs(10))?;
    ensure!(
        !replay.status.success()
            && String::from_utf8_lossy(&replay.stderr).contains("journal is already complete"),
        "completed recovery journal allowed replay"
    );
    println!("PASS Rust source recovery -> C++/MsQuic server");
    Ok(())
}

fn three_node_scatter(root: &Path) -> Result<()> {
    let temporary = tempfile::Builder::new()
        .prefix("pipestream-rust-scatter-")
        .tempdir()?;
    let certs = temporary.path().join("certs");
    certificates(&certs)?;
    let payload = temporary.path().join("input.bin");
    let mut bytes = b"Rust scatter coordinator\0".to_vec();
    for _ in 0..17 {
        bytes.extend(0u8..=255);
    }
    fs::write(&payload, bytes)?;
    let programs = implementations(root)?;
    let mut servers = BTreeMap::new();
    for name in ["java-netty", "rust-quinn", "cpp-msquic"] {
        let program = programs.iter().find(|value| value.name == name).unwrap();
        servers.insert(name, start_server(root, program, temporary.path(), &certs)?);
    }
    let executable = root.join("examples/three-node-scatter/target/release/three-node-scatter");
    let java = &servers["java-netty"];
    let rust = &servers["rust-quinn"];
    let cpp = &servers["cpp-msquic"];
    let command = vec![
        path(&executable),
        "--input".to_owned(),
        path(&payload),
        "--ca".to_owned(),
        path(&certs.join("ca.crt")),
        "--java-server".to_owned(),
        java.address.clone(),
        "--java-output".to_owned(),
        path(&java.output),
        "--rust-server".to_owned(),
        rust.address.clone(),
        "--rust-output".to_owned(),
        path(&rust.output),
        "--cpp-server".to_owned(),
        cpp.address.clone(),
        "--cpp-output".to_owned(),
        path(&cpp.output),
    ];
    let result = run_checked_owned(root, &command, Duration::from_secs(30))?;
    for server in servers.values_mut() {
        finish_server(server)?;
    }
    ensure!(
        String::from_utf8_lossy(&result.stdout)
            .contains("RUST SCATTER COMPLETE parent=77 entities=301,302,303"),
        "Rust scatter example did not report reassembly"
    );
    println!("PASS Rust source scatter -> Java, Rust, and C++ servers");
    Ok(())
}

fn start_server(root: &Path, program: &Program, run_root: &Path, certs: &Path) -> Result<Server> {
    let server_root = run_root.join(format!("{}-server", program.name));
    let output = server_root.join("received");
    fs::create_dir_all(&output)?;
    let ready = server_root.join("ready");
    let mut command = program.command.clone();
    command.extend([
        "serve".to_owned(),
        "--bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cert".to_owned(),
        path(&certs.join("server.crt")),
        "--key".to_owned(),
        path(&certs.join("server.key")),
        "--output-dir".to_owned(),
        path(&output),
        "--ready-file".to_owned(),
        path(&ready),
        "--once".to_owned(),
    ]);
    start_ready_server(root, program.name, &command, &ready, output)
}

fn start_recursive_server(
    root: &Path,
    program: &Program,
    run_root: &Path,
    certs: &Path,
) -> Result<Server> {
    let output = run_root.join("entities");
    fs::create_dir_all(&output)?;
    let ready = run_root.join(format!("ready-{}", unique_suffix()));
    let command = with(
        &program.command,
        &[
            "serve-recursive",
            "--bind",
            "127.0.0.1:0",
            "--cert",
            &path(&certs.join("server.crt")),
            "--key",
            &path(&certs.join("server.key")),
            "--state-db",
            &path(&run_root.join("sessions.sqlite3")),
            "--entity-dir",
            &path(&output),
            "--ready-file",
            &path(&ready),
            "--once",
        ],
    );
    start_ready_server(root, program.name, &command, &ready, output)
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock precedes Unix epoch")
        .as_nanos()
}

fn start_ready_server(
    root: &Path,
    name: &'static str,
    command: &[String],
    ready: &Path,
    output: PathBuf,
) -> Result<Server> {
    let mut child = spawn_owned(root, command)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if ready.is_file() && ready.metadata()?.len() > 0 {
            return Ok(Server {
                name,
                child: Some(child),
                address: fs::read_to_string(ready)?.trim().to_owned(),
                output,
            });
        }
        if let Some(status) = child.try_wait()? {
            let output = child.wait_with_output()?;
            bail!(
                "{name} exited before readiness ({status})\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        ensure!(Instant::now() < deadline, "{name} readiness timed out");
        thread::sleep(Duration::from_millis(25));
    }
}

fn finish_server(server: &mut Server) -> Result<()> {
    let child = server
        .child
        .take()
        .context("server process already consumed")?;
    let output = wait_output(child, Duration::from_secs(30))?;
    ensure_success(&output, server.name)
}

fn finish_server_with_refusal(server: &mut Server, refusal: &str) -> Result<()> {
    let child = server
        .child
        .take()
        .context("server process already consumed")?;
    let output = wait_output(child, Duration::from_secs(30))?;
    ensure!(
        !output.status.success() && String::from_utf8_lossy(&output.stderr).contains(refusal),
        "{} did not report {refusal}\nstdout:\n{}\nstderr:\n{}",
        server.name,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn certificates(output: &Path) -> Result<()> {
    fs::create_dir_all(output)?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "PipeStream-Conformance-CA");
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate()?)?;

    let mut server_params = CertificateParams::new(vec!["localhost".to_owned()])?;
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_key = KeyPair::generate()?;
    let server = server_params.signed_by(&server_key, &ca)?;
    fs::write(output.join("ca.crt"), ca.pem())?;
    fs::write(output.join("server.crt"), server.pem())?;
    fs::write(output.join("server.key"), server_key.serialize_pem())?;
    Ok(())
}

fn single_match(directory: &Path, suffix: &str, description: &str) -> Result<PathBuf> {
    let matches = fs::read_dir(directory)?
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|value| {
            value
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.ends_with(suffix))
        })
        .collect::<Vec<_>>();
    ensure!(matches.len() == 1, "expected one {description}");
    Ok(matches[0].clone())
}

fn executable(candidates: &[&str]) -> Result<String> {
    for candidate in candidates {
        if Command::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            return Ok((*candidate).to_owned());
        }
    }
    bail!("required executable is absent: {candidates:?}")
}

fn spawn_owned(root: &Path, command: &[String]) -> Result<Child> {
    ensure!(!command.is_empty(), "empty process command");
    Command::new(&command[0])
        .args(&command[1..])
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("start {}", command.join(" ")))
}

fn run_checked_owned(root: &Path, command: &[String], timeout: Duration) -> Result<Output> {
    let output = run_output_owned(root, command, timeout)?;
    ensure_success(&output, &command.join(" "))?;
    Ok(output)
}

fn run_output(root: &Path, command: &[&str], timeout: Duration) -> Result<Output> {
    let owned = command
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    run_output_owned(root, &owned, timeout)
}

fn run_output_owned(root: &Path, command: &[String], timeout: Duration) -> Result<Output> {
    wait_output(spawn_owned(root, command)?, timeout)
}

fn wait_output(mut child: Child, timeout: Duration) -> Result<Output> {
    let deadline = Instant::now() + timeout;
    let stdout = capture_pipe(child.stdout.take());
    let stderr = capture_pipe(child.stderr.take());
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait()? {
            break (status, false);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            break (child.wait()?, true);
        }
        thread::sleep(Duration::from_millis(25));
    };
    // A descendant retaining inherited pipe handles must not turn a bounded
    // process wait into an unbounded join. The CLI reports the missing EOF.
    let output_deadline = deadline.max(Instant::now() + Duration::from_millis(250));
    let receive = |reader: mpsc::Receiver<io::Result<Vec<u8>>>| -> Result<Vec<u8>> {
        Ok(reader
            .recv_timeout(output_deadline.saturating_duration_since(Instant::now()))
            .context("child output did not close within its process deadline")??)
    };
    let output = Output {
        status,
        stdout: receive(stdout)?,
        stderr: receive(stderr)?,
    };
    ensure!(
        !timed_out,
        "process timed out\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

fn capture_pipe<R: Read + Send + 'static>(
    reader: Option<R>,
) -> mpsc::Receiver<io::Result<Vec<u8>>> {
    let (sender, receiver) = mpsc::sync_channel(1);
    if let Some(mut reader) = reader {
        thread::spawn(move || {
            let capture = (|| {
                const LIMIT: usize = 1024 * 1024;
                const MARKER: &[u8] = b"\n[output truncated after 1048576 bytes]\n";
                let mut bytes = Vec::new();
                let mut buffer = [0u8; 8192];
                let mut truncated = false;
                loop {
                    let length = match reader.read(&mut buffer) {
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        result => result?,
                    };
                    if length == 0 {
                        break;
                    }
                    let retained = length.min(LIMIT - bytes.len());
                    bytes.extend_from_slice(&buffer[..retained]);
                    truncated |= retained < length;
                }
                if truncated {
                    bytes.truncate(LIMIT - MARKER.len());
                    bytes.extend_from_slice(MARKER);
                }
                Ok(bytes)
            })();
            let _ = sender.send(capture);
        });
    } else {
        let _ = sender.send(Ok(Vec::new()));
    }
    receiver
}

fn ensure_success(output: &Output, description: &str) -> Result<()> {
    ensure!(
        output.status.success(),
        "{description} failed ({})\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn with(base: &[String], arguments: &[&str]) -> Vec<String> {
    let mut result = base.to_vec();
    result.extend(arguments.iter().map(|value| (*value).to_owned()));
    result
}

fn path(value: &Path) -> String {
    value.to_string_lossy().into_owned()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[(byte >> 4) as usize]));
        output.push(char::from(DIGITS[(byte & 0x0f) as usize]));
    }
    output
}

#[cfg(test)]
mod capture_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn output_capture_child() {
        if std::env::var_os("PIPESTREAM_CAPTURE_CHILD").is_some() {
            let bytes = vec![b'x'; 2 * 1024 * 1024];
            std::io::stdout().write_all(&bytes).unwrap();
            std::io::stderr().write_all(&bytes).unwrap();
            std::process::exit(0);
        }
    }

    #[test]
    fn noisy_child_is_drained_before_waiting_for_exit() {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "capture_tests::output_capture_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_CAPTURE_CHILD", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = wait_output(child, Duration::from_secs(3)).unwrap();
        assert!(output.status.success());
        assert!(output.stdout.len() <= 1024 * 1024);
        assert!(output.stderr.len() <= 1024 * 1024);
        assert!(String::from_utf8_lossy(&output.stdout).contains("output truncated"));
        assert!(String::from_utf8_lossy(&output.stderr).contains("output truncated"));
    }
}
