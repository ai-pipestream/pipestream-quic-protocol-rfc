//! Real CLI subprocesses, authenticated QUIC and independent on-disk client/server
//! history. No test calls the authority dispatcher in place of a wire request.
#![cfg(unix)]
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

static COMMANDS: AtomicUsize = AtomicUsize::new(0);
#[path = "v2_cli/managed.rs"]
mod managed;
fn text(path: &Path) -> String {
    path.to_str().unwrap().to_owned()
}
fn run(directory: &Path, args: &[String], success: bool) -> String {
    let serial = COMMANDS.fetch_add(1, Ordering::Relaxed);
    let out = directory.join(format!("command-{serial}.out"));
    let err = directory.join(format!("command-{serial}.err"));
    let mut child = Command::new(env!("CARGO_BIN_EXE_pipestream-quinn"))
        .args(args)
        .stdout(File::create(&out).unwrap())
        .stderr(File::create(&err).unwrap())
        .spawn()
        .unwrap();
    let end = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= end {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!(
                "CLI timeout: {args:?}\n{}",
                fs::read_to_string(err).unwrap()
            );
        }
        thread::sleep(Duration::from_millis(10));
    };
    let output = fs::read_to_string(out).unwrap() + &fs::read_to_string(err).unwrap();
    assert_eq!(status.success(), success, "{args:?}\n{output}");
    output
}
struct Fixture {
    directory: tempfile::TempDir,
    serial: usize,
    allow_skip: bool,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let issuer = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        fs::write(directory.path().join("ca.pem"), issuer.pem()).unwrap();
        let mut map = "sha256\tprincipal\n".to_owned();
        for (name, owner) in [
            ("server", ""),
            ("alice", "alice"),
            ("rotated", "alice"),
            ("bob", "bob"),
        ] {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![if owner.is_empty() {
                ExtendedKeyUsagePurpose::ServerAuth
            } else {
                ExtendedKeyUsagePurpose::ClientAuth
            }];
            let cert = params.signed_by(&key, &issuer).unwrap();
            fs::write(directory.path().join(format!("{name}.pem")), cert.pem()).unwrap();
            fs::write(
                directory.path().join(format!("{name}.key")),
                key.serialize_pem(),
            )
            .unwrap();
            if !owner.is_empty() {
                let hash: String = Sha256::digest(cert.der())
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                map.push_str(&format!("{hash}\t{owner}\n"));
            }
        }
        fs::write(directory.path().join("principals.tsv"), map).unwrap();
        Self {
            directory,
            serial: 0,
            allow_skip: false,
        }
    }
    fn path(&self, name: &str) -> String {
        text(&self.directory.path().join(name))
    }
    fn storage(&self) -> Vec<String> {
        let mut args = vec![
            "--state-db".into(),
            self.path("authority.sqlite"),
            "--object-dir".into(),
            self.path("objects"),
            "--authority".into(),
            "issuer-a".into(),
            "--principal-map".into(),
            self.path("principals.tsv"),
            "--trust-system-clock".into(),
        ];
        if self.allow_skip {
            args.push("--allow-skip".into());
        }
        args
    }
    fn initialize(&self) {
        let mut args = vec!["v2".into(), "init-authority".into()];
        args.extend(self.storage());
        assert!(run(self.directory.path(), &args, true).contains("AUTHORITY_INITIALIZED"));
    }
    fn start(&mut self) -> Server {
        self.serial += 1;
        let ready = self.directory.path().join(format!("ready-{}", self.serial));
        let log = self
            .directory
            .path()
            .join(format!("server-{}.log", self.serial));
        let mut args = vec!["v2".into(), "serve".into()];
        args.extend(self.storage());
        args.extend([
            "--cert".into(),
            self.path("server.pem"),
            "--key".into(),
            self.path("server.key"),
            "--client-ca".into(),
            self.path("ca.pem"),
            "--result-authority".into(),
            "localhost:7443".into(),
            "--ready-file".into(),
            text(&ready),
        ]);
        let log_file = File::create(&log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_pipestream-quinn"))
            .args(args)
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .unwrap();
        let mut server = Server {
            child,
            address: String::new(),
            log,
        };
        let end = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(address) = fs::read_to_string(&ready)
                && address.trim().parse::<std::net::SocketAddr>().is_ok()
            {
                server.address = address.trim().into();
                break;
            }
            assert!(
                server.child.try_wait().unwrap().is_none(),
                "{}",
                fs::read_to_string(&server.log).unwrap()
            );
            assert!(Instant::now() < end, "server readiness timeout");
            thread::sleep(Duration::from_millis(10));
        }
        server
    }
    fn connection(&self, server: &Server, cert: &str) -> Vec<String> {
        vec![
            "--connect".into(),
            server.address.clone(),
            "--ca".into(),
            self.path("ca.pem"),
            "--cert".into(),
            self.path(&format!("{cert}.pem")),
            "--key".into(),
            self.path(&format!("{cert}.key")),
        ]
    }
    fn journal(&self, sequence: u64, results: bool) -> Vec<String> {
        let mut args = vec![
            "--journal".into(),
            self.path(&format!("client-{sequence}.sqlite")),
            "--authority".into(),
            "issuer-a".into(),
            "--owner".into(),
            "alice".into(),
            "--creation-sequence".into(),
            sequence.to_string(),
        ];
        if !results {
            args.push("--no-results".into());
        }
        args
    }
    fn init_client(&self, sequence: u64, results: bool) {
        let mut args = vec!["v2".into(), "init-client".into()];
        args.extend(self.journal(sequence, results));
        run(self.directory.path(), &args, true);
    }
    fn client(
        &self,
        server: &Server,
        sequence: u64,
        results: bool,
        cert: &str,
        operation: &[&str],
        success: bool,
    ) -> String {
        let mut args = vec!["v2".into(), "client".into()];
        args.extend(self.journal(sequence, results));
        args.extend(self.connection(server, cert));
        args.extend(operation.iter().map(|v| v.to_string()));
        run(self.directory.path(), &args, success)
    }
    fn succeeded(&self, server: &Server, sequence: u64, results: bool, work: &str) -> String {
        let end = Instant::now() + Duration::from_secs(20);
        loop {
            let view = self.client(
                server,
                sequence,
                results,
                "alice",
                &["watch", "--work", work],
                true,
            );
            if view.contains("state=5 ") {
                return view;
            }
            assert!(!view.contains("state=6 ") && Instant::now() < end, "{view}");
            thread::sleep(Duration::from_millis(20));
        }
    }
}
struct Server {
    child: Child,
    address: String,
    log: PathBuf,
}
impl Server {
    fn crash(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
    fn stop(&mut self) {
        assert!(self.child.try_wait().unwrap().is_none());
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let end = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "{}",
                    fs::read_to_string(&self.log).unwrap()
                );
                break;
            }
            assert!(Instant::now() < end, "server shutdown timeout");
            thread::sleep(Duration::from_millis(10));
        }
        assert!(fs::read_to_string(&self.log).unwrap().contains("DRAINED"));
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
const DECLARE: &str = "01010101010101010101010101010101";
const ADMIT: &str = "02020202020202020202020202020202";
const CANCEL: &str = "03030303030303030303030303030303";

#[test]
fn cli_retry_is_explicit_replayable_and_skip_requires_enabled_policy() {
    let mut fixture = Fixture::new();
    fixture.allow_skip = true;
    fixture.initialize();
    let mut server = fixture.start();
    fixture.init_client(1, true);
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "declare",
            "--operation",
            DECLARE,
            "--entities",
            "1,2",
            "--seal",
        ],
        true,
    );
    let source = fixture.path("source");
    fs::write(&source, b"retry me once").unwrap();
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "admit",
            "--operation",
            ADMIT,
            "--declaration",
            DECLARE,
            "--work",
            "0:0:1",
            "--input",
            &source,
            "--application",
            "retry-copy/v2",
        ],
        true,
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let view = fixture.client(
            &server,
            1,
            true,
            "alice",
            &["watch", "--work", "0:0:1"],
            true,
        );
        if view.contains("state=2 ") {
            break;
        }
        assert!(Instant::now() < deadline, "{view}");
    }
    let retry = fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "retry",
            "--operation",
            CANCEL,
            "--work",
            "0:0:1",
            "--expected-attempt",
            "1",
        ],
        true,
    );
    assert_eq!(
        retry,
        fixture.client(
            &server,
            1,
            true,
            "alice",
            &["replay", "--operation", CANCEL],
            true
        )
    );
    assert!(
        fixture
            .succeeded(&server, 1, true, "0:0:1")
            .contains("attempt=2 ")
    );
    assert!(
        fixture
            .client(
                &server,
                1,
                true,
                "alice",
                &["manifest", "--work", "0:0:1", "--attempt", "2"],
                true
            )
            .contains("MANIFEST")
    );
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "skip",
            "--operation",
            "04040404040404040404040404040404",
            "--work",
            "0:0:2",
        ],
        true,
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let view = fixture.client(
            &server,
            1,
            true,
            "alice",
            &["watch", "--work", "0:0:2"],
            true,
        );
        if view.contains("state=8 ") {
            break;
        }
        assert!(Instant::now() < deadline, "{view}");
    }
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "cancel",
            "--operation",
            "05050505050505050505050505050505",
            "--work",
            "0:0:2",
        ],
        true,
    );
    assert!(
        fixture
            .client(
                &server,
                1,
                true,
                "alice",
                &["watch", "--work", "0:0:2"],
                true
            )
            .contains("state=8 ")
    );
    assert!(
        fixture
            .client(&server, 1, true, "alice", &["unresolved"], true)
            .contains("UNRESOLVED []")
    );
    server.stop();
}

#[test]
fn cli_reopens_committed_work_rotates_credentials_and_installs_verified_bytes() {
    let mut fixture = Fixture::new();
    fixture.initialize();
    let mut server = fixture.start();
    let mut next = vec!["v2".into(), "next-sequence".into()];
    next.extend(fixture.connection(&server, "alice"));
    assert!(run(fixture.directory.path(), &next, true).contains("NEXT_SEQUENCE 1"));
    fixture.init_client(1, true);
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "declare",
            "--operation",
            DECLARE,
            "--entities",
            "1",
            "--seal",
        ],
        true,
    );
    let bytes = vec![0xb3; 262144];
    let source = fixture.path("source");
    fs::write(&source, &bytes).unwrap();
    let admitted = fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "admit",
            "--operation",
            ADMIT,
            "--declaration",
            DECLARE,
            "--work",
            "0:0:1",
            "--input",
            &source,
            "--application",
            "copy/v2",
        ],
        true,
    );
    server.crash();
    let mut server = fixture.start();
    let replay = fixture.client(
        &server,
        1,
        true,
        "rotated",
        &[
            "replay",
            "--operation",
            ADMIT,
            "--declaration",
            DECLARE,
            "--input",
            &source,
        ],
        true,
    );
    assert_eq!(admitted, replay);
    fs::remove_file(&source).unwrap();
    assert!(
        fixture
            .client(
                &server,
                1,
                true,
                "rotated",
                &[
                    "replay",
                    "--operation",
                    ADMIT,
                    "--declaration",
                    DECLARE,
                    "--input",
                    &source
                ],
                false
            )
            .contains("NOT_FOUND")
    );
    assert_eq!(
        admitted,
        fixture.client(
            &server,
            1,
            true,
            "rotated",
            &["lookup", "--operation", ADMIT],
            true
        )
    );
    assert!(
        fixture
            .client(&server, 1, true, "bob", &["binding"], false)
            .contains("UNAUTHORIZED")
    );
    fixture.succeeded(&server, 1, true, "0:0:1");
    fixture.client(
        &server,
        1,
        true,
        "rotated",
        &[
            "select",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
        ],
        true,
    );
    let target = fixture.path("result");
    fixture.client(
        &server,
        1,
        true,
        "rotated",
        &[
            "read",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
            "--output",
            &target,
        ],
        true,
    );
    assert_eq!(fs::read(target).unwrap(), bytes);
    let page = fixture.client(&server, 1, true, "alice", &["page"], true);
    let seal = page
        .split_whitespace()
        .find_map(|v| v.strip_prefix("seal="))
        .unwrap();
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &["checkpoint", "--seal", seal],
        true,
    );
    assert!(
        fixture
            .client(&server, 1, true, "alice", &["complete"], true)
            .contains("COMPLETED")
    );
    server.stop();
}

#[test]
fn cli_supports_durable_only_and_authority_generated_chunks_without_fallback() {
    let mut fixture = Fixture::new();
    fixture.initialize();
    let mut server = fixture.start();
    fixture.init_client(1, false);
    let source = fixture.path("source");
    fs::write(&source, b"consume without result profile").unwrap();
    fixture.client(
        &server,
        1,
        false,
        "alice",
        &[
            "declare",
            "--operation",
            DECLARE,
            "--entities",
            "1",
            "--seal",
        ],
        true,
    );
    fixture.client(
        &server,
        1,
        false,
        "alice",
        &[
            "admit",
            "--operation",
            ADMIT,
            "--declaration",
            DECLARE,
            "--work",
            "0:0:1",
            "--input",
            &source,
            "--application",
            "consume/v2",
            "--output-count",
            "0",
        ],
        true,
    );
    fixture.succeeded(&server, 1, false, "0:0:1");
    assert!(
        fixture
            .client(
                &server,
                1,
                false,
                "alice",
                &["skip", "--operation", CANCEL, "--work", "0:0:1"],
                false
            )
            .contains("UNAUTHORIZED")
    );
    fixture.init_client(2, true);
    fixture.client(
        &server,
        2,
        true,
        "alice",
        &[
            "declare",
            "--operation",
            DECLARE,
            "--entities",
            "1",
            "--seal",
        ],
        true,
    );
    let bytes = vec![0x84; 200000];
    fs::write(&source, &bytes).unwrap();
    assert!(
        fixture
            .client(
                &server,
                2,
                true,
                "alice",
                &[
                    "admit",
                    "--operation",
                    ADMIT,
                    "--declaration",
                    DECLARE,
                    "--work",
                    "0:0:1",
                    "--input",
                    &source,
                    "--application",
                    "missing/v2"
                ],
                false
            )
            .contains("APPLICATION_UNSUPPORTED")
    );
    // A refused intent cannot be changed under the same locally retained ID.
    let operation = "04040404040404040404040404040404";
    fixture.client(
        &server,
        2,
        true,
        "alice",
        &[
            "admit",
            "--operation",
            operation,
            "--declaration",
            DECLARE,
            "--work",
            "0:0:1",
            "--input",
            &source,
            "--application",
            "chunk-copy/v2",
            "--mode",
            "2",
        ],
        true,
    );
    let view = fixture.succeeded(&server, 2, true, "0:0:1");
    assert!(view.contains("child=1:1"));
    let page = fixture.client(&server, 2, true, "alice", &["page", "--scope", "1"], true);
    assert!(page.contains("declared=4"));
    fixture.client(
        &server,
        2,
        true,
        "alice",
        &[
            "select",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
        ],
        true,
    );
    let target = fixture.path("assembled");
    fixture.client(
        &server,
        2,
        true,
        "alice",
        &[
            "read",
            "--work",
            "0:0:1",
            "--attempt",
            "1",
            "--index",
            "0",
            "--output",
            &target,
        ],
        true,
    );
    assert_eq!(fs::read(target).unwrap(), bytes);
    server.stop();
}

#[test]
fn cli_chunk_application_handles_empty_exact_partial_and_multi_batch_inputs() {
    let mut fixture = Fixture::new();
    fixture.initialize();
    let mut server = fixture.start();
    for (index, length) in [0usize, 65536, 65537, 32 * 65536 + 1]
        .into_iter()
        .enumerate()
    {
        let sequence = index as u64 + 1;
        fixture.init_client(sequence, true);
        fixture.client(
            &server,
            sequence,
            true,
            "alice",
            &[
                "declare",
                "--operation",
                DECLARE,
                "--entities",
                "1",
                "--seal",
            ],
            true,
        );
        let bytes: Vec<u8> = (0..length).map(|n| (n % 251) as u8).collect();
        let source = fixture.path(&format!("chunks-input-{sequence}"));
        fs::write(&source, &bytes).unwrap();
        fixture.client(
            &server,
            sequence,
            true,
            "alice",
            &[
                "admit",
                "--operation",
                ADMIT,
                "--declaration",
                DECLARE,
                "--work",
                "0:0:1",
                "--input",
                &source,
                "--application",
                "chunk-copy/v2",
                "--mode",
                "2",
            ],
            true,
        );
        let view = fixture.succeeded(&server, sequence, true, "0:0:1");
        assert!(view.contains("attempt=1 child=1:1"), "{view}");
        let page = fixture.client(
            &server,
            sequence,
            true,
            "alice",
            &["page", "--scope", "1"],
            true,
        );
        assert!(
            page.contains(&format!("declared={} ", length.div_ceil(65536))),
            "{page}"
        );
        assert!(page.contains("membership_verified=true"), "{page}");
        fixture.client(
            &server,
            sequence,
            true,
            "alice",
            &[
                "select",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0",
            ],
            true,
        );
        let target = fixture.path(&format!("chunks-output-{sequence}"));
        fixture.client(
            &server,
            sequence,
            true,
            "alice",
            &[
                "read",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0",
                "--output",
                &target,
            ],
            true,
        );
        assert_eq!(fs::read(target).unwrap(), bytes);
    }
    server.stop();
}

#[test]
fn cli_caller_branch_exact_coverage_cancellation_and_offline_revocation() {
    let mut fixture = Fixture::new();
    fixture.initialize();
    let mut server = fixture.start();
    fixture.init_client(1, true);
    let parent = fixture.path("parent");
    fs::write(&parent, b"onetwo").unwrap();
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "declare",
            "--operation",
            DECLARE,
            "--entities",
            "1",
            "--seal",
        ],
        true,
    );
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "admit",
            "--operation",
            ADMIT,
            "--declaration",
            DECLARE,
            "--work",
            "0:0:1",
            "--input",
            &parent,
            "--application",
            "reassemble/v2",
            "--mode",
            "1",
        ],
        true,
    );
    let view = fixture.client(
        &server,
        1,
        true,
        "alice",
        &["watch", "--work", "0:0:1"],
        true,
    );
    assert!(view.contains("child=1:0"));
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "declare",
            "--operation",
            CANCEL,
            "--scope",
            "1",
            "--entities",
            "1,2",
            "--seal",
        ],
        true,
    );
    for (entity, operation, bytes) in [
        ("1:0:1", "04040404040404040404040404040404", b"one"),
        ("1:0:2", "05050505050505050505050505050505", b"two"),
    ] {
        let source = fixture.path(entity);
        fs::write(&source, bytes).unwrap();
        fixture.client(
            &server,
            1,
            true,
            "alice",
            &[
                "admit",
                "--operation",
                operation,
                "--declaration",
                CANCEL,
                "--work",
                entity,
                "--input",
                &source,
                "--application",
                "copy/v2",
            ],
            true,
        );
        fixture.succeeded(&server, 1, true, entity);
    }
    fixture.succeeded(&server, 1, true, "0:0:1");
    for scope in ["1", "0"] {
        let page = fixture.client(&server, 1, true, "alice", &["page", "--scope", scope], true);
        let seal = page
            .split_whitespace()
            .find_map(|v| v.strip_prefix("seal="))
            .unwrap();
        fixture.client(
            &server,
            1,
            true,
            "alice",
            &["checkpoint", "--scope", scope, "--seal", seal],
            true,
        );
    }
    fixture.client(&server, 1, true, "alice", &["complete"], true);
    assert!(
        fixture
            .client(
                &server,
                1,
                true,
                "alice",
                &[
                    "retry",
                    "--operation",
                    "06060606060606060606060606060606",
                    "--work",
                    "0:0:1",
                    "--expected-attempt",
                    "1"
                ],
                false
            )
            .contains("ALREADY_TERMINAL")
    );
    fixture.init_client(2, true);
    fixture.client(
        &server,
        2,
        true,
        "alice",
        &["declare", "--operation", DECLARE, "--entities", "1"],
        true,
    );
    let cancelled = fixture.client(
        &server,
        2,
        true,
        "alice",
        &["cancel-scope", "--operation", CANCEL],
        true,
    );
    assert_eq!(
        cancelled,
        fixture.client(
            &server,
            2,
            true,
            "alice",
            &["replay", "--operation", CANCEL],
            true
        )
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let view = fixture.client(
            &server,
            2,
            true,
            "alice",
            &["watch", "--work", "0:0:1"],
            true,
        );
        if view.contains("state=7 ") {
            break;
        }
        assert!(Instant::now() < deadline, "{view}");
    }
    server.stop();
    let mut revoke = vec!["v2".into(), "revoke".into()];
    revoke.extend(fixture.storage());
    revoke.extend([
        "--owner".into(),
        "alice".into(),
        "--generation".into(),
        "2".into(),
    ]);
    assert!(run(fixture.directory.path(), &revoke, true).contains("SESSION_REVOKED 2"));
    let mut server = fixture.start();
    assert!(
        fixture
            .client(&server, 2, true, "alice", &["binding"], false)
            .contains("UNAUTHORIZED")
    );
    fixture.client(&server, 1, true, "alice", &["detach"], true);
    server.stop();
}

#[test]
fn cli_initialization_and_reopen_never_replace_missing_or_existing_history() {
    let mut fixture = Fixture::new();
    let mut init = vec!["v2".into(), "init-authority".into()];
    init.extend(fixture.storage());
    init.retain(|a| a != "--trust-system-clock");
    assert!(run(fixture.directory.path(), &init, false).contains("trust-system-clock"));
    assert!(!Path::new(&fixture.path("authority.sqlite")).exists());
    fixture.initialize();
    init.push("--trust-system-clock".into());
    run(fixture.directory.path(), &init, false);
    let mut server = fixture.start();
    // A client connection must not initialize a missing local journal.
    fixture.client(&server, 1, true, "alice", &["binding"], false);
    fixture.init_client(1, true);
    fixture.client(&server, 1, true, "alice", &["binding"], true);
    let mut repeated = vec!["v2".into(), "init-client".into()];
    repeated.extend(fixture.journal(1, true));
    run(fixture.directory.path(), &repeated, false);
    fixture.client(&server, 1, true, "alice", &["binding"], true);
    let mut next = vec!["v2".into(), "next-sequence".into()];
    next.extend(fixture.connection(&server, "alice"));
    assert!(run(fixture.directory.path(), &next, true).contains("NEXT_SEQUENCE 2"));
    server.stop();
}
