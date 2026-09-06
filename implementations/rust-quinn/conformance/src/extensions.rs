//! Raw QUIC probes use frozen bytes, not an implementation's protocol codec.

use crate::*;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{collections::BTreeMap, ffi::OsString, io::Read, sync::Arc};

const CASES: &str = include_str!("../../../../test-vectors/extension-negotiation.tsv");

pub fn run() -> Result<()> {
    let root = repository_root()?;
    let temporary = tempfile::tempdir()?;
    let certs = temporary.path().join("certs");
    certificates(&certs)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut programs = implementations(&root)?;
    programs.push(Program {
        name: "rust-recursive",
        command: vec![path(
            &root.join("implementations/rust-quinn/target/release/pipestream-quinn"),
        )],
    });
    let server_cases = [
        ("optional-unknown", None),
        ("client-required-unknown", Some(0x0f)),
        ("required-not-supported", Some(0x0d)),
        ("duplicate", Some(0x0d)),
        ("too-many", Some(0x0d)),
        ("rejected-then-valid", Some(0x0f)),
    ];
    let mut count = 0;
    for program in &programs {
        for (name, refusal) in server_cases {
            let mut server = if program.name == "rust-recursive" {
                start_recursive_server(
                    &root,
                    program,
                    &temporary.path().join(format!("recursive-{name}")),
                    &certs,
                )?
            } else {
                start_server(&root, program, &temporary.path().join(name), &certs)?
            };
            let startup_files = startup_snapshot(&server.output)?;
            let input = fixture(
                if name == "rejected-then-valid" {
                    "client-required-unknown"
                } else {
                    name
                },
                3,
            )?;
            runtime
                .block_on(tokio_timeout(async {
                    let endpoint = client_endpoint(&certs)?;
                    let connection = endpoint
                        .connect(server.address.parse()?, "localhost")?
                        .await?;
                    let (mut send, mut recv) = connection.open_bi().await?;
                    if name == "rejected-then-valid" {
                        let mut entity = connection.open_uni().await?;
                        entity
                            .write_all(&fs::read(root.join("test-vectors/valid/entity-text.bin"))?)
                            .await?;
                        entity.finish()?;
                        let mut pipeline = frame_capabilities(&input);
                        pipeline.extend(frame_capabilities(&fixture("optional-unknown", 2)?));
                        pipeline.extend(fs::read(
                            root.join("test-vectors/valid/status-pending.bin"),
                        )?);
                        send.write_all(&pipeline).await?;
                    } else {
                        write_capabilities(&mut send, &input).await?;
                    }
                    if let Some(code) = refusal {
                        expect_close(&connection, code).await?;
                    } else {
                        ensure!(
                            read_capabilities(&mut recv).await? == fixture(name, 4)?,
                            "{} selected an unknown optional extension",
                            program.name
                        );
                        // A second exchange is not a renegotiation mechanism.
                        write_capabilities(&mut send, &input).await?;
                        expect_close(&connection, 0x0d).await?;
                    }
                    endpoint.wait_idle().await;
                    Ok(())
                }))
                .with_context(|| format!("{} server probe {name}", program.name))?;
            finish_server_with_refusal(
                &mut server,
                if refusal == Some(0x0f) {
                    "PIPESTREAM_EXTENSION_UNSUPPORTED"
                } else {
                    "PIPESTREAM_FRAME_ERROR"
                },
            )?;
            ensure!(
                startup_snapshot(&server.output)? == startup_files,
                "retained output changed before negotiation"
            );
            println!("PASS {} server capabilities {name}", program.name);
            count += 1;
        }
    }

    let payload = temporary.path().join("unused-payload");
    fs::write(&payload, b"must not be admitted")?;
    for program in &programs {
        for name in ["unsolicited-selected-response", "window-escalation"] {
            let response = fixture(name, 3)?;
            let (endpoint, address) = runtime.block_on(async {
                let endpoint = server_endpoint(&certs)?;
                let address = endpoint.local_addr()?;
                Ok::<_, anyhow::Error>((endpoint, address))
            })?;
            let mut command = program.command.clone();
            command.extend([
                if program.name == "rust-recursive" {
                    "recursive-scenario"
                } else {
                    "send"
                }
                .to_owned(),
                "--connect".to_owned(),
                address.to_string(),
                "--ca".to_owned(),
                path(&certs.join("ca.crt")),
                "--server-name".to_owned(),
                "localhost".to_owned(),
            ]);
            if program.name == "rust-recursive" {
                command.extend(["--session-id".to_owned(), "capabilities-refusal".to_owned()]);
            } else {
                command.extend([
                    "--entity-id".to_owned(),
                    "7".to_owned(),
                    "--input".to_owned(),
                    path(&payload),
                ]);
            }
            let child = spawn_owned(&root, &command)?;
            let probe = runtime.block_on(tokio_timeout(async {
                let connection = endpoint.accept().await.context("listener closed")?.await?;
                let (mut send, mut recv) = connection.accept_bi().await?;
                let _ = read_capabilities(&mut recv).await?;
                write_capabilities(&mut send, &response).await?;
                tokio::select! {
                    result = connection.accept_uni() => ensure!(result.is_err(), "client sent application work after invalid response"),
                    _ = connection.closed() => ensure!(connection.accept_uni().await.is_err(), "client queued application work after invalid response"),
                }
                Ok(())
            }));
            let output = wait_output(child, Duration::from_secs(10))?;
            probe.with_context(|| format!("{} client probe {name}", program.name))?;
            verify_client_refusal(program.name, name, &output)?;
            println!("PASS {} client capabilities {name}", program.name);
            count += 1;
        }
    }
    println!("all {count} raw QUIC capability probes passed");
    Ok(())
}

fn verify_client_refusal(program: &str, case: &str, output: &Output) -> Result<()> {
    ensure!(
        output.status.code().is_some_and(|code| code != 0)
            && String::from_utf8_lossy(&output.stderr).contains("PIPESTREAM_FRAME_ERROR"),
        "{program} client probe {case} did not exit with a named invalid-response refusal ({})\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

// Capture implementation-owned startup files without interpreting or exempting
// any filename. A refused connection must leave exactly the same bytes behind.
fn startup_snapshot(directory: &Path) -> Result<BTreeMap<OsString, Vec<u8>>> {
    let mut snapshot = BTreeMap::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        ensure!(snapshot.len() < 64, "too many startup files");
        ensure!(
            entry.file_type()?.is_file(),
            "unexpected retained output directory or alias"
        );
        let mut bytes = Vec::new();
        fs::File::open(entry.path())?
            .take(65_537)
            .read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 65_536, "startup file exceeds snapshot bound");
        snapshot.insert(entry.file_name(), bytes);
    }
    Ok(snapshot)
}

fn fixture(name: &str, column: usize) -> Result<Vec<u8>> {
    let row = CASES
        .lines()
        .skip(1)
        .find(|line| line.split('\t').next() == Some(name))
        .with_context(|| format!("missing capability fixture {name}"))?;
    decode_hex(
        row.split('\t')
            .nth(column)
            .context("missing fixture column")?,
    )
}

async fn tokio_timeout(future: impl std::future::Future<Output = Result<()>>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .context("capability probe timeout")?
}

fn client_endpoint(certs: &Path) -> Result<quinn::Endpoint> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(certs.join("ca.crt"))? {
        roots.add(cert?)?;
    }
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"pipestream/1".to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls)?,
    )));
    Ok(endpoint)
}

fn server_endpoint(certs: &Path) -> Result<quinn::Endpoint> {
    let chain =
        CertificateDer::pem_file_iter(certs.join("server.crt"))?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(certs.join("server.key"))?;
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)?;
    tls.alpn_protocols = vec![b"pipestream/1".to_vec()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(8u32.into());
    config.transport_config(Arc::new(transport));
    Ok(quinn::Endpoint::server(config, "127.0.0.1:0".parse()?)?)
}

async fn write_capabilities(send: &mut quinn::SendStream, body: &[u8]) -> Result<()> {
    send.write_all(&frame_capabilities(body)).await?;
    Ok(())
}

fn frame_capabilities(body: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x80];
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(body);
    frame
}

async fn read_capabilities(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
    let mut header = [0; 5];
    recv.read_exact(&mut header).await?;
    ensure!(
        header[0] == 0x80,
        "expected CAPABILITIES before any other response"
    );
    let length = u32::from_be_bytes(header[1..].try_into()?) as usize;
    ensure!(length <= 4096, "probe capability frame too large");
    let mut body = vec![0; length];
    recv.read_exact(&mut body).await?;
    Ok(body)
}

async fn expect_close(connection: &quinn::Connection, code: u64) -> Result<()> {
    let error = connection.closed().await;
    match error {
        quinn::ConnectionError::ApplicationClosed(close) => ensure!(
            close.error_code.into_inner() == code,
            "wrong QUIC refusal code: {close:?}, expected {code}"
        ),
        other => bail!("expected named QUIC application close, got {other}"),
    }
    Ok(())
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn client_refusal_requires_ordinary_failure_and_preserves_exit_diagnostics() {
        use std::os::unix::process::ExitStatusExt;
        let mut output = Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: b"stdout marker".to_vec(),
            stderr: b"PIPESTREAM_FRAME_ERROR: invalid response".to_vec(),
        };
        assert!(verify_client_refusal("client", "window-escalation", &output).is_ok());
        output.status = std::process::ExitStatus::from_raw(11);
        let refusal = verify_client_refusal("client", "window-escalation", &output)
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("client probe window-escalation"));
        assert!(refusal.contains("signal: 11"));
        assert!(refusal.contains("stdout marker"));
        assert!(refusal.contains("PIPESTREAM_FRAME_ERROR"));
        output.status = std::process::ExitStatus::from_raw(0);
        assert!(verify_client_refusal("client", "window-escalation", &output).is_err());
        output.status = std::process::ExitStatus::from_raw(1 << 8);
        output.stderr.clear();
        assert!(verify_client_refusal("client", "window-escalation", &output).is_err());
    }

    #[test]
    fn startup_snapshot_detects_added_and_changed_output_without_exempt_names() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(".policy"), b"initial").unwrap();
        let initial = startup_snapshot(dir.path()).unwrap();
        assert_eq!(initial, startup_snapshot(dir.path()).unwrap());
        fs::write(dir.path().join(".policy"), b"changed").unwrap();
        assert_ne!(initial, startup_snapshot(dir.path()).unwrap());
        fs::write(dir.path().join(".policy"), b"initial").unwrap();
        fs::write(dir.path().join("payload"), b"admitted").unwrap();
        assert_ne!(initial, startup_snapshot(dir.path()).unwrap());
        fs::create_dir(dir.path().join("scope-0")).unwrap();
        assert!(startup_snapshot(dir.path()).is_err());
    }
}
