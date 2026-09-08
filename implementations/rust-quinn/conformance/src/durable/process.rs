//! V2 process ownership. The fixture spawns every subject process, kills only
//! what it spawned, and reaps on every path. Readiness is a non-empty
//! ready file plus a successful authenticated client op — a live process
//! alone is not readiness.

use crate::durable::mtls::{AUTHORITY, Material};
use crate::{ensure_success, path, run_output_owned, unique_suffix, wait_output};
use anyhow::{Context, Result, bail, ensure};
use std::{
    fs,
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const OP_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Everything one scenario needs to own a V2 authority: storage roots,
/// credentials, and the subject binary. `init-authority` has been run against
/// these roots before `start_server` is used.
pub struct AuthorityFixture {
    pub bin: PathBuf,
    pub root: PathBuf,
    pub state_db: PathBuf,
    pub object_dir: PathBuf,
    pub certs: Material,
}

impl AuthorityFixture {
    pub fn new(bin: &Path, root: &Path, certs: Material) -> Result<Self> {
        fs::create_dir_all(root)?;
        Ok(Self {
            bin: bin.to_path_buf(),
            root: root.to_path_buf(),
            state_db: root.join("authority.sqlite"),
            object_dir: root.join("objects"),
            certs,
        })
    }

    pub fn base(&self) -> Vec<String> {
        vec![path(&self.bin), "v2".to_owned()]
    }

    pub fn storage_args(&self) -> Vec<String> {
        vec![
            "--state-db".into(),
            path(&self.state_db),
            "--object-dir".into(),
            path(&self.object_dir),
            "--authority".into(),
            AUTHORITY.to_owned(),
            "--principal-map".into(),
            path(&self.certs.principal_map),
            "--trust-system-clock".into(),
        ]
    }

    pub fn run_init_authority(&self) -> Result<()> {
        let mut command = with_owned(&self.base(), &["init-authority".into()]);
        command.extend(self.storage_args());
        let output = run_output_owned(&self.root, &command, OP_TIMEOUT)?;
        ensure_success(&output, "v2 init-authority")?;
        ensure!(
            String::from_utf8_lossy(&output.stdout).contains("AUTHORITY_INITIALIZED"),
            "init-authority did not confirm initialization\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    pub fn connection_args(&self, server: &OwnedServer, principal: &str) -> Result<Vec<String>> {
        let identity = self.certs.principal(principal)?;
        Ok(vec![
            "--connect".into(),
            server.address.clone(),
            "--server-name".into(),
            "localhost".into(),
            "--ca".into(),
            path(&self.certs.ca_cert),
            "--cert".into(),
            path(&identity.cert),
            "--key".into(),
            path(&identity.key),
        ])
    }

    /// Journal args shared by `init-client` and every `client` op.
    pub fn journal_args(&self, journal: &Path, owner: &str, creation_sequence: u64) -> Vec<String> {
        vec![
            "--journal".into(),
            path(journal),
            "--authority".into(),
            AUTHORITY.to_owned(),
            "--owner".into(),
            owner.to_owned(),
            "--creation-sequence".into(),
            creation_sequence.to_string(),
        ]
    }

    pub fn run_client_op(
        &self,
        journal: &Path,
        owner: &str,
        creation_sequence: u64,
        connection: &[String],
        operation: &[&str],
    ) -> Result<Output> {
        let mut command = with_owned(&self.base(), &["client".into()]);
        command.extend(self.journal_args(journal, owner, creation_sequence));
        command.extend(connection.iter().cloned());
        command.extend(operation.iter().map(|value| (*value).to_owned()));
        run_output_owned(&self.root, &command, OP_TIMEOUT)
    }

    /// Spawn a one-shot client op without waiting for it. The returned child
    /// is fixture-owned: callers must reap it (bounded) on every path.
    pub fn spawn_client_op(
        &self,
        journal: &Path,
        owner: &str,
        creation_sequence: u64,
        connection: &[String],
        operation: &[&str],
    ) -> Result<Child> {
        let mut command = with_owned(&self.base(), &["client".into()]);
        command.extend(self.journal_args(journal, owner, creation_sequence));
        command.extend(connection.iter().cloned());
        command.extend(operation.iter().map(|value| (*value).to_owned()));
        ensure!(!command.is_empty(), "empty client command");
        Command::new(&command[0])
            .args(&command[1..])
            .current_dir(&self.root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("start {}", command.join(" ")))
    }

    /// Reap a spawned client op with bounded output draining.
    pub fn wait_client_op(child: Child, timeout: Duration) -> Result<Output> {
        wait_output(child, timeout)
    }

    /// Authenticated readiness probe: `next-sequence` over mTLS.
    pub fn next_sequence(&self, server: &OwnedServer, principal: &str) -> Result<u64> {
        let connection = self.connection_args(server, principal)?;
        let mut command = self.base();
        command.push("next-sequence".into());
        command.extend(connection);
        let output = run_output_owned(&self.root, &command, OP_TIMEOUT)?;
        ensure_success(&output, "v2 next-sequence readiness probe")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let sequence = stdout
            .split_whitespace()
            .nth(1)
            .context("next-sequence did not print NEXT_SEQUENCE N")?
            .parse::<u64>()
            .context("next-sequence printed a non-decimal sequence")?;
        Ok(sequence)
    }

    /// Spawn `v2 serve`, wait for the ready file, then require one successful
    /// authenticated op before declaring the server ready.
    pub fn start_server(&self) -> Result<OwnedServer> {
        let serial = unique_suffix();
        let ready = self.root.join(format!("ready-{serial:x}"));
        let log = self.root.join(format!("server-{serial:x}.log"));
        let mut command = with_owned(&self.base(), &["serve".into()]);
        command.extend(self.storage_args());
        command.extend([
            "--bind".into(),
            "127.0.0.1:0".into(),
            "--cert".into(),
            path(&self.certs.server.cert),
            "--key".into(),
            path(&self.certs.server.key),
            "--client-ca".into(),
            path(&self.certs.ca_cert),
            "--result-authority".into(),
            "localhost:7443".into(),
            "--ready-file".into(),
            path(&ready),
        ]);
        let log_file = File::create(&log)?;
        let mut spawned = Command::new(&command[0]);
        spawned
            .args(&command[1..])
            .current_dir(&self.root)
            .stdout(Stdio::from(log_file.try_clone()?))
            .stderr(Stdio::from(log_file));
        let mut child = spawned
            .spawn()
            .with_context(|| format!("start {}", command.join(" ")))?;
        let deadline = Instant::now() + READY_TIMEOUT;
        let address = loop {
            if let Ok(text) = fs::read_to_string(&ready)
                && let Some(address) = parse_ready(&text)
            {
                break address;
            }
            if let Some(status) = child.try_wait()? {
                bail!(
                    "v2 serve exited before readiness ({status})\nlog:\n{}",
                    fs::read_to_string(&log).unwrap_or_default()
                );
            }
            ensure!(Instant::now() < deadline, "v2 serve readiness timed out");
            thread::sleep(Duration::from_millis(25));
        };
        let server = OwnedServer {
            name: "v2-serve",
            child: Some(child),
            address,
            log,
        };
        // A live process plus a marker file is not readiness: an authenticated
        // op must succeed before the fixture counts the server as ready.
        self.next_sequence(&server, "alice")?;
        Ok(server)
    }

    /// Restart the same target against the same state DB, object dir and
    /// principal map with a fresh ready file and process start. Valid only
    /// after the fixture itself stopped or killed the previous process.
    #[allow(dead_code)] // exercised by the milestone-2 crash/restart rows
    pub fn restart_server(&self, previous: OwnedServer) -> Result<OwnedServer> {
        previous.kill()?;
        self.start_server()
    }
}

fn parse_ready(text: &str) -> Option<String> {
    let trimmed = text.trim();
    trimmed
        .parse::<std::net::SocketAddr>()
        .ok()
        .map(|_| trimmed.to_owned())
}

fn with_owned(base: &[String], arguments: &[String]) -> Vec<String> {
    let mut result = base.to_vec();
    result.extend(arguments.iter().cloned());
    result
}

pub struct OwnedServer {
    pub name: &'static str,
    child: Option<Child>,
    pub address: String,
    pub log: PathBuf,
}

impl OwnedServer {
    /// Graceful stop: SIGTERM, then require a drained, zero exit.
    pub fn stop(mut self) -> Result<Output> {
        let mut child = self
            .child
            .take()
            .context("server process already consumed")?;
        ensure!(
            child.try_wait()?.is_none(),
            "{} was already reaped before stop",
            self.name
        );
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .context("send SIGTERM to fixture-owned server")?;
        ensure!(status.success(), "failed to signal {}", self.name);
        let output = wait_output(child, SHUTDOWN_TIMEOUT)?;
        let log = fs::read_to_string(&self.log).unwrap_or_default();
        ensure!(
            output.status.success() && log.contains("DRAINED"),
            "{} did not drain cleanly ({})\nlog:\n{log}",
            self.name,
            output.status
        );
        Ok(output)
    }

    /// Immediate hard kill (SIGKILL). Process death only; not power-loss
    /// coverage. The child is reaped before returning.
    pub fn kill(mut self) -> Result<()> {
        let mut child = self
            .child
            .take()
            .context("server process already consumed")?;
        child.kill().context("SIGKILL fixture-owned server")?;
        child.wait().context("reap killed server")?;
        Ok(())
    }
}

impl Drop for OwnedServer {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_file_text_must_parse_as_a_socket_address() {
        assert_eq!(
            parse_ready("127.0.0.1:7443\n"),
            Some("127.0.0.1:7443".to_owned())
        );
        assert_eq!(parse_ready(""), None);
        assert_eq!(parse_ready("not-an-address"), None);
        assert_eq!(parse_ready("127.0.0.1:99999"), None);
    }
}
