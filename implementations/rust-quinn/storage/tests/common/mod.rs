//! Shared docker harness for storage-backend integration tests.
//!
//! Each helper starts a throwaway container, waits for readiness, and
//! returns a handle whose Drop removes the container. Tests skip (pass
//! trivially) when docker is unavailable.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// True when a docker daemon answers. Integration tests skip otherwise.
pub fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// A running throwaway container. Dropping kills and removes it.
#[derive(Debug)]
pub struct Container {
    id: String,
}

impl Container {
    /// Starts `image` with `args`, publishing `container_port` on an
    /// ephemeral localhost port. Extra `-e KEY=VAL` environment pairs may
    /// be passed in `env`.
    pub fn start(image: &str, args: &[&str], env: &[&str], container_port: u16) -> Option<Self> {
        let mut command = Command::new("docker");
        command.args(["run", "-d", "--rm", "-p", &format!("127.0.0.1:0:{container_port}")]);
        for pair in env {
            command.args(["-e", pair]);
        }
        command.arg(image);
        command.args(args);
        let output = command.output().ok()?;
        if !output.status.success() {
            eprintln!("docker run failed: {}", String::from_utf8_lossy(&output.stderr));
            return None;
        }
        let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Some(Container { id })
    }

    /// The ephemeral localhost port mapped to `container_port`.
    pub fn host_port(&self, container_port: u16) -> Option<u16> {
        let output = Command::new("docker")
            .args(["port", &self.id, &container_port.to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
        text.rsplit(':').next()?.parse().ok()
    }

    /// Runs a command inside the container, returning its output.
    #[allow(dead_code)] // used by backends whose readiness probe is in-container
    pub fn exec(&self, args: &[&str]) -> Option<std::process::Output> {
        let mut command = Command::new("docker");
        command.args(["exec", &self.id]);
        command.args(args);
        command.output().ok()
    }

    /// Waits until `ready` returns true, up to `timeout`. Returns false on
    /// timeout.
    pub fn wait_until(&self, timeout: Duration, mut ready: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if ready() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        ready()
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// A running postgres container with its base connection URL.
#[derive(Debug)]
pub struct PostgresContainer {
    // Held for its Drop impl: kills the container when this handle drops.
    _container: Container,
    /// URL without database, e.g. `postgres://postgres:pipestream@127.0.0.1:32768`.
    pub base_url: String,
    db_seq: std::sync::atomic::AtomicU64,
}

impl PostgresContainer {
    /// Starts a postgres container and waits for it to accept connections.
    /// Returns None when docker is unavailable or startup times out.
    pub fn start() -> Option<Self> {
        if !docker_available() {
            return None;
        }
        let container = Container::start(
            "postgres:18.4-alpine",
            &[],
            &[
                "POSTGRES_PASSWORD=pipestream",
                "POSTGRES_USER=postgres",
                "POSTGRES_DB=postgres",
            ],
            5432,
        )?;
        let port = container.host_port(5432)?;
        let base_url = format!("postgres://postgres:pipestream@127.0.0.1:{port}");
        let ready_url = format!("{base_url}/postgres");
        let ready = container.wait_until(Duration::from_secs(60), || {
            postgres::Client::connect(&ready_url, postgres::NoTls).is_ok()
        });
        if !ready {
            eprintln!("postgres container did not become ready");
            return None;
        }
        Some(PostgresContainer {
            _container: container,
            base_url,
            db_seq: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Creates a fresh empty database and returns its connection URL.
    pub fn fresh_database(&self) -> String {
        let seq = self.db_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let db = format!("spi_{}_{}", std::process::id(), seq);
        let mut client = postgres::Client::connect(&format!("{}/postgres", self.base_url), postgres::NoTls)
            .expect("connect to maintenance database");
        client
            .execute(&format!("CREATE DATABASE {db}"), &[])
            .expect("create fresh database");
        format!("{}/{db}", self.base_url)
    }
}
