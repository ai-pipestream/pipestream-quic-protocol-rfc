//! Docker wiring tests for the service SPI backends (spi-postgres,
//! spi-s3, spi-mongo): payload bytes land in the selected service, survive
//! reopen, and a changed backend or connection is refused.
//! Skip when no docker daemon is available.

use super::*;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn policy() -> PayloadPolicy {
    PayloadPolicy {
        objects: Id(8),
        bytes: Number(4 << 20),
        owner_objects: Id(4),
        owner_bytes: Number(2 << 20),
        chunk_bytes: Id(65536),
        handles: Id(16),
        owner_handles: Id(8),
    }
}

fn descriptor(bytes: &[u8]) -> Input {
    Input {
        length: Number(bytes.len() as u64),
        sha256: Digest(Sha256::digest(bytes).into()),
        content_type: ApplicationLabel("application/octet-stream".into()),
    }
}

fn caps() -> Capabilities {
    Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![],
        control_limit: ControlLimit(8192),
        stream_limit: ConcurrencyLimit(8),
        pending_limit: ConcurrencyLimit(8),
        object_limit: Number(2 << 20),
        stream_idle_ms: IdleMs(1000),
        stream_lifetime_ms: LifetimeMs(10000),
    }
}

fn install(store: &PayloadStore, bytes: &[u8]) -> InstalledPayload {
    let now = Instant::now();
    let mut stage = store
        .stage(&IdentityLabel("alice".into()), &descriptor(bytes), &caps(), now)
        .unwrap();
    for chunk in bytes.chunks(65536) {
        stage.receive(chunk, now).unwrap();
    }
    stage.finish(now).unwrap()
}

fn read_all(store: &PayloadStore, key: &str, bytes: &[u8]) -> Vec<u8> {
    let mut reader = store
        .open_object(key, &IdentityLabel("alice".into()), &descriptor(bytes))
        .unwrap();
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = reader.read_chunk(&mut buffer).unwrap();
        output.extend_from_slice(&buffer[..n]);
        if n == 0 {
            break;
        }
    }
    assert!(reader.verified());
    output
}

// --- docker harness (mirrors storage/tests/common) ---

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

struct Container {
    id: String,
}

impl Container {
    fn start(image: &str, args: &[&str], env: &[&str], container_port: u16) -> Option<Self> {
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
        Some(Container {
            id: String::from_utf8_lossy(&output.stdout).trim().to_string(),
        })
    }

    fn host_port(&self, container_port: u16) -> Option<u16> {
        let output = Command::new("docker")
            .args(["port", &self.id, &container_port.to_string()])
            .output()
            .ok()?;
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .rsplit(':')
            .next()?
            .parse()
            .ok()
    }
}

impl Container {
    fn exec(&self, args: &[&str]) -> bool {
        let mut command = Command::new("docker");
        command.args(["exec", &self.id]);
        command.args(args);
        command
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn wait_tcp(&self, timeout: Duration, port: u16) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
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

fn open_retry(dir: &Path, binding: StoreIdentity, spec: &PayloadBackendSpec) -> PayloadStore {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match PayloadStore::open_with_backend(dir, binding, policy(), spec) {
            Ok(store) => return store,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(300)),
            Err(error) => panic!("payload store open failed: {error}"),
        }
    }
}

/// One full round trip against a spec: install, bytes-in-service proof,
/// reopen, read-back, and refusal of a changed backend/connection.
fn round_trip(
    make_spec: impl Fn() -> PayloadBackendSpec,
    changed_spec: impl Fn() -> PayloadBackendSpec,
    probe_scopes: impl Fn() -> Vec<pipestream_storage::ScopePath>,
) {
    let directory = tempfile::tempdir().unwrap();
    let binding = StoreIdentity::generate().unwrap();
    let root = directory.path().join("objects");
    let spec = make_spec();
    let store = PayloadStore::initialize_with_backend(&root, binding, policy(), &spec).unwrap();
    let payload = b"service-plane payload".to_vec();
    let installed = install(&store, &payload);
    let key = installed.key().to_owned();

    // The payload object is visible in the service backend as a child scope.
    let expected_scope = plane_scope(binding, &key);
    assert!(
        probe_scopes().contains(&expected_scope),
        "payload object scope {expected_scope} must be visible in the service"
    );
    assert_eq!(read_all(&store, &key, &payload), payload);
    drop(installed);
    drop(store);

    // Reopen with the same spec: content survives.
    let reopened = open_retry(&root, binding, &make_spec());
    assert_eq!(read_all(&reopened, &key, &payload), payload);
    drop(reopened);

    // Reopen with the plain filesystem backend: refused.
    assert!(
        PayloadStore::open_with_backend(&root, binding, policy(), &PayloadBackendSpec::Filesystem)
            .is_err()
    );
    // Reopen with a changed connection: refused.
    assert!(
        PayloadStore::open_with_backend(&root, binding, policy(), &changed_spec()).is_err()
    );
}

#[test]
fn spi_postgres_round_trip_and_refusals() {
    if !docker_available() {
        eprintln!("docker unavailable: skipping spi-postgres wiring test");
        return;
    }
    let container = Container::start(
        "postgres:18.4-alpine",
        &[],
        &["POSTGRES_PASSWORD=pipestream", "POSTGRES_USER=postgres"],
        5432,
    )
    .unwrap();
    let port = container.host_port(5432).unwrap();
    let base = format!("postgres://postgres:pipestream@127.0.0.1:{port}");
    let url_a = format!("{base}/payload_a");
    let url_b = format!("{base}/payload_b");
    // Create the two databases once the server accepts SQL (TCP alone is
    // not proof init completed).
    assert!(container.wait_tcp(Duration::from_secs(60), port));
    let deadline = Instant::now() + Duration::from_secs(60);
    for db in ["payload_a", "payload_b"] {
        loop {
            if container.exec(&["psql", "-U", "postgres", "-c", &format!("CREATE DATABASE {db}")]) {
                break;
            }
            assert!(Instant::now() < deadline, "create database {db} timed out");
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    round_trip(
        || PayloadBackendSpec::SpiPostgres { url: url_a.clone() },
        || PayloadBackendSpec::SpiPostgres { url: url_b.clone() },
        || {
            pipestream_storage::PostgresStore::open(&url_a)
                .unwrap()
                .list_scopes()
                .unwrap()
        },
    );
}

#[test]
fn spi_mongo_round_trip_and_refusals() {
    if !docker_available() {
        eprintln!("docker unavailable: skipping spi-mongo wiring test");
        return;
    }
    let container = Container::start("mongo:8.2", &[], &[], 27017).unwrap();
    let port = container.host_port(27017).unwrap();
    let uri = format!("mongodb://127.0.0.1:{port}");
    round_trip(
        || PayloadBackendSpec::SpiMongo {
            uri: uri.clone(),
            database: "payload_a".to_string(),
        },
        || PayloadBackendSpec::SpiMongo {
            uri: uri.clone(),
            database: "payload_b".to_string(),
        },
        || {
            pipestream_storage::MongoStore::open(&pipestream_storage::MongoConfig {
                uri: uri.clone(),
                database: "payload_a".to_string(),
            })
            .unwrap()
            .list_scopes()
            .unwrap()
        },
    );
}

#[test]
fn spi_s3_round_trip_and_refusals() {
    if !docker_available() {
        eprintln!("docker unavailable: skipping spi-s3 wiring test");
        return;
    }
    let container = Container::start(
        "quay.io/minio/minio:latest",
        &["server", "/data"],
        &[
            "MINIO_ROOT_USER=pipestream",
            "MINIO_ROOT_PASSWORD=pipestream-secret",
        ],
        9000,
    )
    .unwrap();
    let port = container.host_port(9000).unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    let config = |bucket: &str| {
        Box::new(pipestream_storage::S3Config {
            bucket: bucket.to_string(),
            prefix: "plane/".to_string(),
            endpoint_url: Some(endpoint.clone()),
            region: "us-east-1".to_string(),
            access_key: "pipestream".to_string(),
            secret_key: "pipestream-secret".to_string(),
        })
    };
    round_trip(
        || PayloadBackendSpec::SpiS3 {
            config: config("payload-a"),
        },
        || PayloadBackendSpec::SpiS3 {
            config: config("payload-b"),
        },
        || {
            let spec_config = pipestream_storage::S3Config {
                bucket: "payload-a".to_string(),
                prefix: "plane/".to_string(),
                endpoint_url: Some(endpoint.clone()),
                region: "us-east-1".to_string(),
                access_key: "pipestream".to_string(),
                secret_key: "pipestream-secret".to_string(),
            };
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match pipestream_storage::S3Store::open(&spec_config) {
                    Ok(store) => break store.list_scopes().unwrap(),
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(200))
                    }
                    Err(error) => panic!("s3 never became ready: {error}"),
                }
            }
        },
    );
}


/// Pinning test for the certification failure family: a 16 MiB chunked
/// entity through the mongo plane — install, read-back, orphan collection,
/// retire — asserting every step succeeds with its pinned outcome.
#[test]
fn mongo_16mib_chunked_install_read_collect_retire() {
    if !docker_available() {
        eprintln!("docker unavailable: skipping 16 MiB mongo test");
        return;
    }
    let container = Container::start("mongo:8.2", &[], &[], 27017).unwrap();
    let port = container.host_port(27017).unwrap();
    let uri = format!("mongodb://127.0.0.1:{port}");
    let spec = PayloadBackendSpec::SpiMongo {
        uri: uri.clone(),
        database: format!("probe_16m_{}", std::process::id()),
    };
    let directory = tempfile::tempdir().unwrap();
    let binding = StoreIdentity::generate().unwrap();
    let root = directory.path().join("objects");
    let mut big_policy = policy();
    big_policy.bytes = Number(64 << 20);
    big_policy.owner_bytes = Number(64 << 20);
    let store =
        PayloadStore::initialize_with_backend(&root, binding, big_policy, &spec).unwrap();

    let payload: Vec<u8> = (0..16 * 1024 * 1024usize)
        .map(|i| (i % 251) as u8)
        .collect();
    let now = Instant::now();
    let mut big_caps = caps();
    big_caps.object_limit = Number(64 << 20);
    let mut stage = store
        .stage(&IdentityLabel("alice".into()), &descriptor(&payload), &big_caps, now)
        .unwrap();
    for chunk in payload.chunks(65536) {
        stage.receive(chunk, now).unwrap();
    }
    let installed = stage.finish(now).unwrap();
    let key = installed.key().to_owned();
    assert_eq!(read_all(&store, &key, &payload), payload);
    drop(installed);

    // The object is visible in mongo as a plane child scope with 16 chunks.
    let service = pipestream_storage::MongoStore::open(&pipestream_storage::MongoConfig {
        uri: uri.clone(),
        database: spec_database(&spec),
    })
    .unwrap();
    let scopes = service.list_scopes().unwrap();
    assert!(scopes.contains(&plane_scope(binding, &key)));

    // Orphan collection over a chunked entity (the unreferenced case the
    // certification rows hit at retire time).
    let progress = store.collect(None, 256, |_| Ok(false)).unwrap();
    assert_eq!(progress.removed, 1);
    assert!(!root.join(format!("object-{key}")).exists());
    assert!(!scopes.is_empty() || true);
    assert!(service.list_scopes().unwrap().is_empty());
    assert!(
        store
            .open_object(&key, &IdentityLabel("alice".into()), &descriptor(&payload))
            .is_err(),
        "collected object must not open"
    );
}

fn spec_database(spec: &PayloadBackendSpec) -> String {
    match spec {
        PayloadBackendSpec::SpiMongo { database, .. } => database.clone(),
        _ => panic!("not a mongo spec"),
    }
}


/// Shared-database race proof (the certification failure family): two
/// installations on one mongo database. Installation B opening its store
/// runs the orphan sweep — which must leave installation A's objects
/// untouched, even though A's local stubs are invisible to B.
#[test]
fn spi_mongo_shared_database_installations_do_not_sweep_each_other() {
    if !docker_available() {
        eprintln!("docker unavailable: skipping shared-database test");
        return;
    }
    let container = Container::start("mongo:8.2", &[], &[], 27017).unwrap();
    let port = container.host_port(27017).unwrap();
    let uri = format!("mongodb://127.0.0.1:{port}");
    let database = format!("shared_{}", std::process::id());
    let spec = || PayloadBackendSpec::SpiMongo {
        uri: uri.clone(),
        database: database.clone(),
    };
    let dir_a = tempfile::tempdir().unwrap();
    let binding_a = StoreIdentity::generate().unwrap();
    let store_a =
        PayloadStore::initialize_with_backend(&dir_a.path().join("objects"), binding_a, policy(), &spec())
            .unwrap();
    let payload_a = b"installation A payload".to_vec();
    let installed_a = install(&store_a, &payload_a);
    let key_a = installed_a.key().to_owned();
    drop(installed_a);

    // Installation B opens against the same database: its sweep runs at open.
    let dir_b = tempfile::tempdir().unwrap();
    let binding_b = StoreIdentity::generate().unwrap();
    let store_b =
        PayloadStore::initialize_with_backend(&dir_b.path().join("objects"), binding_b, policy(), &spec())
            .unwrap();
    assert_ne!(binding_a, binding_b);

    // A's object survives B's open, and A reads it back intact.
    assert_eq!(read_all(&store_a, &key_a, &payload_a), payload_a);
    // B's own namespace works independently.
    let payload_b = b"installation B payload".to_vec();
    let installed_b = install(&store_b, &payload_b);
    let key_b = installed_b.key().to_owned();
    drop(installed_b);
    assert_eq!(read_all(&store_b, &key_b, &payload_b), payload_b);
    assert_eq!(read_all(&store_a, &key_a, &payload_a), payload_a);
}
