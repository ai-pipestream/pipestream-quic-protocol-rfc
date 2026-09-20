//! Cross-language storage layout probe CLI (test utility; not part of the
//! shipped SPI surface).
//!
//! Usage: `pipestream-storage-probe <backend> <script>` executes a script;
//! `pipestream-storage-probe fs --report` renders the canonical report of
//! an existing filesystem store without executing operations
//! (crossed-store verification).
//!
//! Backends: `memory`, `fs`, `postgres`, `mongo` (both roles), `s3`
//! (object role only), `kafka` (journal role only). Backend configuration
//! comes from the environment, matching the Java lane's variable names:
//!
//! - `fs`: `PROBE_FS_ROOT` (required)
//! - `postgres`: `PROBE_PG_URL` (required; a `postgres://` URL)
//! - `s3`: `PROBE_S3_ENDPOINT`, `PROBE_S3_BUCKET`, `PROBE_S3_ACCESS_KEY`,
//!   `PROBE_S3_SECRET_KEY` (required); `PROBE_S3_REGION` (default
//!   `us-east-1`), `PROBE_S3_PREFIX` (default `probe/`)
//! - `mongo`: `PROBE_MONGO_URI`, `PROBE_MONGO_DATABASE` (required)
//! - `kafka`: `PROBE_KAFKA_BOOTSTRAP`, `PROBE_KAFKA_TOPIC` (required)
//!
//! Exit codes: 0 success, 1 script/storage failure, 2 usage error. The
//! canonical report goes to stdout; byte-identical to the Java probe on the
//! same script and backend kind.

use pipestream_storage::probe::{self, RoleSelection};
use pipestream_storage::{
    FsStore, KafkaConfig, KafkaJournal, MemoryStore, MongoConfig, MongoStore, PostgresStore,
    S3Config, S3Store,
};
use std::process::ExitCode;

fn required(name: &str) -> Result<String, String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        _ => Err(format!("missing configuration {name}")),
    }
}

fn usage() {
    eprintln!("usage: pipestream-storage-probe <backend> <script>");
    eprintln!("       pipestream-storage-probe fs --report   (report-only, existing store)");
    eprintln!(
        "backends: memory fs postgres mongo (object store + journal), s3 (object only), kafka (journal only)"
    );
    eprintln!(
        "configuration: PROBE_FS_ROOT | PROBE_PG_URL | PROBE_S3_ENDPOINT/BUCKET/ACCESS_KEY/\
         SECRET_KEY[/REGION/PREFIX] | PROBE_MONGO_URI/DATABASE | PROBE_KAFKA_BOOTSTRAP/TOPIC"
    );
}

fn run() -> Result<String, String> {
    let args: Vec<String> = std::env::args().collect();
    // Report-only mode: `probe <backend> --report` renders the canonical
    // report of an existing store without executing any operations
    // (crossed-store verification). Only the filesystem layout is
    // enumerable this way.
    if args.len() == 3 && args[2] == "--report" {
        if args[1] != "fs" {
            usage();
            return Err("usage".to_string());
        }
        let root = required("PROBE_FS_ROOT")?;
        let scopes = probe::discover_fs_scopes(std::path::Path::new(&root)).map_err(|e| {
            eprintln!("probe: {e}");
            "usage".to_string()
        })?;
        let store = FsStore::open(&root).map_err(|e| e.to_string())?;
        return probe::report(&store, RoleSelection::BOTH, scopes).map_err(|e| e.to_string());
    }
    if args.len() != 3 {
        usage();
        return Err("usage".to_string());
    }
    let script = std::fs::read_to_string(&args[2])
        .map_err(|e| format!("read script {}: {e}", args[2]))?;
    match args[1].as_str() {
        "memory" => {
            let mut store = MemoryStore::new();
            probe::run(&mut store, RoleSelection::BOTH, &script).map_err(|e| e.to_string())
        }
        "fs" => {
            let root = required("PROBE_FS_ROOT")?;
            let mut store = FsStore::open(&root).map_err(|e| e.to_string())?;
            probe::run(&mut store, RoleSelection::BOTH, &script).map_err(|e| e.to_string())
        }
        "postgres" => {
            let url = required("PROBE_PG_URL")?;
            let mut store = PostgresStore::open(&url).map_err(|e| e.to_string())?;
            probe::run(&mut store, RoleSelection::BOTH, &script).map_err(|e| e.to_string())
        }
        "s3" => {
            let config = S3Config {
                bucket: required("PROBE_S3_BUCKET")?,
                prefix: std::env::var("PROBE_S3_PREFIX").unwrap_or_else(|_| "probe/".to_string()),
                endpoint_url: Some(required("PROBE_S3_ENDPOINT")?),
                region: std::env::var("PROBE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
                access_key: required("PROBE_S3_ACCESS_KEY")?,
                secret_key: required("PROBE_S3_SECRET_KEY")?,
            };
            let mut store = probe::StoreOnly(S3Store::open(&config).map_err(|e| e.to_string())?);
            probe::run(&mut store, RoleSelection::STORE_ONLY, &script).map_err(|e| e.to_string())
        }
        "mongo" => {
            let config = MongoConfig {
                uri: required("PROBE_MONGO_URI")?,
                database: required("PROBE_MONGO_DATABASE")?,
            };
            let mut store = MongoStore::open(&config).map_err(|e| e.to_string())?;
            probe::run(&mut store, RoleSelection::BOTH, &script).map_err(|e| e.to_string())
        }
        "kafka" => {
            let config = KafkaConfig {
                bootstrap_servers: required("PROBE_KAFKA_BOOTSTRAP")?,
                topic: required("PROBE_KAFKA_TOPIC")?,
            };
            let mut journal =
                probe::JournalOnly(KafkaJournal::open(&config).map_err(|e| e.to_string())?);
            probe::run(&mut journal, RoleSelection::JOURNAL_ONLY, &script).map_err(|e| e.to_string())
        }
        other => {
            eprintln!("probe: unknown backend: {other}");
            usage();
            Err("usage".to_string())
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(report) => {
            print!("{report}");
            ExitCode::SUCCESS
        }
        Err(message) if message == "usage" => ExitCode::from(2),
        Err(message) => {
            eprintln!("probe: {message}");
            ExitCode::FAILURE
        }
    }
}
