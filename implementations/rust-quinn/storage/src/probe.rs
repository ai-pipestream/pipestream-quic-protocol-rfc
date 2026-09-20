//! Cross-language layout probe (test utility, not shipped SPI surface).
//!
//! Executes the language-neutral script format v1
//! (`test-vectors/storage-spi/README.md`) against one backend and renders
//! the canonical report. The Rust and Java probes must print byte-identical
//! reports for the same script and backend kind, and the pinned
//! `expected-report-v1.txt` is asserted verbatim in tests.
//!
//! Script format — one operation per line; `#` starts a comment; blank
//! lines are ignored:
//!
//! ```text
//! put <scope-path> <entity-id-dec> <payload-hex>
//! seal <scope-path> <entity-count-dec>   # digest = canonical content digest now
//! append <intent-id> <payload-hex>
//! commit <intent-id> <payload-hex>
//! retire <scope-path> <cursor-dec>
//! ```
//!
//! Report: scope sections in ascending rendered-path order (entities
//! ascending, per-object SHA-256, canonical content digest, seal record when
//! present), then a `journal` section listing intents and commits flat in
//! ascending cursor order. Single-role backends skip the other role's
//! operations and omit its section, per the README's slicing rule.

use crate::layout;
use crate::{Cursor, Intent, Journal, ObjectStore, ScopePath, StorageError, sha256};
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::Path;

/// Which SPI roles the probed backend implements; the other role's
/// operations are skipped and its report section omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleSelection {
    /// The backend implements [`ObjectStore`].
    pub object_store: bool,
    /// The backend implements [`Journal`].
    pub journal: bool,
}

impl RoleSelection {
    /// Both roles (memory, filesystem, Postgres, MongoDB).
    pub const BOTH: RoleSelection = RoleSelection {
        object_store: true,
        journal: true,
    };
    /// Object role only (S3).
    pub const STORE_ONLY: RoleSelection = RoleSelection {
        object_store: true,
        journal: false,
    };
    /// Journal role only (Kafka).
    pub const JOURNAL_ONLY: RoleSelection = RoleSelection {
        object_store: false,
        journal: true,
    };
}

/// Executes `script` against `backend` and returns the canonical report.
pub fn run<B: ObjectStore + Journal>(
    backend: &mut B,
    roles: RoleSelection,
    script: &str,
) -> Result<String, StorageError> {
    let mut touched: BTreeMap<String, ScopePath> = BTreeMap::new();
    for (index, raw) in script.lines().enumerate() {
        let line = match raw.find('#') {
            Some(at) => &raw[..at],
            None => raw,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        apply(backend, roles, &mut touched, &tokens)
            .map_err(|error| StorageError::failed(format!("script line {}: {error}", index + 1)))?;
    }
    render(backend, roles, &touched.into_values().collect::<Vec<_>>())
}

fn apply<B: ObjectStore + Journal>(
    backend: &mut B,
    roles: RoleSelection,
    touched: &mut BTreeMap<String, ScopePath>,
    tokens: &[&str],
) -> Result<(), StorageError> {
    match tokens.first() {
        Some(&"put") => {
            require(tokens, 4, "put <scope> <entity-id-dec> <payload-hex>")?;
            if !roles.object_store {
                return Ok(());
            }
            let scope = parse_scope(tokens[1])?;
            let entity = parse_entity(tokens[2])?;
            let payload = parse_hex(tokens[3])?;
            backend.put(&scope, entity, &payload, &sha256(&payload))?;
            touched.insert(scope.to_path_string(), scope);
            Ok(())
        }
        Some(&"seal") => {
            require(tokens, 3, "seal <scope> <entity-count-dec>")?;
            if !roles.object_store {
                return Ok(());
            }
            let scope = parse_scope(tokens[1])?;
            let count = parse_u64(tokens[2], "entity count")?;
            let digest = content_digest(backend, &scope)?;
            backend.seal_scope(&scope, &digest, count)?;
            touched.insert(scope.to_path_string(), scope);
            Ok(())
        }
        Some(&"append") => {
            require(tokens, 3, "append <intent-id> <payload-hex>")?;
            if !roles.journal {
                return Ok(());
            }
            let payload = parse_hex(tokens[2])?;
            let intent = Intent::new(tokens[1], payload)
                .map_err(|error| StorageError::failed(format!("invalid intent: {error}")))?;
            backend.append_intent(&intent)?;
            Ok(())
        }
        Some(&"commit") => {
            require(tokens, 3, "commit <intent-id> <payload-hex>")?;
            if !roles.journal {
                return Ok(());
            }
            let payload = parse_hex(tokens[2])?;
            backend.commit(tokens[1], &payload)?;
            Ok(())
        }
        Some(&"retire") => {
            require(tokens, 3, "retire <scope> <cursor-dec>")?;
            if !roles.object_store {
                return Ok(());
            }
            let scope = parse_scope(tokens[1])?;
            let cursor = parse_u64(tokens[2], "retention cursor")?;
            backend.retire(&scope, cursor)?;
            touched.insert(scope.to_path_string(), scope);
            Ok(())
        }
        Some(other) => Err(StorageError::failed(format!("unknown operation: {other}"))),
        None => Ok(()),
    }
}

fn require(tokens: &[&str], count: usize, syntax: &str) -> Result<(), StorageError> {
    if tokens.len() != count {
        return Err(StorageError::failed(format!(
            "expected '{syntax}' but found {} fields",
            tokens.len()
        )));
    }
    Ok(())
}

fn parse_scope(rendered: &str) -> Result<ScopePath, StorageError> {
    ScopePath::parse(rendered)
}

fn parse_entity(decimal: &str) -> Result<u32, StorageError> {
    decimal
        .parse::<u32>()
        .map_err(|_| StorageError::failed(format!("malformed entity id {decimal:?}")))
}

fn parse_u64(decimal: &str, what: &str) -> Result<u64, StorageError> {
    decimal
        .parse::<u64>()
        .map_err(|_| StorageError::failed(format!("malformed {what} {decimal:?}")))
}

fn parse_hex(hex: &str) -> Result<Vec<u8>, StorageError> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(StorageError::failed(format!("malformed payload hex {hex:?}")));
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| {
            u8::from_str_radix(&hex[at..at + 2], 16)
                .map_err(|_| StorageError::failed(format!("malformed payload hex {hex:?}")))
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Reads one entity's payload back from the store.
fn read_payload<B: ObjectStore + Journal>(
    backend: &B,
    scope: &ScopePath,
    entity: u32,
) -> Result<Vec<u8>, StorageError> {
    let mut reader = backend.open_read(scope, entity)?;
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| StorageError::transient(format!("probe read failed: {error}")))?;
    Ok(bytes)
}

/// Canonical content digest of one scope's live entities (pinned decision
/// 3), computed from bytes read back out of the store.
fn content_digest<B: ObjectStore + Journal>(
    backend: &B,
    scope: &ScopePath,
) -> Result<layout::Digest32, StorageError> {
    let mut digests = Vec::new();
    for entity in backend.list(scope)? {
        digests.push((entity, sha256(&read_payload(backend, scope, entity)?)));
    }
    layout::scope_digest(&digests)
}

fn render<B: ObjectStore + Journal>(
    backend: &B,
    roles: RoleSelection,
    scopes: &[ScopePath],
) -> Result<String, StorageError> {
    let mut out = String::new();
    if roles.object_store {
        for scope in scopes {
            out.push_str(&format!("scope {}\n", scope.to_path_string()));
            let mut digests = Vec::new();
            for entity in backend.list(scope)? {
                let payload = read_payload(backend, scope, entity)?;
                let digest = sha256(&payload);
                digests.push((entity, digest));
                out.push_str(&format!(
                    "  entity {} sha256={}\n",
                    layout::entity_component(entity),
                    hex(&digest)
                ));
            }
            out.push_str(&format!(
                "  content-digest {}\n",
                hex(&layout::scope_digest(&digests)?)
            ));
            match backend.verify_scope(scope) {
                Ok(seal) => {
                    out.push_str(&format!(
                        "  sealed digest={} count={}\n",
                        hex(&seal.digest),
                        seal.entity_count
                    ));
                }
                Err(StorageError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
    }
    if roles.journal {
        out.push_str("journal\n");
        // Intent lines at their intent cursor, commit lines at the cursor
        // the commit consumed; flat, ascending cursor order.
        let mut records: Vec<(u64, &str, String, String)> = Vec::new();
        for entry in backend.replay(Cursor(0))? {
            records.push((
                entry.cursor.0,
                "intent",
                entry.intent.id().to_string(),
                hex(&sha256(entry.intent.record())),
            ));
            if let (Some(commit), Some(commit_cursor)) = (&entry.commit, entry.commit_cursor) {
                records.push((
                    commit_cursor.0,
                    "commit",
                    entry.intent.id().to_string(),
                    hex(&sha256(commit)),
                ));
            }
        }
        records.sort_by_key(|(cursor, _, _, _)| *cursor);
        for (cursor, kind, id, digest) in records {
            out.push_str(&format!("  {cursor} {kind} {id} sha256={digest}\n"));
        }
    }
    Ok(out)
}

/// Adapter giving a store-only backend (S3) a never-called journal role so
/// it can run through [`run`] with [`RoleSelection::STORE_ONLY`].
pub struct StoreOnly<S: ObjectStore>(pub S);

impl<S: ObjectStore> ObjectStore for StoreOnly<S> {
    type Reader = S::Reader;
    type PutWriter = S::PutWriter;

    fn begin_put(
        &mut self,
        scope: &ScopePath,
        entity: crate::EntityId,
        expected_digest: &layout::Digest32,
    ) -> Result<Self::PutWriter, StorageError> {
        self.0.begin_put(scope, entity, expected_digest)
    }

    fn put(
        &mut self,
        scope: &ScopePath,
        entity: crate::EntityId,
        bytes: &[u8],
        expected_digest: &layout::Digest32,
    ) -> Result<(), StorageError> {
        self.0.put(scope, entity, bytes, expected_digest)
    }

    fn open_read(
        &self,
        scope: &ScopePath,
        entity: crate::EntityId,
    ) -> Result<Self::Reader, StorageError> {
        self.0.open_read(scope, entity)
    }

    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &layout::Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError> {
        self.0.seal_scope(scope, digest, entity_count)
    }

    fn verify_scope(&self, scope: &ScopePath) -> Result<crate::SealRecord, StorageError> {
        self.0.verify_scope(scope)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<crate::EntityId>, StorageError> {
        self.0.list(scope)
    }

    fn list_scopes(&self) -> Result<Vec<ScopePath>, StorageError> {
        self.0.list_scopes()
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        self.0.retire(scope, retention_cursor)
    }
}

impl<S: ObjectStore> Journal for StoreOnly<S> {
    fn append_intent(&mut self, _intent: &Intent) -> Result<Cursor, StorageError> {
        Err(StorageError::failed("journal role absent"))
    }

    fn commit(&mut self, _intent_id: &str, _record: &[u8]) -> Result<(), StorageError> {
        Err(StorageError::failed("journal role absent"))
    }

    fn replay(&self, _since: Cursor) -> Result<Vec<crate::ReplayEntry>, StorageError> {
        Err(StorageError::failed("journal role absent"))
    }
}

/// Adapter giving a journal-only backend (Kafka) a never-called store role
/// so it can run through [`run`] with [`RoleSelection::JOURNAL_ONLY`].
pub struct JournalOnly<J: Journal>(pub J);

impl<J: Journal> Journal for JournalOnly<J> {
    fn append_intent(&mut self, intent: &Intent) -> Result<Cursor, StorageError> {
        self.0.append_intent(intent)
    }

    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError> {
        self.0.commit(intent_id, record)
    }

    fn replay(&self, since: Cursor) -> Result<Vec<crate::ReplayEntry>, StorageError> {
        self.0.replay(since)
    }
}

impl<J: Journal> ObjectStore for JournalOnly<J> {
    type Reader = io::Cursor<Vec<u8>>;
    type PutWriter = crate::BufferedPutWriter<
        Box<dyn FnOnce(&[u8]) -> Result<(), StorageError> + Send>,
    >;

    fn begin_put(
        &mut self,
        _scope: &ScopePath,
        _entity: crate::EntityId,
        _expected_digest: &layout::Digest32,
    ) -> Result<Self::PutWriter, StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }

    fn put(
        &mut self,
        _scope: &ScopePath,
        _entity: crate::EntityId,
        _bytes: &[u8],
        _expected_digest: &layout::Digest32,
    ) -> Result<(), StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }

    fn open_read(
        &self,
        _scope: &ScopePath,
        _entity: crate::EntityId,
    ) -> Result<Self::Reader, StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }

    fn seal_scope(
        &mut self,
        _scope: &ScopePath,
        _digest: &layout::Digest32,
        _entity_count: u64,
    ) -> Result<(), StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }

    fn verify_scope(&self, _scope: &ScopePath) -> Result<crate::SealRecord, StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }

    fn list(&self, _scope: &ScopePath) -> Result<Vec<crate::EntityId>, StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }

    fn list_scopes(&self) -> Result<Vec<ScopePath>, StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }

    fn retire(&mut self, _scope: &ScopePath, _retention_cursor: u64) -> Result<u64, StorageError> {
        Err(StorageError::failed("object-store role absent"))
    }
}

/// Report-only mode: enumerates the scope tree of a filesystem store by
/// walking the directory hierarchy under `<root>/objects`, without running
/// any operations. Used for crossed-store verification: a store written by
/// either lane renders the canonical report when opened by the other.
///
/// Every 8-hex directory is a scope path component (2-hex directories are
/// entity buckets of the enclosing scope and are not descended into).
/// Fails with [`StorageError::Failed`] when the root holds no initialized
/// store.
pub fn discover_fs_scopes(root: &Path) -> Result<Vec<ScopePath>, StorageError> {
    let objects = root.join("objects");
    if !objects.is_dir() {
        return Err(StorageError::failed(format!(
            "{} does not hold an initialized store",
            root.display()
        )));
    }
    let mut scopes = Vec::new();
    walk_scope_dir(&objects, &[], &mut scopes)?;
    scopes.sort_by_key(|scope| scope.to_path_string());
    Ok(scopes)
}

fn walk_scope_dir(
    dir: &Path,
    components: &[u32],
    scopes: &mut Vec<ScopePath>,
) -> Result<(), StorageError> {
    if !components.is_empty() {
        scopes.push(ScopePath::from_components(components.to_vec())?);
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .collect::<Result<_, _>>()
        .map_err(StorageError::from)?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.len() != 8 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue; // entity bucket or unrelated directory
        }
        let component = u32::from_str_radix(name, 16)
            .map_err(|_| StorageError::corrupt(format!("malformed scope dir {name}")))?;
        let mut child = components.to_vec();
        child.push(component);
        walk_scope_dir(&path, &child, scopes)?;
    }
    Ok(())
}

/// Renders the canonical report for an existing store with no script —
/// the report-only mode. `scopes` are the discovered scope paths (any
/// order; they are sorted here), entities and the journal are read back
/// from the backend.
pub fn report<B: ObjectStore + Journal>(
    backend: &B,
    roles: RoleSelection,
    scopes: Vec<ScopePath>,
) -> Result<String, StorageError> {
    let mut scopes = scopes;
    scopes.sort_by_key(|scope| scope.to_path_string());
    render(backend, roles, &scopes)
}
