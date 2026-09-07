use super::*;

const APPLICATION: i64 = 0x5053434a;
const FORMAT: i64 = 3;
const BLOB_LIMIT: usize = IMAGE_LIMIT + 33;
const SCHEMA: &str = "
PRAGMA application_id=0x5053434a;
PRAGMA user_version=3;
CREATE TABLE journal(singleton INTEGER PRIMARY KEY CHECK(singleton=1), configuration BLOB NOT NULL, binding BLOB) STRICT;
CREATE TABLE operations(row_id INTEGER PRIMARY KEY, operation BLOB NOT NULL UNIQUE CHECK(length(operation)=16), intent BLOB NOT NULL, receipt BLOB,
kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 5), scope INTEGER NOT NULL, producer INTEGER NOT NULL, entity INTEGER) STRICT;
CREATE INDEX operation_target ON operations(kind,scope,producer,entity);
CREATE TABLE observations(row_id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, producer INTEGER NOT NULL, entity INTEGER NOT NULL, image BLOB NOT NULL,
UNIQUE(scope,producer,entity)) STRICT;
CREATE TABLE manifests(row_id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, producer INTEGER NOT NULL, entity INTEGER NOT NULL, image BLOB NOT NULL,
UNIQUE(scope,producer,entity)) STRICT;
CREATE TABLE result_references(row_id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, producer INTEGER NOT NULL, entity INTEGER NOT NULL,
output_index INTEGER NOT NULL CHECK(output_index BETWEEN 0 AND 255), image BLOB NOT NULL,
UNIQUE(scope,producer,entity,output_index)) STRICT;
CREATE TABLE scopes(row_id INTEGER PRIMARY KEY, scope INTEGER NOT NULL UNIQUE, producer INTEGER NOT NULL,
parent_scope INTEGER, parent_producer INTEGER, parent_entity INTEGER, image BLOB NOT NULL,
UNIQUE(parent_scope,parent_producer,parent_entity)) STRICT;
CREATE TABLE scope_members(row_id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, entity INTEGER NOT NULL, image BLOB NOT NULL,
UNIQUE(scope,entity)) STRICT;
CREATE TABLE scope_coverage(row_id INTEGER PRIMARY KEY, scope INTEGER NOT NULL UNIQUE, image BLOB NOT NULL) STRICT;
";

#[derive(Debug)]
pub enum JournalError {
    Protocol(Error),
    Database(rusqlite::Error),
    Physical(crate::persistence::StoreError),
    Io(std::io::Error),
    Corrupt(&'static str),
}
impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(e) => e.fmt(f),
            Self::Database(e) => write!(f, "client journal database: {e}"),
            Self::Physical(e) => e.fmt(f),
            Self::Io(e) => e.fmt(f),
            Self::Corrupt(s) => write!(f, "client journal invalid: {s}"),
        }
    }
}
impl std::error::Error for JournalError {}
impl From<Error> for JournalError {
    fn from(e: Error) -> Self {
        Self::Protocol(e)
    }
}
impl From<std::io::Error> for JournalError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<crate::persistence::StoreError> for JournalError {
    fn from(e: crate::persistence::StoreError) -> Self {
        Self::Physical(e)
    }
}
impl From<rusqlite::Error> for JournalError {
    fn from(e: rusqlite::Error) -> Self {
        if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            Self::Protocol(Error::new(
                ErrorCode::LimitExceeded,
                "client journal physical capacity exhausted",
            ))
        } else {
            Self::Database(e)
        }
    }
}

impl Journal {
    fn configuration(&self) -> Result<Vec<u8>> {
        self.creation.check()?;
        self.limits.check()?;
        let mut w = codec::Writer::new();
        w.array(2);
        self.creation.write(&mut w);
        self.limits.write(&mut w);
        Ok(seal(b"configuration", &w.finish()))
    }
    pub(super) fn open_inner(
        path: &Path,
        creation: Creation,
        limits: JournalLimits,
        physical: PhysicalLimits,
        initialize: bool,
    ) -> Result<Self> {
        creation.check()?;
        limits.check()?;
        if !initialize {
            let metadata = std::fs::symlink_metadata(path)?;
            if !metadata.is_file() || metadata.len() == 0 {
                return Err(JournalError::Corrupt("existing journal missing or empty"));
            }
        }
        let journal = Self {
            physical: PhysicalGuard::open(path, Some(physical))?,
            creation,
            limits,
        };
        if initialize {
            let mut file = std::fs::OpenOptions::new();
            file.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                file.mode(0o600);
            }
            file.open(&journal.physical.path)?.sync_all()?;
        }
        let mut connection = journal.raw_connect(initialize)?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if initialize {
            tx.execute_batch(SCHEMA)?;
            tx.execute(
                "INSERT INTO journal(singleton,configuration,binding) VALUES(1,?1,NULL)",
                [journal.configuration()?],
            )?;
        }
        journal.verify_configuration(&tx)?;
        let binding = journal.read_binding(&tx)?;
        let count: i64 = tx.query_row("SELECT count(*) FROM operations", [], |r| r.get(0))?;
        if count < 0
            || count as u64 > journal.limits.operations.0
            || (count != 0 && binding.is_none())
        {
            return Err(JournalError::Corrupt("journal count or binding missing"));
        }
        let mut statement = tx.prepare("SELECT rowid,operation FROM operations ORDER BY rowid")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            if id <= 0 {
                return Err(JournalError::Corrupt("invalid operation cursor"));
            }
            let operation = read_operation(row, 1)?;
            let intent = journal.read_intent(&tx, operation)?;
            if let Some(image) = read(&tx, "operations", "receipt", id)? {
                let receipt: OperationReceipt =
                    codec::decode(unseal(&operation.0, &image)?, MAX_HEADER)?;
                validate_receipt(&intent, &journal.read_identity(&tx)?, &receipt)?;
                journal.validate_known_receipt(&tx, &intent, &receipt)?;
            }
        }
        drop(rows);
        drop(statement);
        journal.audit_observations(&tx)?;
        tx.commit()?;
        drop(connection);
        crate::persistence::sync_directory(
            journal
                .physical
                .path
                .parent()
                .ok_or(JournalError::Corrupt("journal parent absent"))?,
        )?;
        Ok(journal)
    }
    fn raw_connect(&self, initialize: bool) -> Result<Connection> {
        self.physical.verify()?;
        let connection = Connection::open_with_flags_and_vfs(
            &self.physical.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            GUARDED_VFS,
        )?;
        connection.busy_timeout(Elapsed::from_secs(5))?;
        connection.execute_batch("PRAGMA trusted_schema=OFF;")?;
        if !initialize {
            // Refusal must precede persistent journal-mode changes to a file
            // belonging to another format, owner or creation policy.
            self.verify_configuration(&connection)?;
        }
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA trusted_schema=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-2048;")?;
        Ok(connection)
    }
    pub(super) fn connect(&self) -> Result<Connection> {
        self.raw_connect(false)
    }
    fn verify_configuration(&self, connection: &Connection) -> Result<()> {
        let application: i64 = connection.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        let format: i64 = connection.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if application != APPLICATION
            || format != FORMAT
            || read(connection, "journal", "configuration", 1)? != Some(self.configuration()?)
        {
            return Err(JournalError::Corrupt(
                "journal format, identity or policy changed",
            ));
        }
        Ok(())
    }
}

pub(super) fn binding(control: &Control) -> Result<Control> {
    control.encode(IMAGE_LIMIT)?;
    let Control::Session(Session::Binding {
        authority,
        owner,
        generation,
        creation_sequence,
        policy,
        limits,
        ..
    }) = control
    else {
        return Err(Error::frame("expected session binding").into());
    };
    Ok(Control::Session(Session::Binding {
        request: Id(1),
        authority: authority.clone(),
        owner: owner.clone(),
        generation: *generation,
        creation_sequence: *creation_sequence,
        policy: policy.clone(),
        limits: limits.clone(),
    }))
}

pub(super) fn read_operation(row: &rusqlite::Row<'_>, column: usize) -> Result<OperationId> {
    let bytes = row
        .get_ref(column)?
        .as_blob()
        .map_err(|_| JournalError::Corrupt("operation ID is not bytes"))?;
    let value = OperationId(
        bytes
            .try_into()
            .map_err(|_| JournalError::Corrupt("operation ID length changed"))?,
    );
    value.check()?;
    Ok(value)
}
pub(super) fn operation_row(
    connection: &Connection,
    operation: OperationId,
) -> Result<Option<i64>> {
    operation.check()?;
    Ok(connection
        .query_row(
            "SELECT rowid FROM operations WHERE operation=?1",
            [operation.0.as_slice()],
            |r| r.get(0),
        )
        .optional()?)
}

// Names are private static call-site constants, never caller-controlled SQL.
pub(super) fn read(
    connection: &Connection,
    table: &str,
    column: &str,
    row: i64,
) -> Result<Option<Vec<u8>>> {
    let length: Option<i64> = connection.query_row(
        &format!("SELECT length({column}) FROM {table} WHERE rowid=?1"),
        [row],
        |r| r.get(0),
    )?;
    let Some(length) = length else {
        return Ok(None);
    };
    let limit = blob_limit(table);
    if !(33..=limit as i64).contains(&length) {
        return Err(JournalError::Corrupt("journal image length invalid"));
    }
    let blob = connection.blob_open("main", table, column, row, true)?;
    if blob.len() != length as usize {
        return Err(JournalError::Corrupt("journal image changed while reading"));
    }
    let mut bytes = vec![0; length as usize];
    blob.read_at_exact(&mut bytes, 0)?;
    Ok(Some(bytes))
}
pub(super) fn write(
    connection: &Connection,
    table: &str,
    column: &str,
    row: i64,
    bytes: &[u8],
) -> Result<()> {
    if !(33..=blob_limit(table)).contains(&bytes.len()) {
        return Err(JournalError::Corrupt("journal image exceeds record bound"));
    }
    if connection.execute(
        &format!("UPDATE {table} SET {column}=?1 WHERE rowid=?2"),
        params![bytes, row],
    )? != 1
    {
        return Err(JournalError::Corrupt("journal record missing"));
    }
    Ok(())
}
fn blob_limit(table: &str) -> usize {
    if matches!(table, "observations" | "manifests") {
        MAX_CONTROL_LIMIT + 5 + 32
    } else {
        BLOB_LIMIT
    }
}
pub(super) fn seal(namespace: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(b"pipestream-client-journal-v1");
    h.update((namespace.len() as u64).to_be_bytes());
    h.update(namespace);
    h.update(bytes);
    let mut image = Vec::with_capacity(bytes.len() + 32);
    image.extend_from_slice(bytes);
    image.extend_from_slice(&h.finalize());
    image
}
pub(super) fn unseal<'a>(namespace: &[u8], image: &'a [u8]) -> Result<&'a [u8]> {
    if image.len() < 33 || seal(namespace, &image[..image.len() - 32]) != image {
        return Err(JournalError::Corrupt("journal image checksum changed"));
    }
    Ok(&image[..image.len() - 32])
}

pub(super) fn intent_image(intent: &Intent, identity: &SessionIdentity) -> Result<Vec<u8>> {
    intent
        .mutation
        .digest(identity, Producer(0), intent.operation)?;
    let mut bytes = match &intent.mutation {
        Mutation::Admit(_) => {
            let mut b = vec![0];
            b.extend(intent.input(identity.generation)?.encode()?);
            b
        }
        _ => {
            let mut b = vec![1];
            b.extend(intent.control(Id(1))?.encode(MAX_HEADER)?);
            b
        }
    };
    // Identity-bound checksum catches accidentally transplanted intent records.
    bytes.extend_from_slice(
        &intent
            .mutation
            .digest(identity, Producer(0), intent.operation)?
            .0,
    );
    if bytes.len() > IMAGE_LIMIT {
        return Err(Error::new(
            ErrorCode::LimitExceeded,
            "client intent exceeds record bound",
        )
        .into());
    }
    Ok(seal(&intent.operation.0, &bytes))
}
pub(super) fn decode_intent(
    image: &[u8],
    operation: OperationId,
    identity: &SessionIdentity,
) -> Result<Intent> {
    let bytes = unseal(&operation.0, image)?;
    if bytes.len() <= 33 {
        return Err(JournalError::Corrupt("truncated client intent"));
    }
    let body = &bytes[1..bytes.len() - 32];
    let (actual, mutation) = match bytes[0] {
        0 => {
            let header = InputHeader::decode(body)?;
            if header.generation != identity.generation {
                return Err(JournalError::Corrupt("input journal generation changed"));
            }
            (header.operation, Mutation::Admit(header.parameters))
        }
        1 => {
            let control = Control::decode(body, MAX_HEADER)?;
            if request_id(&control) != Some(Id(1)) {
                return Err(JournalError::Corrupt("noncanonical intent request number"));
            }
            match control {
                Control::Scope(Scope::Declare {
                    operation,
                    scope,
                    entity_ids,
                    seal,
                    ..
                }) => (
                    operation,
                    Mutation::Declare {
                        scope,
                        entity_ids,
                        seal,
                    },
                ),
                Control::Scope(Scope::Cancel {
                    operation, scope, ..
                }) => (operation, Mutation::ScopeCancel { scope }),
                Control::Work(Work::Retry {
                    operation,
                    work,
                    expected_attempt,
                    ..
                }) => (
                    operation,
                    Mutation::Retry {
                        work,
                        expected_attempt,
                    },
                ),
                Control::Work(Work::Cancel {
                    operation, work, ..
                }) => (operation, Mutation::Cancel { work }),
                Control::Work(Work::Skip {
                    operation, work, ..
                }) => (operation, Mutation::Skip { work }),
                _ => return Err(JournalError::Corrupt("stored request is not a mutation")),
            }
        }
        _ => return Err(JournalError::Corrupt("unknown intent encoding")),
    };
    let intent = Intent {
        operation,
        mutation,
    };
    if actual != operation || intent_image(&intent, identity)? != image {
        return Err(JournalError::Corrupt(
            "intent identity or commitment changed",
        ));
    }
    Ok(intent)
}

pub(super) fn validate_receipt(
    intent: &Intent,
    identity: &SessionIdentity,
    receipt: &OperationReceipt,
) -> Result<()> {
    receipt.check()?;
    if receipt.operation != intent.operation
        || receipt.request_digest
            != intent
                .mutation
                .digest(identity, Producer(0), intent.operation)?
    {
        return Err(Error::new(
            ErrorCode::IntegrityError,
            "receipt operation commitment mismatch",
        )
        .into());
    }
    let valid = match (&intent.mutation, &receipt.body) {
        (
            Mutation::Admit(p),
            Outcome::Admitted {
                work,
                admitted_at,
                deadline,
                child,
                ..
            },
        ) => {
            p.work == *work
                && deadline.0 - admitted_at.0 == p.execution_ms.0
                && match p.mode.0 {
                    0 => child.is_none(),
                    1 | 2 => child.as_ref().is_some_and(|c| c.producer.0 == p.mode.0 - 1),
                    _ => false,
                }
        }
        (
            Mutation::Declare {
                scope,
                entity_ids,
                seal,
            },
            Outcome::Declared {
                scope: actual,
                producer,
                accepted_count,
                seal: digest,
                ..
            },
        ) => {
            scope == actual
                && producer.0 == 0
                && accepted_count.0 == entity_ids.len() as u64
                && *seal == digest.is_some()
        }
        (
            Mutation::Retry {
                work,
                expected_attempt,
            },
            Outcome::Retried {
                work: actual,
                expected_attempt: attempt,
                ..
            },
        ) => work == actual && expected_attempt == attempt,
        (Mutation::Cancel { work }, Outcome::Cancelled { work: actual, .. })
        | (Mutation::Skip { work }, Outcome::Skipped { work: actual, .. }) => work == actual,
        (Mutation::ScopeCancel { scope }, Outcome::ScopeCancelled { scope: actual, .. }) => {
            scope == actual
        }
        _ => false,
    };
    if !valid {
        return Err(Error::new(
            ErrorCode::IntegrityError,
            "receipt outcome disagrees with immutable intent",
        )
        .into());
    }
    Ok(())
}
