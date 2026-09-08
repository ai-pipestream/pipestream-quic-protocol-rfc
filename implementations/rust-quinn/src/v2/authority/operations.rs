//! Immutable operation evidence and bounded declaration-to-membership checks.
//! This is local storage encoding, not a different operation commitment on wire.

use super::*;
use sha2::{Digest as _, Sha256};

const MAX_RECORD_BYTES: usize = 4096;

fn identity(tx: &Transaction<'_>, generation: Id) -> Result<SessionIdentity> {
    let (authority, owner): (Option<String>, Option<String>) = tx.query_row(
        "SELECT CASE WHEN length(a.name)<=128 THEN a.name END,
                CASE WHEN length(s.owner)<=128 THEN s.owner END
         FROM authority a JOIN sessions s ON s.generation=?1 WHERE a.singleton=1",
        [sql(generation.0)?],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let value = SessionIdentity {
        authority: IdentityLabel(
            authority.ok_or(StoreError::Corrupt("invalid operation authority"))?,
        ),
        owner: IdentityLabel(owner.ok_or(StoreError::Corrupt("invalid operation owner"))?),
        generation,
    };
    value
        .authority
        .check()
        .and_then(|_| value.owner.check())
        .map_err(|_| StoreError::Corrupt("invalid operation identity"))?;
    Ok(value)
}

fn checksum(
    identity: &SessionIdentity,
    originator: Producer,
    receipt: &OperationReceipt,
    bytes: &[u8],
    declaration: Option<&[u8]>,
) -> Digest {
    let mut out = codec::Writer::new();
    out.array(8);
    identity.authority.write(&mut out);
    identity.owner.write(&mut out);
    identity.generation.write(&mut out);
    originator.write(&mut out);
    receipt.operation.write(&mut out);
    receipt.request_digest.write(&mut out);
    out.bytes(bytes);
    match declaration {
        Some(bytes) => out.bytes(bytes),
        None => out.null(),
    }
    let mut hash = Sha256::new();
    hash.update(b"pipestream-authority-operation-v1");
    hash.update(out.finish());
    Digest(hash.finalize().into())
}

/// The caller funds and authorizes the enclosing mutation transaction. Retaining
/// evidence here neither commits that transaction nor authorizes execution.
pub(super) fn retain(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    originator: Producer,
    receipt: &OperationReceipt,
    declaration: Option<&Scope>,
) -> Result<()> {
    let bytes = codec::encode(receipt, MAX_RECORD_BYTES)?;
    let declaration = declaration
        .map(|value| codec::encode(value, MAX_RECORD_BYTES))
        .transpose()?;
    if matches!(receipt.body, Outcome::Declared { .. }) != declaration.is_some() {
        return Err(StoreError::Corrupt(
            "declaration intent and receipt kind differ",
        ));
    }
    let hash = checksum(
        identity,
        originator,
        receipt,
        &bytes,
        declaration.as_deref(),
    );
    tx.execute(
        "INSERT INTO operations(generation,originator,operation,digest,receipt,declaration,record_hash)
         VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![sql(identity.generation.0)?, sql(originator.0)?, receipt.operation.0.as_slice(),
            receipt.request_digest.0.as_slice(), bytes, declaration, hash.0.as_slice()],
    )?;
    Ok(())
}

struct Retained {
    digest: Option<Vec<u8>>,
    receipt: Option<Vec<u8>>,
    declaration: Option<Vec<u8>>,
    has_declaration: bool,
    checksum: Option<Vec<u8>>,
}

pub(super) fn load(
    tx: &Transaction<'_>,
    generation: Id,
    originator: Producer,
    id: OperationId,
) -> Result<Option<OperationReceipt>> {
    let retained = tx
        .query_row(
            "SELECT CASE WHEN length(digest)=32 THEN digest END,
                CASE WHEN length(receipt)<=4096 THEN receipt END,
                CASE WHEN length(declaration)<=4096 THEN declaration END,
                declaration IS NOT NULL,
                CASE WHEN length(record_hash)=32 THEN record_hash END
         FROM operations WHERE generation=?1 AND originator=?2 AND operation=?3",
            params![sql(generation.0)?, sql(originator.0)?, id.0.as_slice()],
            |row| {
                Ok(Retained {
                    digest: row.get(0)?,
                    receipt: row.get(1)?,
                    declaration: row.get(2)?,
                    has_declaration: row.get(3)?,
                    checksum: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(retained) = retained else {
        return Ok(None);
    };
    let bytes = retained
        .receipt
        .ok_or(StoreError::Corrupt("operation receipt exceeds bound"))?;
    let receipt: OperationReceipt = unpack(&bytes)?;
    if receipt.operation != id
        || retained.digest.as_deref() != Some(receipt.request_digest.0.as_slice())
        || retained.has_declaration != retained.declaration.is_some()
    {
        return Err(StoreError::Corrupt(
            "operation index or intent differs from receipt",
        ));
    }
    let identity = identity(tx, generation)?;
    let expected = checksum(
        &identity,
        originator,
        &receipt,
        &bytes,
        retained.declaration.as_deref(),
    );
    if retained.checksum.as_deref() != Some(expected.0.as_slice()) {
        return Err(StoreError::Corrupt("operation receipt checksum differs"));
    }
    verify_declaration(
        tx,
        &identity,
        originator,
        &receipt,
        retained.declaration.as_deref(),
    )?;
    Ok(Some(receipt))
}

fn verify_declaration(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    originator: Producer,
    receipt: &OperationReceipt,
    bytes: Option<&[u8]>,
) -> Result<()> {
    let Outcome::Declared {
        scope,
        producer,
        accepted_count,
        declared,
        seal,
    } = &receipt.body
    else {
        return if bytes.is_none() {
            Ok(())
        } else {
            Err(StoreError::Corrupt(
                "non-declaration receipt carries declaration intent",
            ))
        };
    };
    let bytes = bytes.ok_or(StoreError::Corrupt("declaration intent missing"))?;
    let Scope::Declare {
        request,
        operation,
        scope: requested_scope,
        entity_ids,
        seal: requested_seal,
    } = unpack(bytes)?
    else {
        return Err(StoreError::Corrupt("retained declaration has wrong kind"));
    };
    if request != Id(1)
        || operation != receipt.operation
        || requested_scope != *scope
        || *producer != originator
        || accepted_count.0 != entity_ids.len() as u64
        || requested_seal != seal.is_some()
    {
        return Err(StoreError::Corrupt(
            "declaration receipt contradicts its intent",
        ));
    }
    let mutation = Mutation::Declare {
        scope: requested_scope,
        entity_ids,
        seal: requested_seal,
    };
    if mutation.digest(identity, originator, operation)? != receipt.request_digest {
        return Err(StoreError::Corrupt("declaration intent commitment differs"));
    }
    let retained =
        scopes::load(tx, identity.generation, *scope).map_err(|failure| match failure {
            StoreError::Protocol(error) if error.code == ErrorCode::NotFound => {
                StoreError::Corrupt("declaration scope missing")
            }
            other => other,
        })?;
    if retained.producer != *producer
        || *declared > retained.declared
        || (seal.is_some() && (*seal != retained.seal || *declared != retained.declared))
    {
        return Err(StoreError::Corrupt(
            "declaration receipt contradicts retained scope",
        ));
    }
    let Mutation::Declare { entity_ids, .. } = mutation else {
        unreachable!()
    };
    // The covering index avoids scanning every scope for each declaration batch.
    let mut statement = tx.prepare(
        "SELECT scope,entity FROM work WHERE generation=?1 AND producer=?2 AND declaration=?3
         ORDER BY scope,entity",
    )?;
    let mut rows = statement.query(params![
        sql(identity.generation.0)?,
        sql(originator.0)?,
        operation.0.as_slice()
    ])?;
    for entity in entity_ids {
        let row = rows
            .next()?
            .ok_or(StoreError::Corrupt("declaration member missing"))?;
        if number(row, 0)? != scope.0 || number(row, 1)? != entity.0 {
            return Err(StoreError::Corrupt("declaration member identity differs"));
        }
    }
    if rows.next()?.is_some() {
        return Err(StoreError::Corrupt("undeclared member linked to operation"));
    }
    Ok(())
}

/// Run after retirement-proof verification. Partial cleanup is permitted only
/// for those proven retiring sessions, not unexplained gaps in a live session.
pub(super) fn verify(tx: &Transaction<'_>) -> Result<()> {
    let mut foreign = tx.prepare("PRAGMA foreign_key_check")?;
    if foreign.query([])?.next()?.is_some() {
        return Err(StoreError::Corrupt("authority foreign-key check failed"));
    }
    let mut sessions = tx.prepare(
        "SELECT generation,entities,operations FROM sessions WHERE retiring=0 ORDER BY generation",
    )?;
    let mut rows = sessions.query([])?;
    while let Some(row) = rows.next()? {
        let generation = Id(number(row, 0)?);
        let entities = number(row, 1)?;
        let operations = number(row, 2)?;
        let identity = identity(tx, generation)?;
        let (binding, _) = sessions::load(tx, &identity.authority, generation)?
            .ok_or(StoreError::Corrupt("live declaration session missing"))?;
        if entities > binding.limits.entities.0 || operations > binding.limits.operations.0 {
            return Err(StoreError::Corrupt("session declaration capacity differs"));
        }
        let mut scope_query =
            tx.prepare("SELECT scope FROM scopes WHERE generation=?1 ORDER BY scope")?;
        let mut scopes = scope_query.query([sql(generation.0)?])?;
        let mut members = 0u64;
        let mut scope_count = 0u64;
        while let Some(scope) = scopes.next()? {
            scope_count = scope_count
                .checked_add(1)
                .ok_or(StoreError::Corrupt("scope count overflow"))?;
            members = members
                .checked_add(verify_scope(tx, &identity, Number(number(scope, 0)?))?)
                .ok_or(StoreError::Corrupt("membership count overflow"))?;
        }
        if scope_count == 0 || scope_count > binding.limits.scopes.0 || members != entities {
            return Err(StoreError::Corrupt("session membership accounting differs"));
        }
        let mut query = tx.prepare("SELECT originator,CASE WHEN length(operation)=16 THEN operation END FROM operations WHERE generation=?1")?;
        let mut retained = query.query([sql(generation.0)?])?;
        let mut observed = 0u64;
        let mut covered = 0u64;
        while let Some(operation) = retained.next()? {
            let originator = Producer(number(operation, 0)?);
            let bytes: Option<Vec<u8>> = operation.get(1)?;
            let bytes = bytes.ok_or(StoreError::Corrupt("invalid operation identity"))?;
            let id = OperationId(
                bytes
                    .try_into()
                    .map_err(|_| StoreError::Corrupt("invalid operation identity"))?,
            );
            originator
                .check()
                .and_then(|_| id.check())
                .map_err(|_| StoreError::Corrupt("invalid operation namespace"))?;
            let receipt = load(tx, generation, originator, id)?
                .ok_or(StoreError::Corrupt("operation disappeared during audit"))?;
            observed = observed
                .checked_add(1)
                .ok_or(StoreError::Corrupt("operation count overflow"))?;
            if let Outcome::Declared { accepted_count, .. } = receipt.body {
                covered = covered
                    .checked_add(accepted_count.0)
                    .ok_or(StoreError::Corrupt("declaration coverage overflow"))?;
            }
        }
        if observed != operations || covered != entities {
            return Err(StoreError::Corrupt(
                "operation coverage or accounting differs",
            ));
        }
    }
    Ok(())
}

fn verify_scope(tx: &Transaction<'_>, identity: &SessionIdentity, scope: Number) -> Result<u64> {
    let state = scopes::load(tx, identity.generation, scope)?;
    let mut seal = state
        .seal
        .map(|_| {
            ScopeSeal::new(
                identity,
                scope,
                state.producer,
                state.parent.as_ref(),
                state.declared,
            )
        })
        .transpose()
        .map_err(|_| StoreError::Corrupt("invalid retained scope seal metadata"))?;
    let mut statement = tx.prepare(
        "SELECT entity,producer,CASE WHEN length(declaration)=16 THEN declaration END
         FROM work WHERE generation=?1 AND scope=?2 ORDER BY entity",
    )?;
    let mut rows = statement.query(params![sql(identity.generation.0)?, sql(scope.0)?])?;
    let mut count = 0u64;
    let mut last = 0u64;
    let mut batch = None;
    let mut batch_end = 0;
    while let Some(row) = rows.next()? {
        let entity = Id(number(row, 0)?);
        let bytes: Option<Vec<u8>> = row.get(2)?;
        let id = OperationId(
            bytes
                .ok_or(StoreError::Corrupt("member declaration missing"))?
                .try_into()
                .map_err(|_| StoreError::Corrupt("invalid member declaration"))?,
        );
        if number(row, 1)? != state.producer.0 {
            return Err(StoreError::Corrupt("member producer differs from scope"));
        }
        if batch != Some(id) {
            if batch.is_some() && count != batch_end {
                return Err(StoreError::Corrupt(
                    "declaration receipt prefix count differs",
                ));
            }
            let receipt = load(tx, identity.generation, state.producer, id)?
                .ok_or(StoreError::Corrupt("member declaration operation missing"))?;
            let Outcome::Declared { declared, .. } = receipt.body else {
                return Err(StoreError::Corrupt(
                    "member linked to non-declaration operation",
                ));
            };
            batch_end = declared.0;
            batch = Some(id);
        }
        count = count
            .checked_add(1)
            .ok_or(StoreError::Corrupt("scope count overflow"))?;
        last = entity.0;
        if let Some(seal) = &mut seal {
            seal.push(entity)
                .map_err(|_| StoreError::Corrupt("scope seal membership count differs"))?;
        }
    }
    if count != state.declared.0
        || last != state.last_entity
        || (batch.is_some() && count != batch_end)
    {
        return Err(StoreError::Corrupt(
            "scope membership or receipt count differs",
        ));
    }
    if let Some(seal) = seal
        && Some(
            seal.finish()
                .map_err(|_| StoreError::Corrupt("scope seal membership count differs"))?,
        ) != state.seal
    {
        return Err(StoreError::Corrupt(
            "scope seal differs from retained members",
        ));
    }
    Ok(count)
}
