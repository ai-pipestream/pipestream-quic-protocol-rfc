//! Refuse changed owned schemas instead of recreating indexes over retained work.

use super::*;

pub(super) const SESSION_IDS: &str = "SELECT CASE WHEN length(CAST(session_id AS BLOB))
    BETWEEN 1 AND 128 THEN session_id ELSE NULL END FROM pipestream_sessions ORDER BY session_id";

pub(super) fn session_id(row: &rusqlite::Row<'_>, column: usize) -> Result<String, StoreError> {
    let id: Option<String> = row.get(column)?;
    let id = id.ok_or_else(|| StoreError::Corrupt("invalid retained session identity".into()))?;
    crate::session::validate_session_id(&id)
        .map_err(|_| StoreError::Corrupt("invalid retained session identity".into()))?;
    Ok(id)
}

pub(super) fn initialize_root(connection: &mut Connection, ddl: &str) -> Result<(), StoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let owned: u32 = transaction.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE name GLOB 'pipestream_*'",
        [],
        |r| r.get(0),
    )?;
    if owned == 0 {
        transaction.execute_batch(ddl)?;
        binding::initialize(&transaction)?;
    } else {
        verify(&transaction, ddl)?;
        binding::read(&transaction)?;
    }
    transaction.commit()?;
    Ok(())
}

/// Only built-in DDL is split here. Whitespace and SQLite's removal of
/// IF NOT EXISTS are immaterial; columns, constraints and index targets are not.
pub(super) fn verify(connection: &Connection, ddl: &str) -> Result<(), StoreError> {
    for statement in ddl.split(';').filter(|s| !s.trim().is_empty()) {
        let normalized = statement.replace("IF NOT EXISTS ", "");
        let expected: Vec<_> = normalized.split_whitespace().collect();
        let name = expected
            .get(2)
            .ok_or_else(|| StoreError::Corrupt("invalid built-in schema declaration".into()))?;
        let actual: Option<Option<String>> = connection.query_row(
            "SELECT CASE WHEN length(CAST(sql AS BLOB)) <= 8192 THEN sql ELSE NULL END FROM sqlite_schema WHERE name = ?1",
            [name], |r| r.get(0),
        ).optional()?;
        if actual
            .flatten()
            .is_none_or(|sql| sql.split_whitespace().collect::<Vec<_>>() != expected)
        {
            return Err(StoreError::Corrupt(format!(
                "missing or changed retained schema: {name}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
