//! Authenticated scope inventory and bottom-up closure evidence. Membership is
//! complete only after the full count and domain-separated seal are verified.
use super::*;
mod closure;
mod known;

codec::record!(ScopeObservation {
    scope: Number, producer: Producer, parent: Option<WorkKey>, declared: Number,
    seal: Option<Digest>, membership_verified: bool
} |s| {
    records::scope_identity(s.scope, s.producer, s.parent.as_ref())?;
    require(!s.membership_verified || s.seal.is_some(), "verified membership lacks seal")
});
codec::record!(ScopeMember { work: WorkKey, terminal: Option<State> } |s| {
    require(s.terminal.is_none_or(State::is_terminal), "nonterminal membership hint")
});

fn namespace(
    identity: &SessionIdentity,
    table: &str,
    scope: Number,
    entity: Option<Id>,
) -> Result<Vec<u8>> {
    let mut bytes = table.as_bytes().to_vec();
    bytes.extend(codec::encode(&identity.authority, MAX_HEADER)?);
    bytes.extend(codec::encode(&identity.owner, MAX_HEADER)?);
    bytes.extend(codec::encode(&identity.generation, MAX_HEADER)?);
    bytes.extend(codec::encode(&scope, MAX_HEADER)?);
    bytes.extend(codec::encode(&entity, MAX_HEADER)?);
    Ok(bytes)
}
fn not_ready(detail: &'static str) -> JournalError {
    Error::new(ErrorCode::NotReady, detail).into()
}
fn scope_row(connection: &Connection, table: &str, scope: Number) -> Result<Option<i64>> {
    scope.check()?;
    Ok(connection
        .query_row(
            &format!("SELECT rowid FROM {table} WHERE scope=?1"),
            [scope.0 as i64],
            |r| r.get(0),
        )
        .optional()?)
}

impl Journal {
    /// Supply the actual correlated request and authenticated response. Pages
    /// may arrive out of order; unsealed snapshots never establish completeness.
    pub fn observe_scope_page(
        &self,
        request: &Control,
        response: &Control,
    ) -> Result<ScopeObservation> {
        request.encode(MAX_CONTROL_LIMIT)?;
        response.encode(MAX_CONTROL_LIMIT)?;
        let Control::Scope(Scope::Page {
            request: id,
            scope: requested,
            after_entity,
            limit,
        }) = request
        else {
            return Err(Error::frame("expected scope page request").into());
        };
        let Control::Scope(Scope::PageResponse {
            request: actual,
            scope,
            producer,
            parent,
            seal,
            declared,
            entries,
            more,
            ..
        }) = response
        else {
            return Err(Error::frame("expected scope page response").into());
        };
        check(
            id == actual
                && requested == scope
                && entries.len() as u64 <= limit.0
                && entries.iter().all(|entry| entry.entity.0 > after_entity.0),
            "scope page correlation mismatch",
        )?;
        check(
            after_entity.0 != 0 || *more || entries.len() as u64 == declared.0,
            "initial final page omits declared membership",
        )?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let incoming = ScopeObservation {
            scope: *scope,
            producer: *producer,
            parent: parent.clone(),
            declared: *declared,
            seal: *seal,
            membership_verified: false,
        };
        let previous = self.read_scope(&tx, *scope)?;
        let mut retained = match &previous {
            Some(old) => merge(old, &incoming)?,
            None => incoming,
        };
        self.scope_room(retained.declared.0)?;
        self.validate_scope_identity(&tx, &retained)?;
        // An append-only declaration cannot insert an omitted old ID between
        // two IDs in a later page. Query only the page interval, not all members.
        if let Some(last) = entries.last() {
            let mut statement = tx.prepare("SELECT entity FROM scope_members WHERE scope=?1 AND entity>?2 AND entity<=?3 ORDER BY entity")?;
            let mut rows = statement.query(params![
                scope.0 as i64,
                after_entity.0 as i64,
                last.entity.0 as i64
            ])?;
            while let Some(row) = rows.next()? {
                let entity = Id(row.get::<_, i64>(0)? as u64);
                check(
                    entries
                        .binary_search_by_key(&entity, |entry| entry.entity)
                        .is_ok(),
                    "page omits known immutable member",
                )?;
            }
        }
        if !*more
            && previous
                .as_ref()
                .is_none_or(|old| declared.0 >= old.declared.0)
        {
            let end = entries
                .last()
                .map_or(after_entity.0, |entry| entry.entity.0);
            let beyond: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM scope_members WHERE scope=?1 AND entity>?2)",
                params![scope.0 as i64, end as i64],
                |r| r.get(0),
            )?;
            check(!beyond, "page falsely ends before known membership")?;
        }
        for entry in entries {
            self.save_member(
                &tx,
                &ScopeMember {
                    work: WorkKey {
                        scope: *scope,
                        producer: *producer,
                        entity: entry.entity,
                    },
                    terminal: entry.state.is_terminal().then_some(entry.state),
                },
            )?;
        }
        if let Some(last) = entries.last() {
            let prefix: i64 = tx.query_row(
                "SELECT count(*) FROM scope_members WHERE scope=?1 AND entity<=?2",
                params![scope.0 as i64, last.entity.0 as i64],
                |r| r.get(0),
            )?;
            check(
                prefix >= 0 && prefix as u64 <= declared.0,
                "page membership exceeds its declared snapshot",
            )?;
        }
        self.refresh_scope(&tx, &mut retained)?;
        self.write_scope(&tx, &retained)?;
        self.validate_scope_relations(&tx)?;
        tx.commit()?;
        Ok(retained)
    }

    pub fn scope_observation(&self, scope: Number) -> Result<Option<ScopeObservation>> {
        let connection = self.connect()?;
        let tx = connection.unchecked_transaction()?;
        let value = self.read_scope(&tx, scope)?;
        if let Some(value) = &value {
            self.check_scope_record(&tx, value)?;
        }
        tx.commit()?;
        Ok(value)
    }

    /// Bounded inventory of known identities. Terminal hints from membership
    /// pages are not WORK views and cannot supply missing attempts or manifests.
    pub fn scope_members(
        &self,
        scope: Number,
        after: Number,
        limit: PageLimit,
    ) -> Result<Vec<ScopeMember>> {
        scope.check()?;
        after.check()?;
        limit.check()?;
        let connection = self.connect()?;
        let tx = connection.unchecked_transaction()?;
        let observed = self
            .read_scope(&tx, scope)?
            .ok_or_else(|| not_ready("scope identity not observed"))?;
        let mut statement = tx.prepare("SELECT entity FROM scope_members WHERE scope=?1 AND entity>?2 ORDER BY entity LIMIT ?3")?;
        let mut rows = statement.query(params![scope.0 as i64, after.0 as i64, limit.0 as i64])?;
        let mut members = Vec::with_capacity(limit.0 as usize);
        while let Some(row) = rows.next()? {
            let work = WorkKey {
                scope,
                producer: observed.producer,
                entity: Id(row.get::<_, i64>(0)? as u64),
            };
            members.push(
                self.read_member(&tx, &work)?
                    .ok_or(JournalError::Corrupt("scope member disappeared"))?,
            );
        }
        drop(rows);
        drop(statement);
        tx.commit()?;
        Ok(members)
    }

    fn scope_room(&self, count: u64) -> Result<()> {
        if count > self.limits.observations.0 {
            return Err(
                Error::new(ErrorCode::LimitExceeded, "client scope inventory ceiling").into(),
            );
        }
        Ok(())
    }
    fn inventory_room(&self, connection: &Connection, table: &str) -> Result<()> {
        let count: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        if count < 0 {
            return Err(JournalError::Corrupt("negative scope inventory"));
        }
        self.scope_room(count as u64 + 1)
    }
    fn read_scope_image<T: Wire>(
        &self,
        connection: &Connection,
        table: &str,
        scope: Number,
    ) -> Result<Option<T>> {
        let Some(row) = scope_row(connection, table, scope)? else {
            return Ok(None);
        };
        let image = storage::read(connection, table, "image", row)?
            .ok_or(JournalError::Corrupt("scope image missing"))?;
        let key = namespace(&self.read_identity(connection)?, table, scope, None)?;
        Ok(Some(codec::decode(
            storage::unseal(&key, &image)?,
            MAX_HEADER,
        )?))
    }
    fn read_scope(
        &self,
        connection: &Connection,
        scope: Number,
    ) -> Result<Option<ScopeObservation>> {
        let value: Option<ScopeObservation> = self.read_scope_image(connection, "scopes", scope)?;
        if let Some(value) = &value {
            check(value.scope == scope, "scope index changed")?;
            let index = connection.query_row("SELECT producer,parent_scope,parent_producer,parent_entity FROM scopes WHERE scope=?1", [scope.0 as i64],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?, r.get::<_, Option<i64>>(2)?, r.get::<_, Option<i64>>(3)?)))?;
            check(
                index
                    == (
                        value.producer.0 as i64,
                        value.parent.as_ref().map(|p| p.scope.0 as i64),
                        value.parent.as_ref().map(|p| p.producer.0 as i64),
                        value.parent.as_ref().map(|p| p.entity.0 as i64),
                    ),
                "scope parent index changed",
            )?;
        }
        Ok(value)
    }
    fn write_scope(&self, connection: &Connection, value: &ScopeObservation) -> Result<()> {
        value.check()?;
        let key = namespace(
            &self.read_identity(connection)?,
            "scopes",
            value.scope,
            None,
        )?;
        let image = storage::seal(&key, &codec::encode(value, MAX_HEADER)?);
        if let Some(row) = scope_row(connection, "scopes", value.scope)? {
            storage::write(connection, "scopes", "image", row, &image)?;
        } else {
            self.inventory_room(connection, "scopes")?;
            connection.execute("INSERT INTO scopes(scope,producer,parent_scope,parent_producer,parent_entity,image) VALUES(?1,?2,?3,?4,?5,?6)",
                params![value.scope.0 as i64, value.producer.0 as i64, value.parent.as_ref().map(|p| p.scope.0 as i64), value.parent.as_ref().map(|p| p.producer.0 as i64), value.parent.as_ref().map(|p| p.entity.0 as i64), image])?;
        }
        Ok(())
    }
    fn read_member(&self, connection: &Connection, work: &WorkKey) -> Result<Option<ScopeMember>> {
        work.check()?;
        let row: Option<i64> = connection
            .query_row(
                "SELECT rowid FROM scope_members WHERE scope=?1 AND entity=?2",
                params![work.scope.0 as i64, work.entity.0 as i64],
                |r| r.get(0),
            )
            .optional()?;
        let Some(row) = row else { return Ok(None) };
        let image = storage::read(connection, "scope_members", "image", row)?
            .ok_or(JournalError::Corrupt("member image missing"))?;
        let key = namespace(
            &self.read_identity(connection)?,
            "scope_members",
            work.scope,
            Some(work.entity),
        )?;
        let member: ScopeMember = codec::decode(storage::unseal(&key, &image)?, MAX_HEADER)?;
        check(member.work == *work, "member identity index changed")?;
        Ok(Some(member))
    }
    fn save_member(&self, connection: &Connection, value: &ScopeMember) -> Result<()> {
        value.check()?;
        self.validate_member(connection, value)?;
        let mut value = value.clone();
        if let Some(old) = self.read_member(connection, &value.work)? {
            if let (Some(a), Some(b)) = (old.terminal, value.terminal) {
                check(a == b, "page terminal outcome changed")?;
            }
            value.terminal = old.terminal.or(value.terminal);
        } else {
            self.inventory_room(connection, "scope_members")?;
        }
        let key = namespace(
            &self.read_identity(connection)?,
            "scope_members",
            value.work.scope,
            Some(value.work.entity),
        )?;
        let image = storage::seal(&key, &codec::encode(&value, MAX_HEADER)?);
        connection.execute("INSERT INTO scope_members(scope,entity,image) VALUES(?1,?2,?3) ON CONFLICT(scope,entity) DO UPDATE SET image=excluded.image", params![value.work.scope.0 as i64, value.work.entity.0 as i64, image])?;
        Ok(())
    }
    fn refresh_scope(&self, connection: &Connection, scope: &mut ScopeObservation) -> Result<()> {
        self.apply_declaration_receipts(connection, scope)?;
        let count: i64 = connection.query_row(
            "SELECT count(*) FROM scope_members WHERE scope=?1",
            [scope.scope.0 as i64],
            |r| r.get(0),
        )?;
        check(
            count >= 0 && count as u64 <= scope.declared.0,
            "scope membership exceeds known count",
        )?;
        scope.membership_verified = false;
        if let Some(expected) = scope.seal
            && count as u64 == scope.declared.0
        {
            let mut seal = ScopeSeal::new(
                &self.read_identity(connection)?,
                scope.scope,
                scope.producer,
                scope.parent.as_ref(),
                scope.declared,
            )?;
            let mut statement = connection
                .prepare("SELECT entity FROM scope_members WHERE scope=?1 ORDER BY entity")?;
            let mut rows = statement.query([scope.scope.0 as i64])?;
            while let Some(row) = rows.next()? {
                let work = WorkKey {
                    scope: scope.scope,
                    producer: scope.producer,
                    entity: Id(row.get::<_, i64>(0)? as u64),
                };
                self.read_member(connection, &work)?
                    .ok_or(JournalError::Corrupt("seal member missing"))?;
                seal.push(work.entity)?;
            }
            check(seal.finish()? == expected, "scope membership seal mismatch")?;
            scope.membership_verified = true;
        }
        self.validate_known_membership(connection, scope)?;
        Ok(())
    }
    fn check_scope_record(&self, connection: &Connection, scope: &ScopeObservation) -> Result<()> {
        self.validate_scope_identity(connection, scope)?;
        let mut actual = scope.clone();
        self.refresh_scope(connection, &mut actual)?;
        check(
            actual == *scope,
            "retained scope observation disagrees with evidence",
        )
    }
}

fn merge(old: &ScopeObservation, incoming: &ScopeObservation) -> Result<ScopeObservation> {
    check(
        old.scope == incoming.scope
            && old.producer == incoming.producer
            && old.parent == incoming.parent,
        "scope identity or parent changed",
    )?;
    if let Some(seal) = old.seal {
        check(
            incoming.declared <= old.declared
                && incoming
                    .seal
                    .is_none_or(|s| s == seal && incoming.declared == old.declared),
            "sealed scope membership changed",
        )?;
        return Ok(old.clone());
    }
    if incoming.seal.is_some() {
        check(
            incoming.declared >= old.declared,
            "sealed count regressed below known declarations",
        )?;
        return Ok(incoming.clone());
    }
    Ok(ScopeObservation {
        declared: old.declared.max(incoming.declared),
        ..old.clone()
    })
}
