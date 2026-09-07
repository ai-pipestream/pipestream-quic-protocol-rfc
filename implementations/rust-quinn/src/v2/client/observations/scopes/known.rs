use super::*;

fn hint_matches_view(hint: State, view: &WorkView) -> Result<()> {
    if view.state.is_terminal() {
        check(
            view.state == hint,
            "membership and work terminal states disagree",
        )?;
    } else if view.state == State::CANCELLING {
        check(
            matches!(hint, State::CANCELLED | State::SKIPPED),
            "membership contradicts observed work fence",
        )?;
    }
    Ok(())
}
fn child_matches(
    scope: &ScopeObservation,
    work: &WorkKey,
    child: Option<&ChildScope>,
) -> Result<()> {
    check(
        scope.parent.as_ref() == Some(work)
            && child.is_some_and(|c| c.scope.0 == scope.scope.0 && c.producer == scope.producer),
        "scope disagrees with immutable parent child allocation",
    )
}

impl Journal {
    pub(super) fn validate_scope_identity(
        &self,
        connection: &Connection,
        scope: &ScopeObservation,
    ) -> Result<()> {
        scope.check()?;
        self.scope_room(scope.declared.0)?;
        if let Some(parent) = &scope.parent {
            self.known_member(connection, parent)?;
            check(
                parent.scope.0 != 0 || parent.producer.0 == 0,
                "child names impossible root producer",
            )?;
            let other: Option<i64> = connection.query_row("SELECT scope FROM scopes WHERE parent_scope=?1 AND parent_producer=?2 AND parent_entity=?3",
                params![parent.scope.0 as i64, parent.producer.0 as i64, parent.entity.0 as i64], |r| r.get(0)).optional()?;
            check(
                other.is_none_or(|s| s as u64 == scope.scope.0),
                "parent acquired a second child scope",
            )?;
            if let Some(view) = self.read_observed(connection, parent)?
                && (view.view.admitted_at.is_some() || view.view.state.is_terminal())
            {
                child_matches(scope, parent, view.view.child.as_ref())?;
            }
            self.with_work_receipts(connection, parent, |_, receipt| {
                if let Outcome::Admitted { child, .. } = &receipt.body {
                    child_matches(scope, parent, child.as_ref())?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }
    fn known_child(
        &self,
        connection: &Connection,
        work: &WorkKey,
        child: Option<&ChildScope>,
    ) -> Result<()> {
        let scope: Option<i64> = connection.query_row("SELECT scope FROM scopes WHERE parent_scope=?1 AND parent_producer=?2 AND parent_entity=?3",
            params![work.scope.0 as i64, work.producer.0 as i64, work.entity.0 as i64], |r| r.get(0)).optional()?;
        if let Some(scope) = scope {
            child_matches(
                &self
                    .read_scope(connection, Number(scope as u64))?
                    .ok_or(JournalError::Corrupt("child disappeared"))?,
                work,
                child,
            )?;
        }
        if let Some(child) = child
            && let Some(scope) = self.read_scope(connection, Number(child.scope.0))?
        {
            child_matches(&scope, work, Some(child))?;
        }
        Ok(())
    }
    fn known_member(&self, connection: &Connection, work: &WorkKey) -> Result<()> {
        if let Some(scope) = self.read_scope(connection, work.scope)? {
            check(
                scope.producer == work.producer,
                "work disagrees with observed scope producer",
            )?;
            if scope.membership_verified {
                check(
                    self.read_member(connection, work)?.is_some(),
                    "work absent from verified sealed membership",
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn validate_member(
        &self,
        connection: &Connection,
        member: &ScopeMember,
    ) -> Result<()> {
        if let Some(terminal) = member.terminal {
            if let Some(view) = self.read_observed(connection, &member.work)? {
                hint_matches_view(terminal, &view.view)?;
            }
            if self.read_manifest(connection, &member.work)?.is_some() {
                check(
                    terminal == State::SUCCEEDED,
                    "membership contradicts retained success manifest",
                )?;
            }
            self.with_work_receipts(connection, &member.work, |_, receipt| {
                receipts::terminal_hint(&receipt.body, terminal)
            })?;
        }
        Ok(())
    }
    pub(in crate::v2::client::observations) fn scope_view(
        &self,
        connection: &Connection,
        view: &WorkView,
    ) -> Result<()> {
        self.known_member(connection, &view.work)?;
        if let Some(member) = self.read_member(connection, &view.work)?
            && let Some(terminal) = member.terminal
        {
            hint_matches_view(terminal, view)?;
        }
        if view.admitted_at.is_some() || view.state.is_terminal() {
            self.known_child(connection, &view.work, view.child.as_ref())?;
        }
        self.with_scope_fences(connection, view.work.scope, |accepted| {
            if let Some(admitted) = view.admitted_at {
                check(
                    admitted <= accepted,
                    "admission follows known ancestor fence",
                )?;
            }
            if matches!(view.state, State::SUCCEEDED | State::FAILED) {
                check(
                    view.terminal_at.expect("validated terminal") <= accepted,
                    "terminal publication follows known ancestor fence",
                )?;
            }
            Ok(())
        })
    }
    pub(in crate::v2::client::observations) fn scope_manifest(
        &self,
        connection: &Connection,
        manifest: &Manifest,
    ) -> Result<()> {
        self.known_member(connection, &manifest.work)?;
        if let Some(member) = self.read_member(connection, &manifest.work)? {
            check(
                member
                    .terminal
                    .is_none_or(|state| state == State::SUCCEEDED),
                "manifest contradicts terminal membership",
            )?;
        }
        self.with_scope_fences(connection, manifest.work.scope, |accepted| {
            check(
                manifest.committed_at <= accepted,
                "manifest committed after known ancestor fence",
            )
        })
    }
    pub(in crate::v2::client::observations) fn scope_receipt(
        &self,
        connection: &Connection,
        intent: &Intent,
        receipt: &OperationReceipt,
    ) -> Result<()> {
        if let Some(work) = receipts::target(intent) {
            self.known_member(connection, work)?;
            if let Some(member) = self.read_member(connection, work)?
                && let Some(terminal) = member.terminal
            {
                receipts::terminal_hint(&receipt.body, terminal)?;
            }
            if let Outcome::Admitted { child, .. } = &receipt.body {
                self.known_child(connection, work, child.as_ref())?;
            }
            if let Some(time) = receipts::event_time(&receipt.body) {
                self.with_scope_fences(connection, work.scope, |accepted| {
                    check(
                        time <= accepted,
                        "work mutation committed after known ancestor fence",
                    )
                })?;
            }
        }
        if let Mutation::Declare { scope, .. } = &intent.mutation
            && let Some(mut observed) = self.read_scope(connection, *scope)?
        {
            apply_declaration(&mut observed, receipt)?;
            self.check_declaration_members(connection, &observed, intent)?;
        }
        Ok(())
    }
    pub(in crate::v2::client::observations) fn scope_receipt_committed(
        &self,
        connection: &Connection,
        intent: &Intent,
    ) -> Result<()> {
        if let Mutation::Declare { scope, .. } = &intent.mutation
            && let Some(mut observed) = self.read_scope(connection, *scope)?
        {
            self.refresh_scope(connection, &mut observed)?;
            self.write_scope(connection, &observed)?;
        }
        if matches!(
            intent.mutation,
            Mutation::Declare { .. } | Mutation::ScopeCancel { .. }
        ) {
            // A sealing receipt can verify cached membership without a new
            // page. Recheck retained children before committing that evidence.
            self.validate_scope_relations(connection)?;
        }
        Ok(())
    }
    fn with_scope_receipts(
        &self,
        connection: &Connection,
        scope: Option<Number>,
        kind: u8,
        mut run: impl FnMut(&Intent, &OperationReceipt) -> Result<()>,
    ) -> Result<()> {
        let mut statement = connection.prepare("SELECT rowid,operation FROM operations WHERE kind=?1 AND (?2 IS NULL OR scope=?2) AND receipt IS NOT NULL")?;
        let mut rows = statement.query(params![kind, scope.map(|s| s.0 as i64)])?;
        while let Some(row) = rows.next()? {
            let operation = storage::read_operation(row, 1)?;
            let intent = self.read_intent(connection, operation)?;
            let image = storage::read(connection, "operations", "receipt", row.get(0)?)?
                .ok_or(JournalError::Corrupt("scope receipt missing"))?;
            let receipt: OperationReceipt =
                codec::decode(storage::unseal(&operation.0, &image)?, MAX_HEADER)?;
            storage::validate_receipt(&intent, &self.read_identity(connection)?, &receipt)?;
            run(&intent, &receipt)?;
        }
        Ok(())
    }
    pub(super) fn apply_declaration_receipts(
        &self,
        connection: &Connection,
        scope: &mut ScopeObservation,
    ) -> Result<()> {
        self.with_scope_receipts(connection, Some(scope.scope), 1, |_, receipt| {
            apply_declaration(scope, receipt)
        })
    }
    fn check_declaration_members(
        &self,
        connection: &Connection,
        scope: &ScopeObservation,
        intent: &Intent,
    ) -> Result<()> {
        if scope.membership_verified
            && let Mutation::Declare { entity_ids, .. } = &intent.mutation
        {
            for entity in entity_ids {
                check(
                    self.read_member(
                        connection,
                        &WorkKey {
                            scope: scope.scope,
                            producer: scope.producer,
                            entity: *entity,
                        },
                    )?
                    .is_some(),
                    "declaration omitted from verified membership",
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn validate_known_membership(
        &self,
        connection: &Connection,
        scope: &ScopeObservation,
    ) -> Result<()> {
        self.with_scope_receipts(connection, Some(scope.scope), 1, |intent, _| {
            self.check_declaration_members(connection, scope, intent)
        })?;
        for table in ["observations", "manifests"] {
            let mut statement = connection.prepare(&format!(
                "SELECT producer,entity FROM {table} WHERE scope=?1"
            ))?;
            let mut rows = statement.query([scope.scope.0 as i64])?;
            while let Some(row) = rows.next()? {
                check(
                    row.get::<_, i64>(0)? as u64 == scope.producer.0,
                    "scope disagrees with known work producer",
                )?;
                if scope.membership_verified {
                    check(
                        self.read_member(
                            connection,
                            &WorkKey {
                                scope: scope.scope,
                                producer: scope.producer,
                                entity: Id(row.get::<_, i64>(1)? as u64),
                            },
                        )?
                        .is_some(),
                        "known work omitted from sealed membership",
                    )?;
                }
            }
        }
        Ok(())
    }
    fn descendant_of(
        &self,
        connection: &Connection,
        mut scope: Number,
        ancestor: Number,
    ) -> Result<bool> {
        if ancestor.0 == 0 {
            return Ok(true);
        }
        loop {
            if scope == ancestor {
                return Ok(true);
            }
            if scope.0 < ancestor.0 {
                return Ok(false);
            }
            let Some(observed) = self.read_scope(connection, scope)? else {
                return Ok(false);
            };
            let Some(parent) = observed.parent else {
                return Ok(false);
            };
            // Scope record validation enforces strictly decreasing parent IDs.
            scope = parent.scope;
        }
    }
    fn with_scope_fences(
        &self,
        connection: &Connection,
        scope: Number,
        mut run: impl FnMut(Number) -> Result<()>,
    ) -> Result<()> {
        self.with_scope_receipts(connection, None, 4, |_, receipt| {
            let Outcome::ScopeCancelled {
                scope: ancestor,
                accepted_at,
            } = receipt.body
            else {
                return Err(JournalError::Corrupt("scope fence kind changed"));
            };
            if self.descendant_of(connection, scope, ancestor)? {
                run(accepted_at)?;
            }
            Ok(())
        })
    }
    pub(super) fn validate_scope_relations(&self, connection: &Connection) -> Result<()> {
        let mut statement = connection.prepare("SELECT scope FROM scopes")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let scope = Number(row.get::<_, i64>(0)? as u64);
            self.validate_scope_identity(
                connection,
                &self
                    .read_scope(connection, scope)?
                    .ok_or(JournalError::Corrupt("scope disappeared"))?,
            )?;
        }
        for table in ["observations", "manifests"] {
            let mut statement =
                connection.prepare(&format!("SELECT scope,producer,entity FROM {table}"))?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let work = WorkKey {
                    scope: Number(row.get::<_, i64>(0)? as u64),
                    producer: Producer(row.get::<_, i64>(1)? as u64),
                    entity: Id(row.get::<_, i64>(2)? as u64),
                };
                if table == "observations" {
                    self.scope_view(
                        connection,
                        &self
                            .read_observed(connection, &work)?
                            .ok_or(JournalError::Corrupt("view disappeared"))?
                            .view,
                    )?;
                } else {
                    self.scope_manifest(
                        connection,
                        &self
                            .read_manifest(connection, &work)?
                            .ok_or(JournalError::Corrupt("manifest disappeared"))?,
                    )?;
                }
            }
        }
        for kind in [0, 2, 3, 5] {
            self.with_scope_receipts(connection, None, kind, |intent, receipt| {
                self.scope_receipt(connection, intent, receipt)
            })?;
        }
        Ok(())
    }
}

fn apply_declaration(scope: &mut ScopeObservation, receipt: &OperationReceipt) -> Result<()> {
    let Outcome::Declared {
        scope: actual,
        producer,
        declared,
        seal,
        ..
    } = &receipt.body
    else {
        return Err(JournalError::Corrupt("declaration receipt kind changed"));
    };
    check(
        scope.scope == *actual && scope.producer == *producer,
        "scope disagrees with declaration receipt identity",
    )?;
    let evidence = ScopeObservation {
        declared: *declared,
        seal: *seal,
        membership_verified: false,
        ..scope.clone()
    };
    *scope = merge(scope, &evidence)?;
    Ok(())
}
