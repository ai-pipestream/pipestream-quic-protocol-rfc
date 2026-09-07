use super::*;

impl Journal {
    /// Persist coverage only after complete membership, every terminal WORK
    /// view and every child scope's already validated coverage agree with it.
    /// A membership page's state column cannot substitute for a full WORK view.
    pub fn record_checkpoint(&self, summary: &ScopeSummary) -> Result<()> {
        summary.check()?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.validate_coverage(&tx, summary)?;
        let key = namespace(
            &self.read_identity(&tx)?,
            "scope_coverage",
            summary.scope,
            None,
        )?;
        let image = storage::seal(&key, &codec::encode(summary, MAX_HEADER)?);
        if let Some(old) = self.read_coverage(&tx, summary.scope)? {
            check(old == *summary, "immutable scope closure changed")?;
        } else {
            self.inventory_room(&tx, "scope_coverage")?;
            tx.execute(
                "INSERT INTO scope_coverage(scope,image) VALUES(?1,?2)",
                params![summary.scope.0 as i64, image],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn covered_scope(&self, scope: Number) -> Result<Option<ScopeSummary>> {
        let connection = self.connect()?;
        let tx = connection.unchecked_transaction()?;
        let value = self.read_coverage(&tx, scope)?;
        if let Some(value) = &value {
            self.validate_stored_coverage(&tx, value)?;
        }
        tx.commit()?;
        Ok(value)
    }

    /// The transport must independently drain pending requests/transfers and
    /// authenticate/correlate the echoed response. This constructs only the
    /// exact durable root cut, not an acknowledgment of connection shutdown.
    pub fn root_completion(&self, request: Id) -> Result<Control> {
        let root = self
            .covered_scope(Number(0))?
            .ok_or_else(|| not_ready("root coverage not recorded"))?;
        let control = Control::Drain(Drain::Complete {
            request,
            generation: self.identity()?.generation,
            root_summary: root,
        });
        control.encode(MAX_HEADER)?;
        Ok(control)
    }

    fn read_coverage(
        &self,
        connection: &Connection,
        scope: Number,
    ) -> Result<Option<ScopeSummary>> {
        let value: Option<ScopeSummary> =
            self.read_scope_image(connection, "scope_coverage", scope)?;
        check(
            value.as_ref().is_none_or(|s| s.scope == scope),
            "scope coverage index changed",
        )?;
        Ok(value)
    }
    fn validate_stored_coverage(
        &self,
        connection: &Connection,
        summary: &ScopeSummary,
    ) -> Result<()> {
        match self.validate_coverage(connection, summary) {
            Err(JournalError::Protocol(error)) if error.code == ErrorCode::NotReady => Err(
                JournalError::Corrupt("stored coverage lost required evidence"),
            ),
            result => result,
        }
    }
    fn validate_coverage(&self, connection: &Connection, summary: &ScopeSummary) -> Result<()> {
        summary.check()?;
        let scope = self
            .read_scope(connection, summary.scope)?
            .ok_or_else(|| not_ready("scope membership not observed"))?;
        self.check_scope_record(connection, &scope)?;
        if !scope.membership_verified {
            return Err(not_ready("full sealed membership not verified"));
        }
        check(
            scope.producer == summary.producer
                && scope.parent == summary.parent
                && scope.seal == Some(summary.seal)
                && scope.declared == summary.declared,
            "checkpoint identity or membership commitment changed",
        )?;
        let mut counts = Counts {
            success: Number(0),
            failure: Number(0),
            cancelled: Number(0),
            skipped: Number(0),
        };
        let mut root = StatusRoot::default();
        let mut statement = connection
            .prepare("SELECT entity FROM scope_members WHERE scope=?1 ORDER BY entity")?;
        let mut rows = statement.query([scope.scope.0 as i64])?;
        while let Some(row) = rows.next()? {
            let work = WorkKey {
                scope: scope.scope,
                producer: scope.producer,
                entity: Id(row.get::<_, i64>(0)? as u64),
            };
            let member = self
                .read_member(connection, &work)?
                .ok_or(JournalError::Corrupt("checkpoint member disappeared"))?;
            self.validate_member(connection, &member)?;
            let observed = self
                .read_observed(connection, &work)?
                .ok_or_else(|| not_ready("terminal work view missing"))?;
            self.validate_view(connection, &observed)?;
            let view = observed.view;
            if !view.state.is_terminal() {
                return Err(not_ready("scope contains nonterminal work"));
            }
            check(
                view.terminal_at.expect("validated terminal") <= summary.closed_at,
                "closure precedes terminal member",
            )?;
            let bucket = match view.state {
                State::SUCCEEDED => &mut counts.success,
                State::FAILED => &mut counts.failure,
                State::CANCELLED => &mut counts.cancelled,
                State::SKIPPED => &mut counts.skipped,
                _ => unreachable!("validated terminal"),
            };
            bucket.0 += 1; // the independently bounded membership inventory is at most 1,000,000
            let child_status_root = if let Some(child) = &view.child {
                let covered = self
                    .read_coverage(connection, Number(child.scope.0))?
                    .ok_or_else(|| not_ready("descendant coverage missing"))?;
                check(
                    covered.parent.as_ref() == Some(&work) && covered.producer == child.producer,
                    "child coverage belongs to another parent or producer",
                )?;
                check(
                    covered.closed_at <= summary.closed_at,
                    "scope closed before its descendant",
                )?;
                if view.state == State::SUCCEEDED {
                    check(
                        covered.counts.success == covered.declared
                            && covered.closed_at <= view.terminal_at.expect("validated terminal"),
                        "successful parent violates strict child closure",
                    )?;
                }
                Some(covered.status_root)
            } else {
                None
            };
            root.push(
                StatusLeaf {
                    work,
                    state: view.state,
                    attempt: view.attempt,
                    manifest_digest: view.manifest.as_ref().map(Manifest::digest).transpose()?,
                    child_status_root,
                }
                .digest()?,
            )?;
        }
        check(
            counts == summary.counts
                && counts.total()? == scope.declared.0
                && root.finish() == summary.status_root,
            "checkpoint counts or status root disagree with terminal evidence",
        )
    }
    pub(in crate::v2::client::observations) fn audit_scopes(
        &self,
        connection: &Connection,
    ) -> Result<()> {
        for table in ["scopes", "scope_members", "scope_coverage"] {
            let count: i64 =
                connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
            check(
                count >= 0 && count as u64 <= self.limits.observations.0,
                "scope inventory exceeds retained ceiling",
            )?;
        }
        // Child scope IDs are strictly newer than their parents. Checking in
        // descending order validates the whole closure DAG without recursion or
        // a whole-tree buffer, including arbitrarily deep chains within quota.
        let mut statement = connection.prepare("SELECT scope FROM scopes ORDER BY scope DESC")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let scope = Number(row.get::<_, i64>(0)? as u64);
            let value = self
                .read_scope(connection, scope)?
                .ok_or(JournalError::Corrupt("scope disappeared"))?;
            self.check_scope_record(connection, &value)?;
            if let Some(summary) = self.read_coverage(connection, scope)? {
                self.validate_stored_coverage(connection, &summary)?;
            }
        }
        for table in ["scope_members", "scope_coverage"] {
            let orphan: bool = connection.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table} t LEFT JOIN scopes s ON s.scope=t.scope WHERE s.scope IS NULL)"), [], |r| r.get(0))?;
            check(!orphan, "orphan scope evidence")?;
        }
        let mut statement = connection.prepare("SELECT m.scope,s.producer,m.entity FROM scope_members m JOIN scopes s ON s.scope=m.scope")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let work = WorkKey {
                scope: Number(row.get::<_, i64>(0)? as u64),
                producer: Producer(row.get::<_, i64>(1)? as u64),
                entity: Id(row.get::<_, i64>(2)? as u64),
            };
            self.validate_member(
                connection,
                &self
                    .read_member(connection, &work)?
                    .ok_or(JournalError::Corrupt("member disappeared"))?,
            )?;
        }
        self.validate_scope_relations(connection)
    }
}
