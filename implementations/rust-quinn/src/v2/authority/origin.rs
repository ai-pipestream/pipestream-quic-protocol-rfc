//! Local producer authority is an opaque current-worker grant, never a wire flag.
use super::*;

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Origin {
    External,
    Worker {
        identity: SessionIdentity,
        parent: WorkKey,
        child: Id,
        attempt: Id,
        lease: Number,
    },
}
impl Origin {
    pub fn producer(&self) -> Producer {
        Producer(u64::from(matches!(self, Self::Worker { .. })))
    }
    pub fn permission(&self, external: Permission) -> Permission {
        if matches!(self, Self::External) {
            external
        } else {
            Permission::Execute
        }
    }
    pub fn check(
        &self,
        store: &AuthorityStore,
        tx: &Transaction<'_>,
        identity: &SessionIdentity,
        scope: Number,
    ) -> Result<()> {
        self.check_time(store, tx, identity, scope).map(|_| ())
    }
    /// Validate local ownership after metadata and authorization work, returning
    /// the same final UTC sample that must also satisfy a new child's deadline.
    /// External receipt observations do not need a clock or execution grant.
    pub fn check_time(
        &self,
        store: &AuthorityStore,
        tx: &Transaction<'_>,
        identity: &SessionIdentity,
        scope: Number,
    ) -> Result<Option<Number>> {
        let Self::Worker {
            identity: expected,
            parent,
            child,
            attempt,
            lease,
        } = self
        else {
            return Ok(None);
        };
        if identity != expected || scope.0 != child.0 {
            return Err(protocol(
                ErrorCode::Unauthorized,
                "worker producer grant targets another scope",
            ));
        }
        store.authorize_session(tx, identity, Permission::Execute)?;
        scopes::unfenced(tx, identity.generation, parent.scope)?;
        let (_, view) = scopes::work(tx, identity.generation, parent)?;
        if view.state.is_terminal() {
            return Err(protocol(ErrorCode::AlreadyTerminal, "parent is terminal"));
        }
        if view.state == State::CANCELLING {
            return Err(protocol(
                ErrorCode::Cancelled,
                "parent cancellation accepted",
            ));
        }
        let row: i64 = tx.query_row(
            "SELECT row_id FROM work WHERE generation=?1 AND scope=?2 AND entity=?3",
            params![
                sql(identity.generation.0)?,
                sql(parent.scope.0)?,
                sql(parent.entity.0)?
            ],
            |row| row.get(0),
        )?;
        let (_, job): (_, jobs::JobRecord) = records::read(
            tx,
            records::Target {
                table: records::Table::Job,
                row,
            },
        )?;
        let now = store.check_clock(tx)?;
        if view.deadline.is_some_and(|deadline| now >= deadline) {
            return Err(protocol(
                ErrorCode::DeadlineExceeded,
                "parent execution deadline reached",
            ));
        }
        if job.parameters.mode != Mode(2)
            || view.state != State::ACTIVE
            || view.child
                != Some(ChildScope {
                    scope: *child,
                    producer: Producer(1),
                })
            || job.attempt != *attempt
            || job.lease != *lease
            || job.stage != Number(1)
            || job.expansion_complete
            || job.lease_until.is_none_or(|until| now >= until)
        {
            return Err(protocol(
                ErrorCode::Conflict,
                "worker producer grant is stale",
            ));
        }
        Ok(Some(now))
    }
}
