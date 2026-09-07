use super::*;

pub(super) struct CreditEntry {
    pub owner: IdentityLabel,
    pub busy: bool,
}

/// Reusable reader capacity reserved before a branch worker claim. A reader
/// retains the credit if it outlives its worker; unrelated opens cannot steal it.
pub(in crate::v2::authority) struct ReadCredit {
    store: PayloadStore,
    id: u64,
}
impl PayloadStore {
    pub(in crate::v2::authority) fn reserve_reader(
        &self,
        owner: &IdentityLabel,
    ) -> Result<Arc<ReadCredit>> {
        let mut entries = self.root.entries()?;
        check_handles(&self.root.policy, &entries, owner)?;
        let id = entries
            .next_reader
            .checked_add(1)
            .ok_or_else(|| protocol(ErrorCode::LimitExceeded, "reader credit counter exhausted"))?;
        entries.next_reader = id;
        entries.readers.insert(
            id,
            CreditEntry {
                owner: owner.clone(),
                busy: false,
            },
        );
        Ok(Arc::new(ReadCredit {
            store: self.clone(),
            id,
        }))
    }
}
impl ReadCredit {
    pub fn open_output(
        self: &Arc<Self>,
        reservation: &str,
        index: OutputIndex,
        owner: &IdentityLabel,
        descriptor: &Input,
    ) -> Result<ObjectReader> {
        self.store
            .open_output_with(reservation, index, owner, descriptor, Some(self))
    }
    pub(super) fn check(&self, entries: &Inventory, owner: &IdentityLabel) -> Result<()> {
        let entry = entries
            .readers
            .get(&self.id)
            .ok_or(StoreError::Corrupt("reader credit disappeared"))?;
        if entry.owner != *owner {
            return Err(protocol(
                ErrorCode::Unauthorized,
                "reader credit owner changed",
            ));
        }
        if entry.busy {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "reader credit already in use",
            ));
        }
        Ok(())
    }
    pub(super) fn set_busy(&self, entries: &mut Inventory, busy: bool) {
        entries
            .readers
            .get_mut(&self.id)
            .expect("retained reader credit")
            .busy = busy;
    }
}
impl Drop for ReadCredit {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.store.root.entries() {
            entries.readers.remove(&self.id);
        }
    }
}
