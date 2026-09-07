//! Local result copies have no authority reservations or execution references.
//! Only the client result-store wrapper exposes these operations.
use super::*;

impl PayloadStore {
    pub(crate) fn verify_local_owner(&self, owner: &IdentityLabel) -> Result<()> {
        let entries = self.root.entries()?;
        if !entries.reservations.is_empty()
            || entries
                .values()
                .any(|e| e.owner != *owner || e.funding.is_some())
        {
            return Err(StoreError::Corrupt(
                "local result inventory has foreign ownership",
            ));
        }
        Ok(())
    }

    pub(crate) fn find_local(
        &self,
        owner: &IdentityLabel,
        input: &Input,
    ) -> Result<Option<String>> {
        let entries = self.root.entries()?;
        Ok(entries.iter().find_map(|(key, entry)| {
            (!entry.incomplete
                && entry.funding.is_none()
                && entry.owner == *owner
                && entry.input.as_ref() == Some(input))
            .then(|| key.clone())
        }))
    }

    pub(crate) fn remove_local(&self, owner: &IdentityLabel, key: &str) -> Result<bool> {
        if !valid_key(key) {
            return Err(protocol(ErrorCode::Conflict, "invalid local result key"));
        }
        let mut entries = self.root.entries()?;
        let Some(entry) = entries.get(key) else {
            return Ok(false);
        };
        if entry.owner != *owner || entry.funding.is_some() || entry.incomplete || entry.live != 0 {
            return Err(protocol(
                ErrorCode::Conflict,
                "local result is foreign or still owned",
            ));
        }
        let path = self.root.path(key, false);
        match checked_file(&path) {
            Ok(_) => fs::remove_file(path)?,
            Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        #[cfg(test)]
        crate::v2::client::results::tests::crash_point("removed");
        sync(&self.root)?;
        entries.remove(key);
        Ok(true)
    }
}
