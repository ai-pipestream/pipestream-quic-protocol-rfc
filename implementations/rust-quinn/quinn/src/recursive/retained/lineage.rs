//! A reservation commits to ownership and capacity, not to an invented digest.

use super::*;

// Marker + final digest + final metadata + receipt + publication stage.
// Retaining this complete allowance after publication avoids reclaim races.
pub(super) const CHARGE: u64 = RECORD_BYTES as u64 * 2 + RECEIPT_BYTES + 32 + 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Reservation {
    session: String,
    pub(super) owner: Owner,
}

impl Reservation {
    fn new(principal: Option<&PrincipalBinding>, session: &str) -> io::Result<Self> {
        validate_storage_session_id(session)?;
        let owner = match principal {
            Some(p) => {
                PrincipalBinding::new(&p.authority, &p.principal).map_err(io::Error::other)?;
                Some((p.authority.clone(), p.principal.clone()))
            }
            None => None,
        };
        Ok(Self {
            session: session.to_owned(),
            owner,
        })
    }

    fn path(&self, root: &Path) -> PathBuf {
        root.join(&self.session).join("lineage.reserve")
    }

    fn encode(&self) -> [u8; RECORD_BYTES] {
        let mut bytes = [0; RECORD_BYTES];
        bytes[..8].copy_from_slice(b"PSLIN001");
        bytes[8] = u8::from(self.owner.is_some());
        let (authority, principal) = self
            .owner
            .as_ref()
            .map(|(authority, principal)| (authority.as_str(), principal.as_str()))
            .unwrap_or(("", ""));
        for (slot, text) in
            bytes[9..396]
                .chunks_exact_mut(129)
                .zip([self.session.as_str(), authority, principal])
        {
            slot[0] = text.len() as u8;
            slot[1..1 + text.len()].copy_from_slice(text.as_bytes());
        }
        let checksum = Sha256::digest(&bytes[..480]);
        bytes[480..].copy_from_slice(&checksum);
        bytes
    }

    fn read(path: &Path) -> io::Result<Self> {
        let bytes = read_fixed::<RECORD_BYTES>(path)?;
        let mut strings = Vec::with_capacity(3);
        for slot in bytes[9..396].chunks_exact(129) {
            let length = slot[0] as usize;
            if length > 128 {
                return Err(corrupt("lineage reservation identity exceeds bound"));
            }
            strings.push(
                std::str::from_utf8(&slot[1..1 + length])
                    .map_err(|_| corrupt("invalid lineage reservation identity"))?,
            );
        }
        let principal = if bytes[8] == 1 {
            Some(PrincipalBinding::new(strings[1], strings[2]).map_err(io::Error::other)?)
        } else {
            None
        };
        let reservation = Self::new(principal.as_ref(), strings[0])?;
        if reservation.encode() != bytes {
            return Err(corrupt("lineage reservation checksum or encoding mismatch"));
        }
        Ok(reservation)
    }
}

impl State {
    fn insert_lineage(
        &mut self,
        reservation: Reservation,
        limits: RetainedLimits,
    ) -> io::Result<()> {
        if self
            .owners
            .get(&reservation.session)
            .is_some_and(|owner| owner != &reservation.owner)
        {
            return Err(io::Error::other(unauthorized()));
        }
        let prior = self
            .principals
            .get(&reservation.owner)
            .copied()
            .unwrap_or_default();
        if self.usage.bytes + CHARGE > limits.bytes
            || self.usage.objects >= limits.objects
            || prior.bytes + CHARGE > limits.principal_bytes
            || prior.objects >= limits.principal_objects
            || (!self.principals.contains_key(&reservation.owner)
                && self.principals.len() as u64 >= limits.principals)
        {
            return Err(limit("final-lineage reservation budget exhausted"));
        }
        let principal = self
            .principals
            .entry(reservation.owner.clone())
            .or_default();
        for usage in [&mut self.usage, principal] {
            usage.bytes += CHARGE;
            usage.objects += 1;
            usage.lineage_reservations += 1;
        }
        self.owners
            .insert(reservation.session.clone(), reservation.owner.clone());
        self.lineages
            .insert(reservation.session.clone(), reservation);
        Ok(())
    }
}

impl RetainedRoot {
    pub(crate) fn reserve_lineage(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
        session: &str,
    ) -> io::Result<()> {
        let reservation = Reservation::new(principal, session)?;
        self.verify_policy()?;
        let key = (session.to_owned(), None);
        let path = reservation.path(&self.path);
        let parent = path
            .parent()
            .ok_or_else(|| corrupt("lineage reservation lacks parent"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("retained state poisoned"))?;
        if state.durable_lineages.contains(session) {
            drop(state);
            return self.verify_lineage(principal, session);
        }
        if state.active.len() as u64 >= self.limits.staging_objects || state.active.contains(&key) {
            return Err(limit("lineage reservation already has an active writer"));
        }
        if !state.directories.contains(parent)
            && state.directories.len() as u64 >= 2 * self.limits.objects
        {
            return Err(limit("lineage directory budget exhausted"));
        }
        if let Some(prefix) = state.incomplete_lineages.get(session)
            && !reservation.encode().starts_with(prefix)
        {
            return Err(corrupt(
                "lineage reservation retry differs from durable prefix",
            ));
        }
        if let Some(existing) = state.lineages.get(session) {
            if existing != &reservation {
                return Err(io::Error::other(unauthorized()));
            }
        } else {
            let incomplete = state.incomplete_lineages.contains_key(session);
            if incomplete {
                state.usage.bytes -= CHARGE;
                state.usage.objects -= 1;
                state.usage.lineage_reservations -= 1;
                state.usage.incomplete_lineage_reservations -= 1;
            }
            if let Err(error) = state.insert_lineage(reservation.clone(), self.limits) {
                if incomplete {
                    state.usage.bytes += CHARGE;
                    state.usage.objects += 1;
                    state.usage.lineage_reservations += 1;
                    state.usage.incomplete_lineage_reservations += 1;
                }
                return Err(error);
            }
            state.incomplete_lineages.remove(session);
        }
        state.directories.insert(parent.to_owned());
        state.usage.directories = state.directories.len() as u64;
        state.active.insert(key.clone());
        drop(state);
        let _operation = ReservationOperation {
            root: self.clone(),
            key,
        };
        fs::create_dir_all(parent)?;
        write_prefix(&path, &reservation.encode())?;
        sync_ancestors(parent, &self.path)?;
        self.state
            .lock()
            .map_err(|_| corrupt("retained state poisoned"))?
            .durable_lineages
            .insert(session.to_owned());
        Ok(())
    }

    pub(crate) fn verify_lineage(
        &self,
        principal: Option<&PrincipalBinding>,
        session: &str,
    ) -> io::Result<()> {
        let expected = Reservation::new(principal, session)?;
        self.verify_policy()?;
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("retained state poisoned"))?;
        let retained = state
            .lineages
            .get(session)
            .ok_or_else(|| corrupt("session has no final-lineage reservation"))?;
        if retained != &expected {
            return Err(io::Error::other(unauthorized()));
        }
        drop(state);
        if Reservation::read(&expected.path(&self.path))? != expected {
            return Err(corrupt("lineage reservation changed"));
        }
        Ok(())
    }

    pub(crate) fn require_lineage_reservations(&self) -> io::Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("retained state poisoned"))?;
        for entry in state.entries.values() {
            if state
                .lineages
                .get(&entry.record.key.0)
                .is_none_or(|reservation| reservation.owner != entry.record.owner)
            {
                return Err(corrupt(
                    "retained payload lacks its admission lineage reservation",
                ));
            }
        }
        Ok(())
    }
}

struct ReservationOperation {
    root: Arc<RetainedRoot>,
    key: Key,
}
impl Drop for ReservationOperation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.root.state.lock() {
            state.active.remove(&self.key);
        }
    }
}

pub(super) fn scan(
    root: &Path,
    limits: RetainedLimits,
    files: &BTreeSet<PathBuf>,
    state: &mut State,
    accounted: &mut BTreeSet<PathBuf>,
) -> io::Result<()> {
    for path in files.iter().filter(|path| {
        path.file_name()
            .is_some_and(|name| name == "lineage.reserve")
    }) {
        let session = path
            .parent()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str())
            .ok_or_else(|| corrupt("lineage reservation has invalid path"))?;
        validate_storage_session_id(session)?;
        if path != &root.join(session).join("lineage.reserve") {
            return Err(corrupt("lineage reservation outside session directory"));
        }
        let length =
            regular_length(path, 1)?.ok_or_else(|| corrupt("lineage reservation disappeared"))?;
        if length < RECORD_BYTES as u64 {
            if state.usage.bytes + CHARGE > limits.bytes || state.usage.objects >= limits.objects {
                return Err(limit("incomplete lineage reservation exceeds global quota"));
            }
            let mut prefix = vec![0; length as usize];
            File::open(path)?.read_exact(&mut prefix)?;
            state.incomplete_lineages.insert(session.to_owned(), prefix);
            state.usage.bytes += CHARGE;
            state.usage.objects += 1;
            state.usage.lineage_reservations += 1;
            state.usage.incomplete_lineage_reservations += 1;
        } else {
            let reservation = Reservation::read(path)?;
            if reservation.session != session {
                return Err(corrupt("lineage reservation path identity mismatch"));
            }
            state.insert_lineage(reservation, limits)?;
            // A complete prefix can survive a writer that failed before fsync.
            File::open(path)?.sync_all()?;
            sync_ancestors(path.parent().expect("session path checked"), root)?;
            state.durable_lineages.insert(session.to_owned());
        }
        accounted.insert(path.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
