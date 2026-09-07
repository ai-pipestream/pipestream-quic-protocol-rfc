//! Durable output space, independent of the transaction that admits a job.
//! A reservation is installed before its metadata reference. Until that commit
//! it is an orphan, never an admission receipt or a successful work outcome.

use super::*;

const BUDGET_MAGIC: &[u8; 8] = b"PSBUD004";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Funding {
    pub key: String,
    pub index: OutputIndex,
}
impl Wire for Funding {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 2)?;
        let key = d.str().map_err(codec::malformed)?;
        require(valid_key(key), "invalid output reservation identity")?;
        Ok(Self {
            key: key.to_owned(),
            index: OutputIndex::read(d)?,
        })
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(2);
        w.text(&self.key);
        self.index.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        require(valid_key(&self.key), "invalid output reservation identity")?;
        self.index.check()
    }
}

#[derive(Debug)]
pub(super) struct ReservationEntry {
    pub owner: IdentityLabel,
    pub budget: OutputBudget,
    pub live: usize,
    pub incomplete: bool,
}
impl Wire for ReservationEntry {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 2)?;
        Ok(Self {
            owner: IdentityLabel::read(d)?,
            budget: OutputBudget::read(d)?,
            live: 0,
            incomplete: false,
        })
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(2);
        self.owner.write(w);
        self.budget.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.owner.check()?;
        self.budget.check()
    }
}
impl ReservationEntry {
    pub(super) fn charge(&self) -> Result<u64> {
        self.budget
            .total_bytes
            .0
            .checked_add((self.budget.count.0 + 1) * OVERHEAD)
            .filter(|b| *b <= MAX_NUMBER)
            .ok_or_else(exhausted)
    }
}

fn exhausted() -> StoreError {
    protocol(
        ErrorCode::LimitExceeded,
        "output reservation capacity exhausted",
    )
}
pub(super) fn path(root: &Root, key: &str, incomplete: bool) -> PathBuf {
    root.path.join(format!(
        "{}-{key}",
        if incomplete { "funding" } else { "reserve" }
    ))
}

pub(super) fn read(path: &Path) -> Result<ReservationEntry> {
    let metadata = checked_file(path)?;
    if !(45..=HEADER_LIMIT as u64 + 44).contains(&metadata.len()) {
        return Err(StoreError::Corrupt(
            "output reservation file length invalid",
        ));
    }
    let mut storage = [0u8; HEADER_LIMIT + 44];
    let mut file = File::open(path)?;
    let bytes = &mut storage[..metadata.len() as usize];
    file.read_exact(bytes)?;
    let mut extra = [0u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(StoreError::Corrupt(
            "output reservation changed while reading",
        ));
    }
    let length = u32::from_be_bytes(bytes[8..12].try_into().expect("four bytes")) as usize;
    if &bytes[..8] != BUDGET_MAGIC
        || length == 0
        || length > HEADER_LIMIT
        || bytes.len() != length + 44
        || bytes.len() as u64 != metadata.len()
        || Sha256::digest(&bytes[..length + 12]).as_slice() != &bytes[length + 12..]
    {
        return Err(StoreError::Corrupt(
            "output reservation checksum or framing changed",
        ));
    }
    unpack(&bytes[12..12 + length])
}

fn totals(entries: &Inventory, owner: Option<&IdentityLabel>) -> Result<(u64, u64)> {
    let mut objects = 0u64;
    let mut bytes = 0u64;
    for entry in entries
        .values()
        .filter(|e| owner.is_none_or(|o| e.owner == *o))
    {
        objects += u64::from(entry.funding.is_none());
        bytes = bytes.checked_add(entry.charge()?).ok_or_else(exhausted)?;
    }
    for entry in entries
        .reservations
        .values()
        .filter(|e| owner.is_none_or(|o| e.owner == *o))
    {
        objects = objects
            .checked_add(entry.budget.count.0 + 1)
            .ok_or_else(exhausted)?;
        bytes = bytes.checked_add(entry.charge()?).ok_or_else(exhausted)?;
    }
    Ok((objects, bytes))
}

pub(super) fn check_capacity(
    policy: &PayloadPolicy,
    entries: &Inventory,
    owner: &IdentityLabel,
    objects: u64,
    bytes: u64,
) -> Result<()> {
    let global = totals(entries, None)?;
    let own = totals(entries, Some(owner))?;
    for (used, more, limit) in [
        (global.0, objects, policy.objects.0),
        (global.1, bytes, policy.bytes.0),
        (own.0, objects, policy.owner_objects.0),
        (own.1, bytes, policy.owner_bytes.0),
    ] {
        if used.checked_add(more).is_none_or(|v| v > limit) {
            return Err(exhausted());
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationUsage {
    pub outputs: u64,
    /// Includes each incomplete output's entire maximum, not just received bytes.
    pub allocated_bytes: u64,
}
fn used(entries: &Inventory, key: &str) -> Result<ReservationUsage> {
    let mut usage = ReservationUsage {
        outputs: 0,
        allocated_bytes: 0,
    };
    for entry in entries
        .values()
        .filter(|e| e.funding.as_ref().is_some_and(|f| f.key == key))
    {
        usage.outputs += 1;
        usage.allocated_bytes = usage
            .allocated_bytes
            .checked_add(entry.maximum.0)
            .ok_or_else(exhausted)?;
    }
    Ok(usage)
}

pub(super) fn audit(policy: &PayloadPolicy, entries: &Inventory) -> Result<()> {
    // Rebuild owner totals and per-reservation occupancy in one pass. Keys
    // borrow the inventory, and each output-index set is exactly 256 bits.
    let mut owners: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    let mut allocations: BTreeMap<&str, (u64, [u64; 4])> = BTreeMap::new();
    let mut global = (0u64, 0u64);
    fn add(total: &mut (u64, u64), objects: u64, bytes: u64) -> Result<()> {
        total.0 = total.0.checked_add(objects).ok_or_else(exhausted)?;
        total.1 = total.1.checked_add(bytes).ok_or_else(exhausted)?;
        Ok(())
    }
    for entry in entries.reservations.values() {
        let objects = entry.budget.count.0 + 1;
        let bytes = entry.charge()?;
        add(&mut global, objects, bytes)?;
        add(owners.entry(&entry.owner.0).or_default(), objects, bytes)?;
    }
    for entry in entries.values() {
        if let Some(funding) = &entry.funding {
            let reserve = entries
                .reservations
                .get(&funding.key)
                .ok_or(StoreError::Corrupt("output funding disappeared"))?;
            if reserve.owner != entry.owner || funding.index.0 >= reserve.budget.count.0 {
                return Err(StoreError::Corrupt("output funding identity mismatch"));
            }
            let allocated = allocations.entry(&funding.key).or_default();
            allocated.0 = allocated
                .0
                .checked_add(entry.maximum.0)
                .ok_or_else(exhausted)?;
            if allocated.0 > reserve.budget.total_bytes.0 {
                return Err(StoreError::Corrupt("retained outputs exceed reservation"));
            }
            let word = funding.index.0 as usize / 64;
            let bit = 1u64 << (funding.index.0 % 64);
            if allocated.1[word] & bit != 0 {
                return Err(StoreError::Corrupt("output slot appears twice"));
            }
            allocated.1[word] |= bit;
        } else {
            let bytes = entry.charge()?;
            add(&mut global, 1, bytes)?;
            add(owners.entry(&entry.owner.0).or_default(), 1, bytes)?;
        }
    }
    if global.0 > policy.objects.0
        || global.1 > policy.bytes.0
        || owners.values().any(|(objects, bytes)| {
            *objects > policy.owner_objects.0 || *bytes > policy.owner_bytes.0
        })
    {
        return Err(StoreError::Corrupt(
            "retained output funding exceeds policy",
        ));
    }
    Ok(())
}

fn key(entries: &Inventory) -> Result<String> {
    let mut random = [0; 16];
    SysRng
        .try_fill_bytes(&mut random)
        .map_err(|e| StoreError::Io(std::io::Error::other(e)))?;
    let key: String = random.iter().map(|b| format!("{b:02x}")).collect();
    if entries.contains_key(&key) || entries.reservations.contains_key(&key) {
        return Err(exhausted());
    }
    Ok(key)
}

struct Installation {
    store: PayloadStore,
    key: String,
    file: Option<File>,
}
impl Drop for Installation {
    fn drop(&mut self) {
        self.file.take();
        if self.key.is_empty() {
            return;
        }
        if let Ok(mut entries) = self.store.root.entries()
            && let Some(entry) = entries.reservations.get_mut(&self.key)
        {
            entry.live = entry.live.saturating_sub(1);
            if entry.incomplete
                && fs::remove_file(path(&self.store.root, &self.key, true)).is_ok()
                && sync(&self.store.root).is_ok()
            {
                entries.reservations.remove(&self.key);
            }
        }
    }
}

impl PayloadStore {
    /// Reserve maximum output count/bytes durably before a metadata admission.
    /// This is a quota promise and installed file evidence, not job acceptance.
    pub fn reserve_outputs(
        &self,
        owner: &IdentityLabel,
        budget: &OutputBudget,
    ) -> Result<OutputReservation> {
        owner.check()?;
        budget.check()?;
        let proposed = ReservationEntry {
            owner: owner.clone(),
            budget: budget.clone(),
            live: 1,
            incomplete: true,
        };
        let mut entries = self.root.entries()?;
        check_capacity(
            &self.root.policy,
            &entries,
            owner,
            budget.count.0 + 1,
            proposed.charge()?,
        )?;
        check_handles(&self.root.policy, &entries, owner)?;
        let key = key(&entries)?;
        let encoded = codec::encode(&proposed, HEADER_LIMIT)?;
        let file = new_staging_file(&self.root, &path(&self.root, &key, true))?;
        #[cfg(test)]
        tests::crash_point("reserve-created");
        entries.reservations.insert(key.clone(), proposed);
        drop(entries);
        let mut install = Installation {
            store: self.clone(),
            key: key.clone(),
            file: Some(file),
        };
        let mut bytes = Vec::with_capacity(44 + encoded.len());
        bytes.extend_from_slice(BUDGET_MAGIC);
        bytes.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&encoded);
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        let file = install.file.as_mut().expect("new reservation");
        file.write_all(&bytes).map_err(io)?;
        file.sync_all().map_err(io)?;
        #[cfg(test)]
        tests::crash_point("reserve-synced");
        let mut entries = self.root.entries()?;
        install_file(
            &self.root,
            &path(&self.root, &key, true),
            &path(&self.root, &key, false),
        )?;
        entries
            .reservations
            .get_mut(&key)
            .expect("installed reservation")
            .incomplete = false;
        #[cfg(test)]
        tests::crash_point("reserve-renamed");
        sync(&self.root)?;
        #[cfg(test)]
        tests::crash_point("reserve-directory-synced");
        install.file.take();
        install.key.clear();
        Ok(OutputReservation {
            store: self.clone(),
            key,
            owner: owner.clone(),
            budget: budget.clone(),
        })
    }

    /// Caller supplies the descriptor retained in its authenticated authority
    /// record. A filename alone cannot change owner or enlarge a reservation.
    pub fn open_reservation(
        &self,
        key: &str,
        owner: &IdentityLabel,
        budget: &OutputBudget,
    ) -> Result<OutputReservation> {
        owner.check()?;
        budget.check()?;
        let mut entries = self.root.entries()?;
        let entry = entries
            .reservations
            .get(key)
            .ok_or_else(|| protocol(ErrorCode::OutputUnavailable, "output reservation absent"))?;
        if entry.incomplete || entry.owner != *owner || entry.budget != *budget {
            return Err(protocol(
                ErrorCode::OutputUnavailable,
                "output reservation binding changed",
            ));
        }
        let actual = read(&path(&self.root, key, false))?;
        if actual.owner != *owner || actual.budget != *budget {
            return Err(StoreError::Corrupt("output reservation changed on disk"));
        }
        // An earlier installation may have returned an uncertain sync error
        // without a token. Complete durability before issuing new evidence.
        File::open(path(&self.root, key, false))?
            .sync_all()
            .map_err(io)?;
        sync(&self.root)?;
        check_handles(&self.root.policy, &entries, owner)?;
        entries
            .reservations
            .get_mut(key)
            .expect("checked reservation")
            .live += 1;
        Ok(OutputReservation {
            store: self.clone(),
            key: key.to_owned(),
            owner: owner.clone(),
            budget: budget.clone(),
        })
    }
}

/// Pins the reservation and its uncommitted output objects against collection.
/// The authority must separately fence attempt/worker publication and commit the
/// exact manifest. Holding this handle does not confer permission to execute.
pub struct OutputReservation {
    store: PayloadStore,
    key: String,
    owner: IdentityLabel,
    budget: OutputBudget,
}
impl OutputReservation {
    pub(crate) fn store(&self) -> &PayloadStore {
        &self.store
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn owner(&self) -> &IdentityLabel {
        &self.owner
    }
    pub fn budget(&self) -> &OutputBudget {
        &self.budget
    }
    pub fn binding(&self) -> StoreIdentity {
        self.store.binding()
    }
    pub fn usage(&self) -> Result<ReservationUsage> {
        let entries = self.store.root.entries()?;
        used(&entries, &self.key)
    }

    /// The length and hash may be unknown until the callback finishes. Reserve
    /// this output's maximum before writing; completion refunds unused capacity
    /// only inside this reservation, never to an unrelated uploader.
    pub fn stage(
        &self,
        index: OutputIndex,
        maximum: Number,
        content_type: ApplicationLabel,
        caps: &Capabilities,
        now: Instant,
    ) -> Result<OutputStaging> {
        index.check()?;
        maximum.check()?;
        content_type.check()?;
        caps.check()?;
        if index.0 >= self.budget.count.0 || maximum.0 > caps.object_limit.0 {
            return Err(exhausted());
        }
        let mut entries = self.store.root.entries()?;
        let retained = entries
            .reservations
            .get(&self.key)
            .ok_or(StoreError::Corrupt("live reservation disappeared"))?;
        if retained.incomplete || retained.owner != self.owner || retained.budget != self.budget {
            return Err(StoreError::Corrupt("live output reservation changed"));
        }
        let usage = used(&entries, &self.key)?;
        if usage.outputs >= self.budget.count.0
            || usage
                .allocated_bytes
                .checked_add(maximum.0)
                .is_none_or(|n| n > self.budget.total_bytes.0)
        {
            return Err(exhausted());
        }
        if entries.values().any(|e| {
            e.funding
                .as_ref()
                .is_some_and(|f| f.key == self.key && f.index == index)
        }) {
            return Err(protocol(
                ErrorCode::Conflict,
                "output slot already allocated",
            ));
        }
        check_handles(&self.store.root.policy, &entries, &self.owner)?;
        let key = key(&entries)?;
        let file = new_staging_file(&self.store.root, &self.store.root.path(&key, true))?;
        #[cfg(test)]
        tests::crash_point("output-created");
        entries.insert(
            key.clone(),
            Entry {
                owner: self.owner.clone(),
                input: None,
                maximum,
                funding: Some(Funding {
                    key: self.key.clone(),
                    index,
                }),
                offset: OVERHEAD,
                incomplete: true,
                live: 1,
            },
        );
        drop(entries);
        let mut stage = OutputStaging {
            store: self.store.clone(),
            key,
            file: Some(file),
            content_type,
            maximum,
            received: 0,
            hash: Sha256::new(),
            started: now,
            last_progress: now,
            idle: Elapsed::from_millis(caps.stream_idle_ms.0),
            lifetime: Elapsed::from_millis(caps.stream_lifetime_ms.0),
            failed: true,
        };
        // Uninstalled stages have no valid object descriptor. Installation
        // supplies the complete computed descriptor in this fixed header slot.
        stage
            .file
            .as_mut()
            .expect("new output")
            .write_all(&[0; OVERHEAD as usize])
            .map_err(io)?;
        #[cfg(test)]
        tests::crash_point("output-header-slot");
        stage.failed = false;
        Ok(stage)
    }
}
impl Drop for OutputReservation {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.store.root.entries()
            && let Some(entry) = entries.reservations.get_mut(&self.key)
        {
            entry.live = entry.live.saturating_sub(1);
        }
    }
}

pub struct OutputStaging {
    store: PayloadStore,
    key: String,
    file: Option<File>,
    content_type: ApplicationLabel,
    maximum: Number,
    received: u64,
    hash: Sha256,
    started: Instant,
    last_progress: Instant,
    idle: Elapsed,
    lifetime: Elapsed,
    failed: bool,
}
impl OutputStaging {
    pub fn check_deadline(&mut self, now: Instant) -> Result<()> {
        self.store.root.owned()?;
        if self.failed {
            return Err(protocol(
                ErrorCode::IntegrityError,
                "output staging already failed",
            ));
        }
        self.failed = true;
        let total = now
            .checked_duration_since(self.started)
            .ok_or_else(|| protocol(ErrorCode::ClockUnsafe, "output monotonic clock regressed"))?;
        let idle = now
            .checked_duration_since(self.last_progress)
            .ok_or_else(|| protocol(ErrorCode::ClockUnsafe, "output monotonic clock regressed"))?;
        if total >= self.lifetime || idle >= self.idle {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "output staging deadline reached",
            ));
        }
        self.failed = false;
        Ok(())
    }
    pub fn write(&mut self, bytes: &[u8], now: Instant) -> Result<()> {
        self.check_deadline(now)?;
        self.failed = true;
        if bytes.len() as u64 > self.store.root.policy.chunk_bytes.0
            || bytes.len() as u64 > self.maximum.0 - self.received
        {
            return Err(exhausted());
        }
        self.file
            .as_mut()
            .ok_or(StoreError::Corrupt("output stage closed"))?
            .write_all(bytes)
            .map_err(io)?;
        self.hash.update(bytes);
        self.received += bytes.len() as u64;
        #[cfg(test)]
        tests::crash_point("output-written");
        if !bytes.is_empty() {
            self.last_progress = now;
        }
        self.failed = false;
        Ok(())
    }
    pub fn finish(mut self, now: Instant) -> Result<InstalledPayload> {
        self.check_deadline(now)?;
        let mut entries = self.store.root.entries()?;
        let entry = entries
            .get_mut(&self.key)
            .ok_or(StoreError::Corrupt("output stage disappeared"))?;
        let input = Input {
            length: Number(self.received),
            sha256: Digest(self.hash.clone().finalize().into()),
            content_type: self.content_type.clone(),
        };
        let header = Header {
            owner: entry.owner.clone(),
            input: input.clone(),
            funding: entry.funding.clone(),
        };
        let encoded = codec::encode(&header, HEADER_LIMIT)?;
        let file = self
            .file
            .as_mut()
            .ok_or(StoreError::Corrupt("output stage closed"))?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(MAGIC).map_err(io)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())
            .map_err(io)?;
        file.write_all(&encoded).map_err(io)?;
        file.write_all(&[0; HEADER_LIMIT][..HEADER_LIMIT - encoded.len()])
            .map_err(io)?;
        file.sync_all().map_err(io)?;
        #[cfg(test)]
        tests::crash_point("output-synced");
        install_file(
            &self.store.root,
            &self.store.root.path(&self.key, true),
            &self.store.root.path(&self.key, false),
        )?;
        entry.input = Some(input.clone());
        entry.maximum = input.length;
        entry.incomplete = false;
        #[cfg(test)]
        tests::crash_point("output-renamed");
        sync(&self.store.root)?;
        #[cfg(test)]
        tests::crash_point("output-directory-synced");
        let installed = InstalledPayload {
            store: self.store.clone(),
            key: self.key.clone(),
            owner: entry.owner.clone(),
            input,
        };
        self.file.take();
        self.key.clear();
        Ok(installed)
    }
}
impl Drop for OutputStaging {
    fn drop(&mut self) {
        self.file.take();
        if self.key.is_empty() {
            return;
        }
        if let Ok(mut entries) = self.store.root.entries()
            && let Some(entry) = entries.get_mut(&self.key)
        {
            entry.live = entry.live.saturating_sub(1);
            if entry.incomplete
                && fs::remove_file(self.store.root.path(&self.key, true)).is_ok()
                && sync(&self.store.root).is_ok()
            {
                entries.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests;
