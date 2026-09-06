use super::{codec::*, *};

record!(Capabilities { response: ResponseFlag, supported: Vec<ProfileId>, required: Vec<ProfileId>, control_limit: ControlLimit, stream_limit: ConcurrencyLimit, pending_limit: ConcurrencyLimit, object_limit: Number, stream_idle_ms: IdleMs, stream_lifetime_ms: LifetimeMs } |s| {
    for list in [&s.supported, &s.required] {
        require(list.len() <= 32 && list.windows(2).all(|p| p[0] < p[1]), "profile list not sorted unique/bounded")?;
    }
    require(s.required.iter().all(|id| s.supported.contains(id)), "required profile not supported")?;
    require(s.stream_idle_ms.0 <= s.stream_lifetime_ms.0, "idle exceeds lifetime")?;
    if s.has(RESULT_DELIVERY) && !s.has(DURABLE_WORK) {
        return Err(Error::new(ErrorCode::ExtensionUnsupported, "result delivery requires durable work"));
    }
    require(s.response.0 == 0 || !s.supported.iter().any(|p| (65281..=65283).contains(&p.0)), "legacy profile selected on V2")
});

impl Capabilities {
    pub fn has(&self, profile: u16) -> bool {
        self.supported.contains(&ProfileId(u64::from(profile)))
    }

    /// Compute selection from the caller offer and this implementation's enabled
    /// inventory. `authenticated_owner` is an already verified TLS/policy result,
    /// not a fingerprint check. This helper does not enable endpoint profiles.
    pub fn select(offer: &Self, enabled: &Self, authenticated_owner: bool) -> Result<Self, Error> {
        offer.check()?;
        enabled.check()?;
        require(
            offer.response.0 == 0 && enabled.response.0 == 0,
            "selection needs two offers",
        )?;
        let mut required = offer.required.clone();
        required.extend_from_slice(&enabled.required);
        required.sort();
        required.dedup();
        if !authenticated_owner
            && required
                .iter()
                .any(|p| p.0 == u64::from(DURABLE_WORK) || p.0 == u64::from(RESULT_DELIVERY))
        {
            return Err(Error::new(
                ErrorCode::Unauthorized,
                "required durable profile lacks authenticated owner",
            ));
        }
        // Only contracts this implementation understands can be selected, even
        // if a caller accidentally includes unknown IDs in its enabled inventory.
        let supported: Vec<_> = offer
            .supported
            .iter()
            .copied()
            .filter(|id| {
                authenticated_owner
                    && [u64::from(DURABLE_WORK), u64::from(RESULT_DELIVERY)].contains(&id.0)
                    && enabled.supported.contains(id)
            })
            .collect();
        if required.iter().any(|id| !supported.contains(id)) {
            return Err(Error::new(
                ErrorCode::ExtensionUnsupported,
                "required profile unavailable",
            ));
        }
        let selected = Self {
            response: ResponseFlag(1),
            supported,
            required,
            control_limit: offer.control_limit.min(enabled.control_limit),
            stream_limit: offer.stream_limit.min(enabled.stream_limit),
            pending_limit: offer.pending_limit.min(enabled.pending_limit),
            object_limit: offer.object_limit.min(enabled.object_limit),
            stream_idle_ms: offer.stream_idle_ms.min(enabled.stream_idle_ms),
            stream_lifetime_ms: offer.stream_lifetime_ms.min(enabled.stream_lifetime_ms),
        };
        selected.check()?;
        Ok(selected)
    }

    /// Validate the response using only what the client actually knows. The
    /// server's private inventory is not available for recomputing intersection.
    pub fn validate_selection(&self, response: &Self) -> Result<(), Error> {
        self.check()?;
        response.check()?;
        require(
            self.response.0 == 0 && response.response.0 == 1,
            "wrong selection direction",
        )?;
        require(
            response
                .supported
                .iter()
                .all(|id| self.supported.contains(id))
                && self
                    .required
                    .iter()
                    .all(|id| response.required.contains(id)),
            "unsolicited selection or required omission",
        )?;
        require(
            response.control_limit <= self.control_limit
                && response.stream_limit <= self.stream_limit
                && response.pending_limit <= self.pending_limit
                && response.object_limit <= self.object_limit
                && response.stream_idle_ms <= self.stream_idle_ms
                && response.stream_lifetime_ms <= self.stream_lifetime_ms,
            "selected limit increased",
        )
    }
}
