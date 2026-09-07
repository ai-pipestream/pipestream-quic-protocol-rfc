use super::*;

impl Journal {
    pub async fn physical_usage(&self) -> Result<PhysicalUsage> {
        self.execute(Store::physical_usage).await
    }
    pub async fn binding(&self) -> Result<Option<Control>> {
        self.execute(Store::binding).await
    }
    pub async fn identity(&self) -> Result<SessionIdentity> {
        self.execute(Store::identity).await
    }
    /// Call only after actual TLS authentication and response correlation.
    pub async fn record_binding(&self, response: Control, selected: Capabilities) -> Result<()> {
        response.encode(MAX_CONTROL_LIMIT)?;
        self.creation().validate_selection(&selected)?;
        self.execute(move |store| store.record_binding(&response, &selected))
            .await
    }
    /// Await this commit before sending any bytes of a new mutation. A failed
    /// or cancelled wait grants no permission to substitute another identity.
    pub async fn prepare(&self, intent: Intent) -> Result<()> {
        intent.mutation.validate()?;
        self.execute(move |store| store.prepare(&intent)).await
    }
    pub async fn intent(&self, operation: OperationId) -> Result<Intent> {
        self.execute(move |store| store.intent(operation)).await
    }
    pub async fn record_receipt(&self, receipt: OperationReceipt) -> Result<()> {
        // The wire wrapper validates variable-length diagnostic fields without
        // cloning unvalidated caller input. Its request ID is not retained.
        let frame = Control::Work(Work::OperationResponse {
            request: Id(1),
            receipt,
        });
        frame.encode(MAX_CONTROL_LIMIT)?;
        self.execute(move |store| {
            let Control::Work(Work::OperationResponse { receipt, .. }) = frame else {
                unreachable!()
            };
            store.record_receipt(&receipt)
        })
        .await
    }
    pub async fn receipt(&self, operation: OperationId) -> Result<Option<OperationReceipt>> {
        self.execute(move |store| store.receipt(operation)).await
    }
    /// At most 256 bounded intent records. Returned values belong to the caller,
    /// not an internal history accumulated by this worker.
    pub async fn unresolved(&self, after: Number, limit: PageLimit) -> Result<Vec<(Id, Intent)>> {
        self.execute(move |store| store.unresolved(after, limit))
            .await
    }
    pub async fn observe_work(&self, revision: Id, view: WorkView) -> Result<ObservedWork> {
        view.validate_profiles(self.creation().results)?;
        self.execute(move |store| store.observe_work(revision, &view))
            .await
    }
    pub async fn observed_work(&self, work: WorkKey) -> Result<Option<ObservedWork>> {
        self.execute(move |store| store.observed_work(&work)).await
    }
    pub async fn remember_manifest(&self, manifest: Manifest) -> Result<()> {
        manifest.encode()?;
        self.execute(move |store| store.remember_manifest(&manifest))
            .await
    }
    pub async fn remember_reference(
        &self,
        manifest: Manifest,
        index: OutputIndex,
    ) -> Result<RetainedReference> {
        manifest.encode()?;
        self.execute(move |store| store.remember_reference(&manifest, index))
            .await
    }
    pub async fn retained_reference(
        &self,
        work: WorkKey,
        attempt: Id,
        index: OutputIndex,
    ) -> Result<RetainedReference> {
        self.execute(move |store| store.retained_reference(&work, attempt, index))
            .await
    }
    pub async fn observe_scope_page(
        &self,
        request: Control,
        response: Control,
    ) -> Result<ScopeObservation> {
        request.encode(MAX_CONTROL_LIMIT)?;
        response.encode(MAX_CONTROL_LIMIT)?;
        self.execute(move |store| store.observe_scope_page(&request, &response))
            .await
    }
    pub async fn scope_observation(&self, scope: Number) -> Result<Option<ScopeObservation>> {
        self.execute(move |store| store.scope_observation(scope))
            .await
    }
    pub async fn scope_members(
        &self,
        scope: Number,
        after: Number,
        limit: PageLimit,
    ) -> Result<Vec<ScopeMember>> {
        self.execute(move |store| store.scope_members(scope, after, limit))
            .await
    }
    pub async fn record_checkpoint(&self, summary: ScopeSummary) -> Result<()> {
        summary.encode()?;
        self.execute(move |store| store.record_checkpoint(&summary))
            .await
    }
    pub async fn covered_scope(&self, scope: Number) -> Result<Option<ScopeSummary>> {
        self.execute(move |store| store.covered_scope(scope)).await
    }
    /// The exact saved root cut only. The network connection must independently
    /// drain its requests/transfers and validate the echoed DRAIN response.
    pub async fn root_completion(&self, request: Id) -> Result<Control> {
        self.execute(move |store| store.root_completion(request))
            .await
    }
}
