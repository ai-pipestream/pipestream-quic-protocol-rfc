use super::*;

impl Client {
    /// Persist immutable intent before sending. A refusal, lost reply or local
    /// receipt-write failure leaves that same intent available for recovery.
    pub async fn mutate(&self, intent: Intent) -> Result<OperationReceipt> {
        intent.control(Id(1))?;
        self.operation(move |context, _| {
            Box::pin(async move {
                context
                    .store(context.journal.prepare(intent.clone()))
                    .await?;
                let response = context.control(intent.control(Id(1))?).await?;
                context.receipt(response).await
            })
        })
        .await
    }
    /// Lookup only a locally persisted original operation. NOT_FOUND does not
    /// authorize a replacement identity or prove that an earlier request cannot
    /// still commit at the authority.
    pub async fn recover_operation(&self, operation: OperationId) -> Result<OperationReceipt> {
        self.operation(move |context, _| {
            Box::pin(async move {
                context.store(context.journal.intent(operation)).await?;
                let response = context
                    .control(Control::Work(Work::Operation {
                        request: Id(1),
                        operation,
                    }))
                    .await?;
                context.receipt(response).await
            })
        })
        .await
    }
    pub async fn intent(&self, operation: OperationId) -> Result<Intent> {
        self.operation(move |context, _| {
            Box::pin(async move { context.store(context.journal.intent(operation)).await })
        })
        .await
    }
    pub async fn receipt(&self, operation: OperationId) -> Result<Option<OperationReceipt>> {
        self.operation(move |context, _| {
            Box::pin(async move { context.store(context.journal.receipt(operation)).await })
        })
        .await
    }
    pub async fn unresolved(&self, after: Number, limit: PageLimit) -> Result<Vec<(Id, Intent)>> {
        self.operation(move |context, _| {
            Box::pin(async move {
                context
                    .store(context.journal.unresolved(after, limit))
                    .await
            })
        })
        .await
    }
    pub async fn watch(
        &self,
        work: WorkKey,
        after_revision: Number,
        wait_ms: WaitMs,
    ) -> Result<ObservedWork> {
        let request = Control::Work(Work::Watch {
            request: Id(1),
            work,
            after_revision,
            wait_ms,
        });
        request.encode(MAX_CONTROL_LIMIT)?;
        self.operation(move |context, _| {
            Box::pin(async move {
                let Control::Work(Work::View { revision, work, .. }) =
                    context.control(request).await?
                else {
                    return Err(error(ErrorCode::FrameError, "expected work observation"));
                };
                context
                    .store(context.journal.observe_work(revision, *work))
                    .await
            })
        })
        .await
    }
    pub async fn observed_work(&self, work: WorkKey) -> Result<Option<ObservedWork>> {
        self.operation(move |context, _| {
            Box::pin(async move { context.store(context.journal.observed_work(work)).await })
        })
        .await
    }
    pub async fn scope_page(
        &self,
        scope: Number,
        after_entity: Number,
        limit: PageLimit,
    ) -> Result<ScopeObservation> {
        let request = Control::Scope(Scope::Page {
            request: Id(1),
            scope,
            after_entity,
            limit,
        });
        request.encode(MAX_CONTROL_LIMIT)?;
        self.operation(move |context, _| {
            Box::pin(async move {
                let response = context.control(request).await?;
                let actual = request_id(&response)
                    .ok_or_else(|| error(ErrorCode::FrameError, "uncorrelated scope page"))?;
                let request = Control::Scope(Scope::Page {
                    request: actual,
                    scope,
                    after_entity,
                    limit,
                });
                context
                    .store(context.journal.observe_scope_page(request, response))
                    .await
            })
        })
        .await
    }
    pub async fn scope_members(
        &self,
        scope: Number,
        after: Number,
        limit: PageLimit,
    ) -> Result<Vec<super::super::journal::ScopeMember>> {
        self.operation(move |context, _| {
            Box::pin(async move {
                context
                    .store(context.journal.scope_members(scope, after, limit))
                    .await
            })
        })
        .await
    }
    pub async fn checkpoint(
        &self,
        scope: Number,
        seal: Digest,
        wait_ms: WaitMs,
    ) -> Result<ScopeSummary> {
        let request = Control::Scope(Scope::Checkpoint {
            request: Id(1),
            scope,
            seal,
            wait_ms,
        });
        request.encode(MAX_CONTROL_LIMIT)?;
        self.operation(move |context, _| {
            Box::pin(async move {
                let Control::Scope(Scope::CheckpointResponse { summary, .. }) =
                    context.control(request).await?
                else {
                    return Err(error(ErrorCode::FrameError, "expected scope checkpoint"));
                };
                context
                    .store(context.journal.record_checkpoint(summary.clone()))
                    .await?;
                Ok(summary)
            })
        })
        .await
    }
    pub async fn covered_scope(&self, scope: Number) -> Result<Option<ScopeSummary>> {
        self.operation(move |context, _| {
            Box::pin(async move { context.store(context.journal.covered_scope(scope)).await })
        })
        .await
    }
    /// Retain the full authenticated manifest, including a legitimate empty
    /// output list. This does not select an output or renew its availability.
    pub async fn manifest(&self, work: WorkKey, attempt: Id) -> Result<Manifest> {
        let request = Control::Result(ResultMessage::GetManifest {
            request: Id(1),
            work,
            attempt,
        });
        request.encode(MAX_CONTROL_LIMIT)?;
        self.operation(move |context, _| {
            Box::pin(async move {
                let Control::Result(ResultMessage::ManifestResponse { manifest, .. }) =
                    context.control(request).await?
                else {
                    return Err(error(ErrorCode::FrameError, "expected result manifest"));
                };
                context
                    .store(context.journal.remember_manifest(manifest.clone()))
                    .await?;
                Ok(manifest)
            })
        })
        .await
    }
    /// Select and durably retain an authenticated manifest/index before reading
    /// it. No URL-derived identity, credentials, redirects or implicit rerun.
    pub async fn select_output(
        &self,
        work: WorkKey,
        attempt: Id,
        index: OutputIndex,
    ) -> Result<RetainedReference> {
        let request = Control::Result(ResultMessage::GetManifest {
            request: Id(1),
            work,
            attempt,
        });
        request.encode(MAX_CONTROL_LIMIT)?;
        if index.0 > 255 {
            return Err(error(ErrorCode::FrameError, "invalid output index"));
        }
        self.operation(move |context, _| {
            Box::pin(async move {
                let Control::Result(ResultMessage::ManifestResponse { manifest, .. }) =
                    context.control(request).await?
                else {
                    return Err(error(ErrorCode::FrameError, "expected result manifest"));
                };
                context
                    .store(context.journal.remember_reference(manifest, index))
                    .await
            })
        })
        .await
    }
    pub async fn retained_reference(
        &self,
        work: WorkKey,
        attempt: Id,
        index: OutputIndex,
    ) -> Result<RetainedReference> {
        self.operation(move |context, _| {
            Box::pin(async move {
                context
                    .store(context.journal.retained_reference(work, attempt, index))
                    .await
            })
        })
        .await
    }
    /// Barrier over already accepted facade operations, followed by the server's
    /// actual connection cut. A successful acknowledgment closes this client.
    /// A refusal reopens acceptance without inventing completion or coverage.
    pub async fn complete(&self) -> Result<ScopeSummary> {
        let Control::Drain(Drain::Completed { root_summary, .. }) = self.cut(true).await? else {
            return Err(error(
                ErrorCode::FrameError,
                "expected exact completed-session cut",
            ));
        };
        Ok(root_summary)
    }
    /// Connection-only barrier. This never asserts durable work completion.
    pub async fn detach(&self) -> Result<()> {
        if !matches!(
            self.cut(false).await?,
            Control::Drain(Drain::Detached { .. })
        ) {
            return Err(error(
                ErrorCode::FrameError,
                "expected connection detach acknowledgment",
            ));
        }
        Ok(())
    }
    async fn cut(&self, complete: bool) -> Result<Control> {
        let ticket = self.ticket()?;
        let inner = self.0.clone();
        let context = inner.context.clone();
        let (reply, receive) = oneshot::channel();
        let task = Box::pin(async move {
            let value = async {
                let request = if complete {
                    context
                        .store(context.journal.root_completion(Id(1)))
                        .await?
                } else {
                    Control::Drain(Drain::Detach { request: Id(1) })
                };
                context.control(request).await
            }
            .await;
            {
                let mut accept = inner
                    .accept
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                accept.barrier = false;
                if value.is_ok() {
                    accept.sender.take();
                }
            }
            let _ = reply.send(Packet {
                value,
                _ticket: ticket,
            });
        });
        {
            let mut accept =
                self.0.accept.lock().map_err(|_| {
                    error(ErrorCode::InternalError, "durable client acceptance lock")
                })?;
            if accept.barrier {
                return Err(error(ErrorCode::NotReady, "durable client drain barrier"));
            }
            accept
                .sender
                .as_ref()
                .ok_or_else(closed)?
                .try_send(Job {
                    task,
                    barrier: true,
                })
                .map_err(|_| closed())?;
            accept.barrier = true;
        }
        receive
            .await
            .map_err(|_| error(ErrorCode::InternalError, "durable cut owner failed"))?
            .value
    }
}
