use super::*;

impl Pending {
    async fn blocking<T: Send + 'static>(
        &self,
        run: impl FnOnce(&Shared) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = self
            .ticket
            .shared
            .authority
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| error(ErrorCode::LimitExceeded, "metadata concurrency exhausted"))?;
        self.blocking_with(permit, run).await
    }

    async fn blocking_with<T: Send + 'static>(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
        run: impl FnOnce(&Shared) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let ticket = self.ticket.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            ticket.shared.authorize()?;
            run(&ticket.shared)
        })
        .await
        .map_err(|_| error(ErrorCode::InternalError, "metadata task failed"))?
    }

    /// Database errors become bounded diagnostics, never success. Detach expiry
    /// is fatal: the endpoint must close without a detached/completed assertion.
    pub async fn run(self) -> Result<Response, Error> {
        let request = request_id(&self.message).expect("submission checked request");
        let body = match self.execute().await {
            Ok(body) => body,
            Err(error) if matches!(self.ticket.kind, Kind::Detach) => return Err(error),
            Err(error) => ResponseBody::Control(refusal(request, error)),
        };
        let body = match body {
            ResponseBody::Control(control) => {
                match control.encode(self.ticket.shared.caps.control_limit.0 as usize) {
                    Ok(_) => ResponseBody::Control(control),
                    Err(_) => ResponseBody::Control(refusal(
                        request,
                        error(ErrorCode::LimitExceeded, "response exceeds control limit"),
                    )),
                }
            }
            body => body,
        };
        Ok(Response {
            body,
            _ticket: self.ticket,
        })
    }

    async fn execute(&self) -> Result<ResponseBody, Error> {
        let shared = &self.ticket.shared;
        if let Control::Drain(Drain::Detach { request }) = self.message {
            let deadline = self.accepted + Elapsed::from_millis(shared.caps.stream_lifetime_ms.0);
            loop {
                // Register before inspecting state so the final completion
                // cannot be lost between inspection and waiting.
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if Instant::now() >= deadline {
                    return Err(error(ErrorCode::LimitExceeded, "detach deadline"));
                }
                if shared.state()?.pending == 1 {
                    return Ok(ResponseBody::Control(Control::Drain(Drain::Detached {
                        request,
                    })));
                }
                tokio::time::timeout_at(deadline, changed)
                    .await
                    .map_err(|_| error(ErrorCode::LimitExceeded, "detach deadline"))?;
            }
        }
        if let Control::Session(session) = &self.message {
            let session = session.clone();
            return self
                .blocking(move |shared| {
                    let store = &shared.authority.store;
                    let binding = match session {
                        Session::NextSequence { request } => {
                            return Ok(ResponseBody::Control(Control::Session(
                                Session::Sequence {
                                    request,
                                    next_creation_sequence: store
                                        .next_creation(&shared.identity.owner)
                                        .map_err(storage)?,
                                },
                            )));
                        }
                        Session::Create {
                            request,
                            creation_sequence,
                            policy,
                        } => (
                            request,
                            store
                                .create_session(
                                    &shared.identity.owner,
                                    creation_sequence,
                                    &policy,
                                    &shared.caps,
                                )
                                .map_err(storage)?,
                        ),
                        Session::Attach {
                            request,
                            authority,
                            owner,
                            generation,
                        } => (
                            request,
                            store
                                .attach_session(
                                    &shared.identity.owner,
                                    &SessionIdentity {
                                        authority,
                                        owner,
                                        generation,
                                    },
                                    &shared.caps,
                                )
                                .map_err(storage)?,
                        ),
                        _ => {
                            return Err(error(
                                ErrorCode::FrameError,
                                "unexpected session response",
                            ));
                        }
                    };
                    let response = binding.1.response(binding.0);
                    shared.state()?.binding = Some(binding.1);
                    Ok(ResponseBody::Control(response))
                })
                .await;
        }
        let identity = self
            .binding
            .as_ref()
            .ok_or_else(|| error(ErrorCode::NotReady, "session not attached"))?
            .identity
            .clone();
        let wait = match &self.message {
            Control::Work(Work::Watch { wait_ms, .. })
            | Control::Scope(Scope::Checkpoint { wait_ms, .. }) => wait_ms.0,
            _ => 0,
        };
        let deadline = self.accepted + Elapsed::from_millis(wait);
        let message = self.message.clone();
        let first_identity = identity.clone();
        let mut response = self
            .blocking(move |shared| dispatch(shared, &first_identity, message))
            .await?;
        loop {
            let ready = match (&self.message, &response) {
                (
                    Control::Work(Work::Watch { after_revision, .. }),
                    Some(ResponseBody::Control(Control::Work(Work::View { revision, .. }))),
                ) => after_revision.0 == 0 || revision.0 != after_revision.0,
                (_, Some(_)) => true,
                (_, None) => false,
            };
            if ready || Instant::now() >= deadline {
                return response
                    .ok_or_else(|| error(ErrorCode::WaitTimeout, "checkpoint wait expired"));
            }
            // No SQLite transaction, metadata permit or thread is held here.
            tokio::time::sleep_until(deadline.min(Instant::now() + Elapsed::from_millis(20))).await;
            // This is an existing bounded waiter, not a new operation. Join the
            // fair semaphore queue so a mutation using the slot does not turn
            // a revision/checkpoint wait into an unrelated capacity refusal.
            // A timeout returns the last consistent snapshot, never a fabricated
            // new revision or checkpoint summary. Requests waiting on this
            // semaphore still hold their connection's pending slot.
            let permit = match tokio::time::timeout_at(
                deadline,
                shared.authority.slots.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err(error(ErrorCode::InternalError, "metadata pool closed")),
                Err(_) => {
                    shared.authorize()?;
                    return response
                        .ok_or_else(|| error(ErrorCode::WaitTimeout, "checkpoint wait expired"));
                }
            };
            let message = self.message.clone();
            let identity = identity.clone();
            response = self
                .blocking_with(permit, move |shared| dispatch(shared, &identity, message))
                .await?;
        }
    }
}

fn dispatch(
    shared: &Shared,
    identity: &SessionIdentity,
    message: Control,
) -> Result<Option<ResponseBody>, Error> {
    let store = &shared.authority.store;
    let control = match message {
        Control::Scope(scope) => Control::Scope(match scope {
            Scope::Declare {
                request,
                operation,
                scope,
                entity_ids,
                seal,
            } => Scope::Declared {
                request,
                receipt: store
                    .declare(identity, operation, scope, &entity_ids, seal)
                    .map_err(storage)?,
            },
            Scope::Page {
                request,
                scope,
                after_entity,
                limit,
            } => {
                return store
                    .scope_page(identity, request, scope, after_entity, limit)
                    .map(|control| Some(ResponseBody::Control(control)))
                    .map_err(storage);
            }
            Scope::Checkpoint {
                request,
                scope,
                seal,
                ..
            } => {
                let Some(summary) = store.checkpoint(identity, scope, seal).map_err(storage)?
                else {
                    return Ok(None);
                };
                Scope::CheckpointResponse { request, summary }
            }
            Scope::Cancel {
                request,
                operation,
                scope,
            } => Scope::Cancelled {
                request,
                receipt: store
                    .cancel_scope(identity, operation, scope)
                    .map_err(storage)?,
            },
            _ => return Err(error(ErrorCode::FrameError, "unexpected scope response")),
        }),
        Control::Work(work) => Control::Work(match work {
            Work::Operation { request, operation } => Work::OperationResponse {
                request,
                receipt: store.operation(identity, operation).map_err(storage)?,
            },
            Work::Watch {
                request,
                work,
                after_revision,
                ..
            } => {
                let (revision, work) = store
                    .work_view(identity, &work, after_revision)
                    .map_err(storage)?;
                Work::View {
                    request,
                    revision,
                    work: Box::new(work),
                }
            }
            Work::Retry {
                request,
                operation,
                work,
                expected_attempt,
            } => Work::Retried {
                request,
                receipt: store
                    .retry_work(identity, operation, &work, expected_attempt)
                    .map_err(storage)?,
            },
            Work::Cancel {
                request,
                operation,
                work,
            } => Work::Cancelled {
                request,
                receipt: store
                    .cancel_work(identity, operation, &work)
                    .map_err(storage)?,
            },
            Work::Skip {
                request,
                operation,
                work,
            } => Work::Skipped {
                request,
                receipt: store
                    .skip_work(identity, operation, &work)
                    .map_err(storage)?,
            },
            _ => return Err(error(ErrorCode::FrameError, "unexpected work response")),
        }),
        Control::Result(message @ ResultMessage::Read { .. }) => {
            return shared
                .authority
                .results
                .begin_read(identity, &message, &shared.caps, std::time::Instant::now())
                .map(|read| Some(ResponseBody::Result(read)))
                .map_err(storage);
        }
        Control::Result(ResultMessage::GetManifest {
            request,
            work,
            attempt,
        }) => Control::Result(ResultMessage::ManifestResponse {
            request,
            manifest: shared
                .authority
                .results
                .manifest(identity, &work, attempt, &shared.caps)
                .map_err(storage)?,
        }),
        Control::Drain(Drain::Complete {
            request,
            generation,
            root_summary,
        }) => {
            store
                .complete_session(identity, generation, &root_summary)
                .map_err(storage)?;
            Control::Drain(Drain::Completed {
                request,
                generation,
                root_summary,
            })
        }
        _ => return Err(error(ErrorCode::FrameError, "unexpected durable request")),
    };
    Ok(Some(ResponseBody::Control(control)))
}
