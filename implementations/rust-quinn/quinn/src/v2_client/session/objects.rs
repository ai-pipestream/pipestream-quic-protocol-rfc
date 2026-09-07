use super::*;

pub struct Admission {
    writer: transport::InputWriter,
    receipt: oneshot::Receiver<Packet<OperationReceipt>>,
}
impl Admission {
    pub fn stream_id(&self) -> StreamId {
        self.writer.stream_id()
    }
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        Ok(self.writer.write(bytes).await?)
    }
    /// Local FIN only. Await `receipt` for a durably recorded admission result.
    pub async fn finish(&mut self) -> Result<()> {
        Ok(self.writer.finish().await?)
    }
    /// A successful return includes completed journal validation and commit.
    /// Dropping this waiter does not stop the independently owned collector.
    pub async fn receipt(self) -> Result<OperationReceipt> {
        self.receipt
            .await
            .map_err(|_| error(ErrorCode::InternalError, "admission collector failed"))?
            .value
    }
    /// Stop unfinished transmission, then await the still-owned admission
    /// collector. A header replay or racing commit can return a valid receipt.
    pub async fn abort(self) -> Result<OperationReceipt> {
        let Self { writer, receipt } = self;
        drop(writer);
        receipt
            .await
            .map_err(|_| error(ErrorCode::InternalError, "admission collector failed"))?
            .value
    }
}
pub struct Output {
    inner: transport::Output,
    _ticket: Ticket,
}
impl Output {
    pub(super) fn into_parts(self) -> (transport::Output, Ticket) {
        (self.inner, self._ticket)
    }
    pub fn header(&self) -> &ResultHeader {
        self.inner.header()
    }
    pub async fn read_unverified(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.read_unverified().await?)
    }
    pub fn verification(&self) -> Option<&transport::VerifiedObject> {
        self.inner.verification()
    }
}
impl Client {
    /// Require an already recorded declaration receipt covering this entity,
    /// persist admission intent, then open the stream. The response collector
    /// starts before the writer is returned and outlives its caller's waiter.
    pub async fn input(&self, intent: Intent, declaration: OperationId) -> Result<Admission> {
        intent.input(self.identity().generation)?;
        let ticket = self.ticket()?;
        let context = self.0.context.clone();
        let (ready, receive) = oneshot::channel();
        let (receipt, collected) = oneshot::channel();
        self.submit(Box::pin(async move {
            let opened = async {
                let Mutation::Admit(parameters) = &intent.mutation else { unreachable!() };
                let covering = context.store(context.journal.intent(declaration)).await?;
                let covers = matches!(&covering.mutation, Mutation::Declare { scope, entity_ids, .. }
                    if *scope == parameters.work.scope && parameters.work.producer.0 == 0 && entity_ids.contains(&parameters.work.entity));
                if !covers || context.store(context.journal.receipt(declaration)).await?.is_none() {
                    return Err(error(ErrorCode::NotReady, "input needs a covering durable declaration receipt"));
                }
                context.store(context.journal.prepare(intent.clone())).await?;
                Ok(context.transport.input(intent.input(context.identity.generation)?).await?.split())
            }.await;
            match opened {
                Ok((writer, response)) => {
                    let _ = ready.send(Packet { value: Ok(Admission { writer, receipt: collected }), _ticket: ticket.clone() });
                    let value = match response.receive().await {
                        Ok(response) => context.receipt(response).await,
                        Err(e) => Err(e.into()),
                    };
                    let _ = receipt.send(Packet { value, _ticket: ticket });
                },
                Err(e) => { let _ = ready.send(Packet { value: Err(e), _ticket: ticket }); },
            }
        }))?;
        receive
            .await
            .map_err(|_| error(ErrorCode::InternalError, "input preparation owner failed"))?
            .value
    }
    /// Read only an explicit, previously persisted manifest/index selection.
    /// Availability and authorization are checked afresh by the authority.
    pub async fn read_output(
        &self,
        work: WorkKey,
        attempt: Id,
        index: OutputIndex,
    ) -> Result<Output> {
        self.operation(move |context, ticket| {
            Box::pin(async move {
                let reference = context
                    .store(context.journal.retained_reference(work, attempt, index))
                    .await?;
                match context
                    .transport
                    .exchange(reference.read(Id(1))?, Some(reference.manifest()))
                    .await?
                {
                    transport::Reply::Object(inner) => Ok(Output {
                        inner,
                        _ticket: ticket,
                    }),
                    transport::Reply::Control(Control::Refusal(r)) => Err(Failure::Refused(r)),
                    _ => Err(error(ErrorCode::FrameError, "expected result stream")),
                }
            })
        })
        .await
    }
}
