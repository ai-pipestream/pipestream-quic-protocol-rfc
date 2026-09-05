use super::spool::{PAYLOAD_IO_CHUNK, Payload, SpoolConnection};
use super::*;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub(super) enum Event {
    Control(u8, Vec<u8>),
    Entity(Box<Received>),
}

pub(super) struct Received {
    pub header: EntityHeader,
    pub body: Payload,
}

pub(super) fn start(
    connection: quinn::Connection,
    mut control: quinn::RecvStream,
    layers: LayerSupport,
    limits: RecursiveLimits,
    spool: SpoolConnection,
    deadlines: Arc<control::Deadlines>,
) -> (
    JoinSet<()>,
    mpsc::Receiver<Result<Event, ProtocolError>>,
    mpsc::Receiver<ProtocolError>,
) {
    // At most 32 complete events, each with a protocol-bounded frame or charged spool.
    // Control overflow refuses the connection instead of hiding later deadlines behind
    // a blocked send while storage is stalled.
    let (sender, receiver) = mpsc::channel(32);
    let (failures, failure) = mpsc::channel(1);
    let mut tasks = JoinSet::new();
    let controls = sender.clone();
    let control_connection = connection.clone();
    tasks.spawn(async move {
        loop {
            let (kind, bytes) = match read_control(&mut control).await {
                Ok(frame) => frame,
                Err(error) => {
                    if control_connection.close_reason().is_some() {
                        let _ = controls.send(Err(error)).await;
                    } else {
                        let _ = failures.send(error).await;
                    }
                    break;
                }
            };
            let validated = (|| {
                match kind {
                    FRAME_CAPABILITIES => return Err(frame_error("duplicate CAPABILITIES")),
                    FRAME_CHECKPOINT => {
                        deadlines.received(decode_checkpoint_for(&bytes, layers)?)?
                    }
                    FRAME_STATUS => {
                        let status = decode_status_frame(&bytes, layers)?;
                        if status.status.state == pipestream_core::STATUS_UNSPECIFIED
                            && status.status.entity_id == CONNECTION_LEVEL
                            && status.status.cursor.is_none()
                        {
                            return Ok(false);
                        }
                        if status.status.state != pipestream_core::STATUS_PENDING
                            || status.extension.is_some()
                        {
                            return Err(entity_error("entity announcement must be plain PENDING"));
                        }
                    }
                    FRAME_WORK_SET => {
                        work_set::decode(&bytes)?;
                    }
                    FRAME_SCOPE_DIGEST => {
                        decode_scope_digest(&bytes)?;
                    }
                    FRAME_BARRIER => {
                        decode_barrier(&bytes)?;
                    }
                    FRAME_RECOVERY => {
                        recovery::decode(&bytes)?;
                    }
                    FRAME_CLAIM_REDEMPTION => {
                        decode_claim_redemption(&bytes)?;
                    }
                    FRAME_GOAWAY => {
                        decode_goaway(&bytes)?;
                    }
                    _ => return Ok(false),
                }
                Ok(true)
            })();
            let error = match validated {
                Ok(false) => continue,
                Ok(true) => match controls.try_send(Ok(Event::Control(kind, bytes))) {
                    Ok(()) => continue,
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        limit_error("connection control backlog exhausted")
                    }
                },
                Err(error) => error,
            };
            let _ = failures.send(error).await;
            break;
        }
    });
    tasks.spawn(async move {
        let mut streams = JoinSet::new();
        loop {
            tokio::select! {
                result = streams.join_next(), if !streams.is_empty() => {
                    if let Some(Err(error)) = result {
                        let _ = sender.send(Err(frame_error(error))).await;
                        break;
                    }
                }
                accepted = connection.accept_uni(), if streams.len() < 8 => {
                    let stream = match accepted {
                        Ok(stream) => stream,
                        Err(error) => {
                            let _ = sender.send(Err(frame_error(error))).await;
                            break;
                        }
                    };
                    let sender = sender.clone();
                    let spool = spool.clone();
                    streams.spawn(async move {
                        let result = receive(stream, layers, limits, spool).await.map(|entity| Event::Entity(Box::new(entity)));
                        let _ = sender.send(result).await;
                    });
                }
            }
        }
    });
    (tasks, receiver, failure)
}

async fn receive(
    mut stream: quinn::RecvStream,
    layers: LayerSupport,
    limits: RecursiveLimits,
    spool: SpoolConnection,
) -> Result<Received, ProtocolError> {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).await.map_err(frame_error)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_ENTITY_HEADER {
        return Err(limit_error("entity header exceeds local limit"));
    }
    let mut encoded = vec![0; length];
    stream.read_exact(&mut encoded).await.map_err(frame_error)?;
    let header = pipestream_core::decode_entity_header_for(&encoded, layers)?;
    drop(encoded);
    if header
        .payload_length
        .is_some_and(|length| length > limits.max_entity_bytes as u64)
    {
        return Err(limit_error("entity payload exceeds local limit"));
    }
    if header
        .chunk_info
        .is_some_and(|info| info.total_chunks > limits.max_chunks_per_entity)
    {
        return Err(limit_error("chunk count exceeds local limit"));
    }
    let mut writer = spool.create().await?;
    while let Some(chunk) = stream
        .read_chunk(PAYLOAD_IO_CHUNK, true)
        .await
        .map_err(frame_error)?
    {
        if writer.len() + chunk.bytes.len() as u64 > limits.max_entity_bytes as u64 {
            return Err(limit_error("Entity Stream exceeds local payload limit"));
        }
        // Refuse rather than wait while incomplete entities hold disk credit.
        writer = writer.append(&chunk.bytes).await?;
    }
    let body = writer.finish().await?;
    pipestream_core::validate_entity_payload(&header, body.len(), body.digest())?;
    Ok(Received { header, body })
}

#[derive(Default)]
pub(super) struct Chunks {
    entities: BTreeMap<EntityKey, BTreeMap<u64, Received>>,
    bytes: u64,
}

impl Chunks {
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    pub fn insert(
        &mut self,
        received: Received,
        limits: RecursiveLimits,
    ) -> Result<Option<Assembly>, ProtocolError> {
        let Some(info) = received.header.chunk_info else {
            return Ok(Some(Assembly {
                header: received.header,
                parts: vec![received.body],
                chunked: false,
            }));
        };
        if info.total_chunks > limits.max_chunks_per_entity {
            return Err(limit_error("chunk count exceeds local limit"));
        }
        if self.bytes + received.body.len() > limits.max_entity_bytes as u64 {
            return Err(limit_error("aggregate chunk payload exceeds local limit"));
        }
        let key = EntityKey {
            scope_id: received.header.scope_id.unwrap_or(0),
            entity_id: received.header.entity_id,
        };
        let chunks = self.entities.entry(key).or_default();
        if let Some(first) = chunks.values().next()
            && (!same_chunk_identity(&first.header, &received.header)
                || first.header.chunk_info.unwrap().total_chunks != info.total_chunks)
        {
            return Err(entity_error(
                "entity identity or total-chunks changed between chunks",
            ));
        }
        if chunks.contains_key(&info.chunk_index) {
            return Err(entity_error("chunk-index is duplicated"));
        }
        self.bytes += received.body.len();
        chunks.insert(info.chunk_index, received);
        if chunks.len() as u64 != info.total_chunks {
            return Ok(None);
        }
        let mut ordered: Vec<_> = self.entities.remove(&key).unwrap().into_values().collect();
        ordered.sort_by_key(|chunk| chunk.header.chunk_info.unwrap().chunk_offset);
        let length: u64 = ordered.iter().map(|chunk| chunk.body.len()).sum();
        let mut offset = 0;
        for chunk in &ordered {
            if chunk.header.chunk_info.unwrap().chunk_offset != offset {
                return Err(entity_error(
                    "chunk ranges contain a gap, overlap, or duplicate offset",
                ));
            }
            offset += chunk.body.len();
        }
        self.bytes -= length;
        let mut ordered = ordered.into_iter();
        let result = ordered.next().unwrap();
        let mut parts = vec![result.body];
        parts.extend(ordered.map(|chunk| chunk.body));
        Ok(Some(Assembly {
            header: result.header,
            parts,
            chunked: true,
        }))
    }
}

pub(super) struct Assembly {
    pub header: EntityHeader,
    parts: Vec<Payload>,
    chunked: bool,
}

impl Assembly {
    /// Run in a bounded admission worker, never in the connection dispatch loop.
    pub fn finish(mut self) -> Result<Received, ProtocolError> {
        let body = if self.chunked {
            Payload::concatenate_blocking(self.parts)?
        } else {
            self.parts.pop().expect("unchunked entity has one body")
        };
        if self.chunked {
            self.header.chunk_info = None;
            self.header.payload_length = Some(body.len());
            self.header.checksum = Some(body.digest());
        }
        Ok(Received {
            header: self.header,
            body,
        })
    }
}
