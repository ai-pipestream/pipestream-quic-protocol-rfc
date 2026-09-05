use super::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;

pub(super) enum Event {
    Control(u8, Vec<u8>),
    Entity(Box<Received>),
}

pub(super) struct Received {
    pub header: EntityHeader,
    pub body: Vec<u8>,
    _credit: OwnedSemaphorePermit,
}

pub(super) fn start(
    connection: quinn::Connection,
    mut control: quinn::RecvStream,
    layers: LayerSupport,
    limits: RecursiveLimits,
) -> (JoinSet<()>, mpsc::Receiver<Result<Event, ProtocolError>>) {
    let (sender, receiver) = mpsc::channel(4);
    let mut tasks = JoinSet::new();
    let controls = sender.clone();
    tasks.spawn(async move {
        loop {
            let result = read_control(&mut control)
                .await
                .map(|(kind, bytes)| Event::Control(kind, bytes));
            let failed = result.is_err();
            if controls.send(result).await.is_err() || failed {
                break;
            }
        }
    });
    tasks.spawn(async move {
        let budget = Arc::new(Semaphore::new(limits.max_entity_bytes + 8 * (MAX_ENTITY_HEADER + 4)));
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
                    let budget = Arc::clone(&budget);
                    streams.spawn(async move {
                        let result = receive(stream, layers, limits, budget).await.map(|entity| Event::Entity(Box::new(entity)));
                        let _ = sender.send(result).await;
                    });
                }
            }
        }
    });
    (tasks, receiver)
}

async fn receive(
    mut stream: quinn::RecvStream,
    layers: LayerSupport,
    limits: RecursiveLimits,
    budget: Arc<Semaphore>,
) -> Result<Received, ProtocolError> {
    let mut credit = Arc::clone(&budget)
        .try_acquire_many_owned(0)
        .map_err(limit_error)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.read_chunk(8192, true).await.map_err(frame_error)? {
        if bytes.len() + chunk.bytes.len() > limits.max_entity_bytes + MAX_ENTITY_HEADER + 4 {
            return Err(limit_error("Entity Stream exceeds local payload limit"));
        }
        // Refuse, rather than wait: incomplete entities must not deadlock each other
        // while holding all of the connection's receive budget.
        credit.merge(
            Arc::clone(&budget)
                .try_acquire_many_owned(chunk.bytes.len() as u32)
                .map_err(|_| limit_error("connection receive byte budget exhausted"))?,
        );
        bytes.extend_from_slice(&chunk.bytes);
    }
    let (header, payload) = decode_entity_for(&bytes, layers)?;
    let length = payload.len();
    if length > limits.max_entity_bytes {
        return Err(limit_error("entity payload exceeds local limit"));
    }
    let payload_start = bytes.len() - length;
    bytes.copy_within(payload_start.., 0);
    bytes.truncate(length);
    Ok(Received {
        header,
        body: bytes,
        _credit: credit,
    })
}

#[derive(Default)]
pub(super) struct Chunks {
    entities: BTreeMap<EntityKey, BTreeMap<u64, Received>>,
    bytes: usize,
}

impl Chunks {
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    pub fn insert(
        &mut self,
        received: Received,
        limits: RecursiveLimits,
    ) -> Result<Option<Received>, ProtocolError> {
        let Some(info) = received.header.chunk_info else {
            return Ok(Some(received));
        };
        if info.total_chunks > limits.max_chunks_per_entity {
            return Err(limit_error("chunk count exceeds local limit"));
        }
        if self.bytes + received.body.len() > limits.max_entity_bytes {
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
        let length: usize = ordered.iter().map(|chunk| chunk.body.len()).sum();
        let mut offset = 0;
        for chunk in &ordered {
            if chunk.header.chunk_info.unwrap().chunk_offset != offset as u64 {
                return Err(entity_error(
                    "chunk ranges contain a gap, overlap, or duplicate offset",
                ));
            }
            offset += chunk.body.len();
        }
        self.bytes -= length;
        let mut ordered = ordered.into_iter();
        let mut result = ordered.next().unwrap();
        result.body.reserve(length - result.body.len());
        for chunk in ordered {
            result._credit.merge(chunk._credit);
            result.body.extend_from_slice(&chunk.body);
        }
        result.header.chunk_info = None;
        result.header.payload_length = Some(length as u64);
        result.header.checksum = Some(Sha256::digest(&result.body).into());
        Ok(Some(result))
    }
}
