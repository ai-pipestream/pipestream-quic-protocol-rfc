//! Connection-local checkpoint clocks run independently of storage and output I/O.

use super::*;
use std::sync::Mutex;
use tokio::{sync::Notify, time::Instant};

#[derive(Default)]
pub(super) struct Deadlines {
    pending: Mutex<Pending>,
    changed: Notify,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(sequence: u64, timeout: u64) -> Checkpoint {
        Checkpoint {
            checkpoint_id: "cut".into(),
            sequence_number: sequence,
            checkpoint_entity_id: 2,
            scope_id: None,
            flags: 0,
            timeout_ms: Some(timeout),
        }
    }

    #[tokio::test]
    async fn queued_duplicates_keep_their_clock_after_an_earlier_ack() {
        let clocks = Deadlines::default();
        clocks.received(request(1, 20)).unwrap();
        clocks.received(request(1, 20)).unwrap();
        clocks.acknowledged((0, 1), 1).unwrap();
        assert_eq!(clocks.pending.lock().unwrap().count, 1);
        assert_eq!(clocks.expired().await.code, 0x0e);
        assert_eq!(clocks.acknowledged((0, 1), 1).unwrap_err().code, 0x0e);
    }

    #[tokio::test]
    async fn acknowledged_clock_is_removed_and_new_shorter_clock_wakes_waiter() {
        let clocks = Arc::new(Deadlines::default());
        clocks.received(request(1, 5000)).unwrap();
        clocks.acknowledged((0, 1), 1).unwrap();
        let waiting = clocks.clone();
        let waiter = tokio::spawn(async move { waiting.expired().await });
        clocks.received(request(2, 20)).unwrap();
        let error = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(error.code, 0x0e);
    }

    #[test]
    fn duplicate_requests_consume_budget_and_cannot_extend_the_initial_deadline() {
        let clocks = Deadlines::default();
        clocks.received(request(1, 5000)).unwrap();
        let initial = clocks.pending.lock().unwrap().requests[&(0, 1)].arrivals[0];
        assert_eq!(
            clocks.received(request(1, 6000)).unwrap_err().code,
            ERROR_ENTITY_INVALID
        );
        for _ in 1..1024 {
            clocks.received(request(1, 5000)).unwrap();
        }
        assert_eq!(
            clocks.received(request(1, 5000)).unwrap_err().code,
            ERROR_LIMIT_EXCEEDED
        );
        assert_eq!(
            clocks.pending.lock().unwrap().requests[&(0, 1)].arrivals[0],
            initial
        );
        clocks.acknowledged((0, 1), 1024).unwrap();
        assert_eq!(clocks.pending.lock().unwrap().count, 0);
    }
}

#[derive(Default)]
struct Pending {
    requests: BTreeMap<(u32, u64), RequestClock>,
    count: usize,
}

struct RequestClock {
    request: Checkpoint,
    arrivals: VecDeque<Instant>,
}

pub(super) fn timeout_error() -> ProtocolError {
    ProtocolError::new(
        0x0e,
        "PIPESTREAM_CHECKPOINT_TIMEOUT",
        "checkpoint deadline expired",
    )
}

impl Deadlines {
    pub(super) fn received(&self, request: Checkpoint) -> Result<(), ProtocolError> {
        let key = (request.scope_id.unwrap_or(0), request.sequence_number);
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(request.timeout_ms.unwrap_or(30_000)))
            .ok_or_else(|| frame_error("checkpoint timeout overflows clock"))?;
        let mut pending = self.pending.lock().map_err(storage_error)?;
        if let Some(previous) = pending.requests.get(&key)
            && previous.request != request
        {
            return Err(entity_error("checkpoint request changed while pending"));
        }
        if pending.count >= 1024 {
            return Err(limit_error("pending checkpoint limit exhausted"));
        }
        pending
            .requests
            .entry(key)
            .or_insert_with(|| RequestClock {
                request,
                arrivals: VecDeque::new(),
            })
            .arrivals
            .push_back(deadline);
        pending.count += 1;
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn check(&self, key: (u32, u64)) -> Result<(), ProtocolError> {
        let pending = self.pending.lock().map_err(storage_error)?;
        let deadline = pending
            .requests
            .get(&key)
            .and_then(|clock| clock.arrivals.front())
            .ok_or_else(|| entity_error("checkpoint clock is absent"))?;
        if Instant::now() >= *deadline {
            return Err(timeout_error());
        }
        Ok(())
    }

    pub(super) fn acknowledged(&self, key: (u32, u64), count: usize) -> Result<(), ProtocolError> {
        self.check(key)?;
        let mut pending = self.pending.lock().map_err(storage_error)?;
        let requests = pending
            .requests
            .get_mut(&key)
            .ok_or_else(|| entity_error("checkpoint clock is absent"))?;
        if count > requests.arrivals.len() {
            return Err(entity_error("checkpoint clock count differs"));
        }
        requests.arrivals.drain(..count);
        if requests.arrivals.is_empty() {
            pending.requests.remove(&key);
        }
        pending.count -= count;
        self.changed.notify_one();
        Ok(())
    }

    pub(super) async fn expired(&self) -> ProtocolError {
        loop {
            let changed = self.changed.notified();
            let deadline = match self.pending.lock() {
                Ok(pending) => pending
                    .requests
                    .values()
                    .filter_map(|clock| clock.arrivals.front().copied())
                    .min(),
                Err(error) => return storage_error(error),
            };
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    return timeout_error();
                }
                tokio::select! {
                    biased;
                    _ = changed => {},
                    _ = tokio::time::sleep_until(deadline) => return timeout_error(),
                }
            } else {
                changed.await;
            }
        }
    }
}
