//! Connection-owned send admission and receive-window geometry for V2.
//! Stream priority orders packets but cannot reserve connection credit by itself.
use pipestream_core::v2::{Error, ErrorCode};
use std::{
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

fn error(code: ErrorCode, detail: &'static str) -> Error {
    Error { code, detail }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum unacknowledged bytes admitted by data writers, including any
    /// already outstanding control bytes. Control alone can use the extra space.
    pub data_send: u64,
    pub control_send: u64,
    /// Uniform receive window per stream. Receive credit also reserves a control
    /// window and headroom for the transport's batched connection-credit updates.
    pub receive_stream: u32,
    pub data_streams: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            data_send: 65536,
            control_send: 65536,
            receive_stream: 65536,
            data_streams: 4,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<(), Error> {
        if !(1..=8 * 1024 * 1024).contains(&self.data_send)
            || !(1..=8 * 1024 * 1024).contains(&self.control_send)
            || !(1024..=1024 * 1024).contains(&self.receive_stream)
            || self.data_streams > 128
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid V2 flow-control limits",
            ));
        }
        Ok(())
    }
    /// Raw receive credit, not a process-memory or native-allocation measurement.
    pub fn receive_budget(&self) -> Result<u64, Error> {
        self.validate()?;
        let windows = u64::from(self.receive_stream) * (u64::from(self.data_streams) + 1);
        // Pinned quinn-proto 0.11.17 withholds MAX_DATA until consumed bytes
        // reach R/8, independently of stream-credit updates. Keep R-R/8 at
        // least N*W+W so replacement data cannot spend the control reservation.
        // Recheck this bound against the actual transport when upgrading Quinn.
        Ok((windows * 8).div_ceil(7))
    }
    /// Apply before the handshake. Both peers need compatible receive geometry;
    /// local send admission cannot manufacture credit an arbitrary peer withholds.
    pub fn configure(
        &self,
        transport: &mut quinn::TransportConfig,
        side: quinn::Side,
    ) -> Result<(), Error> {
        let receive = self.receive_budget()?;
        transport
            // Only the client opens a bidirectional stream. Its control receive
            // half still gets W, without granting the server another bidi window.
            .max_concurrent_bidi_streams(u32::from(side == quinn::Side::Server).into())
            .max_concurrent_uni_streams(self.data_streams.into())
            .stream_receive_window(self.receive_stream.into())
            .receive_window(receive.try_into().expect("bounded receive budget"))
            .send_window(self.data_send);
        Ok(())
    }
}

struct Shared {
    connection: quinn::Connection,
    limits: Limits,
    admission: Mutex<()>,
    #[cfg(test)]
    blocked_data: std::sync::atomic::AtomicU64,
}
/// Own all outgoing stream writes for this connection through these wrappers.
/// Construct once, before opening streams. Do not change the underlying send
/// window or write through another raw stream handle while this owner is active.
#[derive(Clone)]
pub struct Connection {
    shared: Arc<Shared>,
}
impl Connection {
    pub fn new(connection: quinn::Connection, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        connection.set_send_window(limits.data_send);
        Ok(Self {
            shared: Arc::new(Shared {
                connection,
                limits,
                admission: Mutex::new(()),
                #[cfg(test)]
                blocked_data: std::sync::atomic::AtomicU64::new(0),
            }),
        })
    }
    pub(crate) fn belongs_to(&self, connection: &quinn::Connection) -> bool {
        self.shared.connection.stable_id() == connection.stable_id()
    }
    /// Test-only observation of actual pending QUIC writes, not a simulated stall.
    #[cfg(test)]
    pub(crate) fn blocked_data_polls(&self) -> u64 {
        self.shared
            .blocked_data
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub async fn open_data(&self) -> Result<Writer, Error> {
        let stream = self
            .shared
            .connection
            .open_uni()
            .await
            .map_err(|_| error(ErrorCode::Cancelled, "data stream could not open"))?;
        self.writer(stream, false)
    }
    pub async fn open_control(&self) -> Result<(Writer, quinn::RecvStream), Error> {
        if self.shared.connection.side() != quinn::Side::Client {
            return Err(error(
                ErrorCode::FrameError,
                "only a client opens Control Stream 0",
            ));
        }
        let (send, recv) = self
            .shared
            .connection
            .open_bi()
            .await
            .map_err(|_| error(ErrorCode::ControlReset, "control stream could not open"))?;
        Ok((self.writer(send, true)?, recv))
    }
    pub async fn accept_control(&self) -> Result<(Writer, quinn::RecvStream), Error> {
        if self.shared.connection.side() != quinn::Side::Server {
            return Err(error(
                ErrorCode::FrameError,
                "only a server accepts Control Stream 0",
            ));
        }
        let (send, recv) = self.shared.connection.accept_bi().await.map_err(|_| {
            error(
                ErrorCode::ControlReset,
                "control stream could not be accepted",
            )
        })?;
        Ok((self.writer(send, true)?, recv))
    }
    fn writer(&self, mut stream: quinn::SendStream, control: bool) -> Result<Writer, Error> {
        if control && u64::from(stream.id()) != 0 {
            let _ = stream.reset(
                ErrorCode::FrameError
                    .quic_error()
                    .try_into()
                    .expect("fixed error"),
            );
            return Err(error(ErrorCode::FrameError, "control must use Stream 0"));
        }
        stream
            .set_priority(if control { i32::MAX } else { 0 })
            .map_err(|_| error(ErrorCode::Cancelled, "stream closed during setup"))?;
        Ok(Writer {
            stream,
            shared: self.shared.clone(),
            control,
            retry: None,
        })
    }
}

/// A stream opened by its flow owner, not an unrelated caller-supplied handle.
/// Dropping preserves Quinn's FIN behavior; use `reset` on aborted object sends.
pub struct Writer {
    stream: quinn::SendStream,
    shared: Arc<Shared>,
    control: bool,
    retry: Option<Pin<Box<tokio::time::Sleep>>>,
}
struct Restore<'a>(&'a Shared);
impl Drop for Restore<'_> {
    fn drop(&mut self) {
        self.0.connection.set_send_window(self.0.limits.data_send);
    }
}
impl Shared {
    fn poll(
        &self,
        control: bool,
        run: impl FnOnce() -> Poll<Result<usize, quinn::WriteError>>,
    ) -> Poll<Result<usize, Error>> {
        let _admission = match self.admission.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return Poll::Ready(Err(error(
                    ErrorCode::InternalError,
                    "send admission lock poisoned",
                )));
            }
        };
        // No await, filesystem I/O or user callback occurs under this mutex.
        // Data cannot poll while the temporary control-only allowance is active.
        let _restore = control.then(|| {
            self.connection
                .set_send_window(self.limits.data_send + self.limits.control_send);
            Restore(self)
        });
        run().map_err(|_| {
            error(
                if control {
                    ErrorCode::ControlReset
                } else {
                    ErrorCode::Cancelled
                },
                "transport writer stopped",
            )
        })
    }
}
impl Writer {
    /// One nonblocking admission attempt. On Pending, no bytes were accepted.
    /// Control also registers a 20 ms retry because Quinn's ordinary writable
    /// wake condition observes the restored data window, not the control reserve.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<Result<usize, Error>> {
        let result = self.shared.poll(self.control, || {
            Pin::new(&mut self.stream).poll_write(cx, bytes)
        });
        if self.control && result.is_pending() {
            let interval = std::time::Duration::from_millis(20);
            let retry = self
                .retry
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(interval)));
            if retry.as_mut().poll(cx).is_ready() {
                retry.as_mut().reset(tokio::time::Instant::now() + interval);
                cx.waker().wake_by_ref();
            }
        } else {
            self.retry = None;
        }
        #[cfg(test)]
        if !self.control && result.is_pending() {
            self.shared
                .blocked_data
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        result
    }
    pub async fn write(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        poll_fn(|cx| self.poll_write(cx, bytes)).await
    }
    /// Cancellation may have sent a prefix; abandon the stream instead of
    /// restarting this operation with the original bytes.
    pub async fn write_all(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        while !bytes.is_empty() {
            let count = self.write(bytes).await?;
            if count == 0 {
                return Err(error(ErrorCode::InternalError, "writer made no progress"));
            }
            bytes = &bytes[count..];
        }
        Ok(())
    }
    pub fn id(&self) -> quinn::StreamId {
        self.stream.id()
    }
    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.retry = None;
        self.stream.finish()
    }
    pub fn reset(&mut self, code: quinn::VarInt) -> Result<(), quinn::ClosedStream> {
        self.retry = None;
        self.stream.reset(code)
    }
    pub fn stopped(
        &self,
    ) -> impl Future<Output = Result<Option<quinn::VarInt>, quinn::StoppedError>> + Send + 'static
    {
        self.stream.stopped()
    }
}
