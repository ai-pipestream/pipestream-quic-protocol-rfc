//! Adapt the pinned Quinn/rustls TLS error boundary, not QUIC transport errors.
//!
//! In quinn-proto 0.11.17, TlsSession::read_handshake maps rustls read_hs errors
//! without an alert to PROTOCOL_VIOLATION. RFC 9001 Section 4.8 instead provides
//! the TLS alert space, including generic handshake_failure. Only this specific
//! call boundary is adapted. Transport-parameter validation, framing and packet
//! protection remain Quinn's responsibility; no diagnostics are string-matched.

use quinn::crypto::{
    self,
    rustls::{QuicClientConfig, QuicServerConfig},
};
use quinn_proto::{
    ConnectError, ConnectionId, Side, TransportError, TransportErrorCode,
    transport_parameters::TransportParameters,
};
use std::{any::Any, sync::Arc};

pub(super) struct Server(Arc<QuicServerConfig>);

impl Server {
    pub(super) fn new(inner: QuicServerConfig) -> Self {
        Self(Arc::new(inner))
    }
}

impl crypto::ServerConfig for Server {
    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> Result<crypto::Keys, crypto::UnsupportedVersion> {
        self.0.initial_keys(version, dst_cid)
    }

    fn retry_tag(&self, version: u32, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        self.0.retry_tag(version, orig_dst_cid, packet)
    }

    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &TransportParameters,
    ) -> Box<dyn crypto::Session> {
        Box::new(Session(self.0.clone().start_session(version, params)))
    }
}

pub(super) struct Client(Arc<QuicClientConfig>);

impl Client {
    pub(super) fn new(inner: QuicClientConfig) -> Self {
        Self(Arc::new(inner))
    }
}

impl crypto::ClientConfig for Client {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        server_name: &str,
        params: &TransportParameters,
    ) -> Result<Box<dyn crypto::Session>, ConnectError> {
        Ok(Box::new(Session(self.0.clone().start_session(
            version,
            server_name,
            params,
        )?)))
    }
}

// Construction is private and restricted to the pinned rustls implementation.
// Applying this mapping to an arbitrary crypto backend would lose provenance.
struct Session(Box<dyn crypto::Session>);

fn tls_read_error(mut error: TransportError) -> TransportError {
    if error.code == TransportErrorCode::PROTOCOL_VIOLATION && error.frame.is_none() {
        error.code = TransportErrorCode::crypto(rustls::AlertDescription::HandshakeFailure.into());
    }
    error
}

impl crypto::Session for Session {
    fn initial_keys(&self, dst_cid: &ConnectionId, side: Side) -> crypto::Keys {
        self.0.initial_keys(dst_cid, side)
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        self.0.handshake_data()
    }

    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        self.0.peer_identity()
    }

    fn early_crypto(&self) -> Option<(Box<dyn crypto::HeaderKey>, Box<dyn crypto::PacketKey>)> {
        self.0.early_crypto()
    }

    fn early_data_accepted(&self) -> Option<bool> {
        self.0.early_data_accepted()
    }

    fn is_handshaking(&self) -> bool {
        self.0.is_handshaking()
    }

    fn read_handshake(&mut self, buf: &[u8]) -> Result<bool, TransportError> {
        self.0.read_handshake(buf).map_err(tls_read_error)
    }

    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        self.0.transport_parameters()
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<crypto::Keys> {
        self.0.write_handshake(buf)
    }

    fn next_1rtt_keys(&mut self) -> Option<crypto::KeyPair<Box<dyn crypto::PacketKey>>> {
        self.0.next_1rtt_keys()
    }

    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        self.0.is_valid_retry(orig_dst_cid, header, payload)
    }

    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), crypto::ExportKeyingMaterialError> {
        self.0.export_keying_material(output, label, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_alertless_tls_read_errors_are_changed() {
        let missing_alert = TransportError {
            code: TransportErrorCode::PROTOCOL_VIOLATION,
            frame: None,
            reason: "local TLS failure without an alert".into(),
        };
        let mapped = tls_read_error(missing_alert.clone());
        assert_eq!(u64::from(mapped.code), 0x128);
        assert_eq!(mapped.reason, missing_alert.reason);
        assert_eq!(mapped.frame, missing_alert.frame);

        // Every existing TLS alert retains its exact code and diagnostics.
        for alert in 0..=255 {
            let original = TransportError {
                code: TransportErrorCode::crypto(alert),
                ..missing_alert.clone()
            };
            assert_eq!(tls_read_error(original.clone()), original);
        }
        for code in [
            TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
            TransportErrorCode::FRAME_ENCODING_ERROR,
            TransportErrorCode::INTERNAL_ERROR,
            TransportErrorCode::CRYPTO_BUFFER_EXCEEDED,
        ] {
            let original = TransportError {
                code,
                ..missing_alert.clone()
            };
            assert_eq!(tls_read_error(original.clone()), original);
        }
    }
}
