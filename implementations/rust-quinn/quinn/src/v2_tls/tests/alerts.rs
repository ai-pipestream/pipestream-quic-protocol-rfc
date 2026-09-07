use super::*;
use quinn::crypto;
use quinn_proto::transport_parameters::TransportParameters;

#[tokio::test]
async fn client_local_tls_failure_also_uses_a_fatal_tls_close() {
    let fixture = Fixture::new();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots(&fixture.issuer))
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.resumption = rustls::client::Resumption::disabled();
    tls.time_provider = Arc::new(Clock(AtomicU64::new(0)));
    // Inject a failed client UTC source into the same adapter used by the
    // public client factory; no certificate checks are suppressed.
    let config = quinn::ClientConfig::new(Arc::new(alert_mapping::Client::new(
        QuicClientConfig::try_from(tls).unwrap(),
    )));
    let exchange = fixture.connect(config, "localhost").await;
    let error = exchange.client.as_ref().unwrap_err();
    let expected =
        quinn::TransportErrorCode::crypto(rustls::AlertDescription::HandshakeFailure.into());
    assert!(
        matches!(error, quinn::ConnectionError::TransportError(error) if error.code == expected),
        "{error:?}"
    );
    let error = exchange
        .server
        .as_ref()
        .err()
        .expect("local TLS failure admitted a peer")
        .downcast_ref::<quinn::ConnectionError>()
        .unwrap();
    assert!(
        matches!(error, quinn::ConnectionError::ConnectionClosed(close) if close.error_code == expected),
        "{error:?}"
    );
}

struct WrongConnectionId(Arc<QuicClientConfig>);

impl crypto::ClientConfig for WrongConnectionId {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        server_name: &str,
        _params: &TransportParameters,
    ) -> Result<Box<dyn crypto::Session>, quinn::ConnectError> {
        // RFC 9000 parameter 0x0f, zero-length connection ID. This is valid
        // TLS extension syntax, but disagrees with the real client's nonempty
        // initial source CID. Quinn must reject it outside the TLS adapter.
        let mut bytes: &[u8] = &[0x0f, 0x00];
        let wrong = TransportParameters::read(quinn::Side::Server, &mut bytes).unwrap();
        self.0.clone().start_session(version, server_name, &wrong)
    }
}

#[tokio::test]
async fn tls_adapter_does_not_relabel_actual_quic_transport_parameter_failure() {
    let fixture = Fixture::new();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots(&fixture.issuer))
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(WrongConnectionId(Arc::new(
        QuicClientConfig::try_from(tls).unwrap(),
    ))));
    let exchange = fixture.connect(config, "localhost").await;
    let error = exchange
        .server
        .as_ref()
        .err()
        .expect("invalid QUIC parameters admitted a peer")
        .downcast_ref::<quinn::ConnectionError>()
        .unwrap();
    assert!(
        matches!(error, quinn::ConnectionError::TransportError(error) if error.code == quinn::TransportErrorCode::TRANSPORT_PARAMETER_ERROR),
        "{error:?}"
    );
    let close = match &exchange.client {
        Ok(client) => tokio::time::timeout(HANDSHAKE, client.closed())
            .await
            .unwrap(),
        Err(error) => error.clone(),
    };
    assert!(
        matches!(&close, quinn::ConnectionError::ConnectionClosed(close) if close.error_code == quinn::TransportErrorCode::TRANSPORT_PARAMETER_ERROR),
        "{close:?}"
    );
}
