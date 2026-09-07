use super::*;

#[tokio::test]
async fn accepted_peer_uses_owned_authentication_not_the_endpoint_default() {
    let mut fixture = Fixture::new();
    // A listener default may have been left behind during a configuration
    // reload. The admission boundary must choose its own TLS policy explicitly.
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![fixture.certificate.der.clone()],
            fixture.certificate.key.clone_key(),
        )
        .unwrap();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    fixture.endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap())),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let exchange = fixture.connect(fixture.config(Some(0)), "localhost").await;
    let peer = exchange.server.as_ref().unwrap();
    assert_eq!(
        peer.authorize(Some(&fixture.policy))
            .unwrap()
            .unwrap()
            .owner
            .0,
        "alice"
    );

    // An invalid presented credential must fail TLS, not reach anonymous Core
    // because the endpoint default forgot to request a client certificate.
    let invalid = fixture.connect(fixture.config(Some(3)), "localhost").await;
    let error = invalid
        .server
        .as_ref()
        .err()
        .expect("endpoint default bypassed TLS authentication")
        .downcast_ref::<quinn::ConnectionError>()
        .unwrap();
    assert!(crypto(error), "{error:?}");
}
