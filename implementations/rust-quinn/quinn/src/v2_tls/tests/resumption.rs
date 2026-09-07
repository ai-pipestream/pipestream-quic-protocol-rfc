use super::*;
use rustls::{
    NamedGroup,
    client::{
        ClientSessionMemoryCache, ClientSessionStore, Tls12ClientSessionValue,
        Tls13ClientSessionValue,
    },
    pki_types::ServerName,
};
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
struct Tickets {
    cache: ClientSessionMemoryCache,
    received: AtomicUsize,
}
impl ClientSessionStore for Tickets {
    fn set_kx_hint(&self, name: ServerName<'static>, group: NamedGroup) {
        self.cache.set_kx_hint(name, group);
    }
    fn kx_hint(&self, name: &ServerName<'_>) -> Option<NamedGroup> {
        self.cache.kx_hint(name)
    }
    fn set_tls12_session(&self, name: ServerName<'static>, value: Tls12ClientSessionValue) {
        self.cache.set_tls12_session(name, value);
    }
    fn tls12_session(&self, name: &ServerName<'_>) -> Option<Tls12ClientSessionValue> {
        self.cache.tls12_session(name)
    }
    fn remove_tls12_session(&self, name: &ServerName<'static>) {
        self.cache.remove_tls12_session(name);
    }
    fn insert_tls13_ticket(&self, name: ServerName<'static>, value: Tls13ClientSessionValue) {
        self.received.fetch_add(1, Ordering::SeqCst);
        self.cache.insert_tls13_ticket(name, value);
    }
    fn take_tls13_ticket(&self, name: &ServerName<'static>) -> Option<Tls13ClientSessionValue> {
        self.cache.take_tls13_ticket(name)
    }
}

// A bounded Core capability round trip lets both transport drivers process
// their post-handshake flights. It does not implement/advertise durable work.
async fn core_roundtrip(exchange: &Exchange) {
    let client = exchange.client.as_ref().unwrap();
    let server = exchange.server.as_ref().unwrap().connection();
    let mut offered = offer(false);
    offered.supported.clear();
    let request = Control::Capabilities(offered.clone())
        .encode(INITIAL_CONTROL_LIMIT)
        .unwrap();
    let response = Control::Capabilities(Capabilities::select(&offered, &offered, false).unwrap())
        .encode(INITIAL_CONTROL_LIMIT)
        .unwrap();
    let receive_length = request.len();
    tokio::join!(
        async {
            let (mut send, mut recv) = client.open_bi().await.unwrap();
            send.write_all(&request).await.unwrap();
            let mut received = vec![0; response.len()];
            recv.read_exact(&mut received).await.unwrap();
            assert_eq!(received, response);
        },
        async {
            let (mut send, mut recv) = server.accept_bi().await.unwrap();
            let mut received = vec![0; receive_length];
            recv.read_exact(&mut received).await.unwrap();
            assert_eq!(received, request);
            send.write_all(&response).await.unwrap();
        }
    );
}

#[tokio::test]
async fn a_resumption_enabled_client_gets_no_ticket_and_expiry_requires_full_handshake() {
    let fixture = Fixture::new();
    let tickets = Arc::new(Tickets {
        cache: ClientSessionMemoryCache::new(4),
        received: AtomicUsize::new(0),
    });
    let (certificates, private_key) = fixture.clients[0].identity();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots(&fixture.issuer))
        .with_client_auth_cert(certificates, private_key)
        .unwrap();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.resumption = rustls::client::Resumption::store(tickets.clone());
    tls.enable_early_data = true;
    let config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
    let exchange = fixture.connect(config.clone(), "localhost").await;
    tokio::time::timeout(HANDSHAKE, core_roundtrip(&exchange))
        .await
        .unwrap();
    assert_eq!(tickets.received.load(Ordering::SeqCst), 0);

    fixture.clock.0.store(at(2032, 1, 1), Ordering::SeqCst);
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connecting = endpoint
        .connect(fixture.endpoint.local_addr().unwrap(), "localhost")
        .unwrap();
    let connecting = match connecting.into_0rtt() {
        Err(full_handshake) => full_handshake,
        Ok((connection, _)) => {
            connection.close(0u32.into(), b"unexpected early data");
            panic!("server supplied resumable state despite its fresh-credential policy");
        }
    };
    let (client, server) = tokio::join!(
        async { tokio::time::timeout(HANDSHAKE, connecting).await.unwrap() },
        async {
            let incoming = tokio::time::timeout(HANDSHAKE, fixture.endpoint.accept())
                .await
                .unwrap()
                .unwrap();
            fixture.security.accept(incoming, HANDSHAKE).await
        }
    );
    let server = server
        .err()
        .expect("expired credential resumed without validation");
    assert!(crypto(
        server.downcast_ref::<quinn::ConnectionError>().unwrap()
    ));
    if let Ok(client) = client {
        assert!(crypto(
            &tokio::time::timeout(HANDSHAKE, client.closed())
                .await
                .unwrap()
        ));
    }
    endpoint.close(0u32.into(), b"test finished");
}

#[tokio::test]
async fn public_client_requires_fresh_tls_even_when_the_other_server_offers_tickets() {
    let mut fixture = Fixture::new();
    fixture.clock.0.store(at(2031, 1, 1) - 1, Ordering::SeqCst);
    // Model another implementation offering resumable TLS, independently of
    // the public server factory's no-ticket policy. Use the same server config
    // and client config across both connections so tickets would be usable.
    let mut tls = rustls::ServerConfig::builder()
        .with_client_cert_verifier(fixture.policy.verifier.clone())
        .with_single_cert(
            vec![fixture.certificate.der.clone()],
            fixture.certificate.key.clone_key(),
        )
        .unwrap();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.time_provider = fixture.clock.clone();
    tls.max_early_data_size = u32::MAX;
    fixture.endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap())),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let config = fixture.config(Some(0));
    let exchange = fixture.connect(config.clone(), "localhost").await;
    tokio::time::timeout(HANDSHAKE, core_roundtrip(&exchange))
        .await
        .unwrap();
    fixture.clock.0.store(at(2031, 1, 1) + 1, Ordering::SeqCst);
    let next = fixture.connect(config, "localhost").await;
    let error = next
        .server
        .as_ref()
        .err()
        .expect("expired credential bypassed fresh TLS");
    // A resumed handshake would reach the separate post-handshake guard and
    // return application UNAUTHORIZED. Require the actual TLS validation error.
    let error = error
        .downcast_ref::<quinn::ConnectionError>()
        .expect("client resumed rather than requiring full TLS");
    assert!(crypto(error), "{error:?}");
}
