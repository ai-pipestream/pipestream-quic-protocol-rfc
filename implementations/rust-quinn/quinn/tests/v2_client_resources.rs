//! Separate process: the client's process-wide connection ceiling is not shared
//! with concurrently running unit tests. Counts are not heap/RSS measurements.
use pipestream_quic::{
    v2::*,
    v2_client::transport::{Options, Reply, Security, Transport},
    v2_core,
    v2_tls::ServerSecurity,
};
use std::time::Duration as Elapsed;

#[tokio::test]
async fn sixty_four_real_client_connections_refuse_the_next_and_recover_after_draining() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate.cert.der().clone()).unwrap();
    let security = ServerSecurity::new(
        vec![certificate.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
        None,
    )
    .unwrap();
    let server = v2_core::Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        security,
        v2_core::Options {
            anonymous_connections: 64,
            ..Default::default()
        },
    )
    .unwrap();
    let address = server.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(server.run(async {
        let _ = stopped.await;
    }));
    let open = || {
        Transport::connect(
            "127.0.0.1:0".parse().unwrap(),
            address,
            "localhost",
            Security::new(roots.clone(), None).unwrap(),
            Options::default(),
        )
    };
    let mut clients = Vec::new();
    for _ in 0..64 {
        clients.push(open().await.unwrap());
    }
    let extra = open().await.err().unwrap();
    assert_eq!(
        extra.downcast_ref::<Error>().unwrap().code,
        ErrorCode::LimitExceeded
    );
    for client in &clients {
        let reply = client
            .exchange(Control::Drain(Drain::Detach { request: Id(1) }), None)
            .await
            .unwrap();
        assert!(matches!(
            reply,
            Reply::Control(Control::Drain(Drain::Detached { .. }))
        ));
        client.close();
    }
    for client in &clients {
        tokio::time::timeout(Elapsed::from_secs(10), client.closed())
            .await
            .unwrap();
    }
    let replacement = open().await.unwrap();
    replacement.close();
    tokio::time::timeout(Elapsed::from_secs(10), replacement.closed())
        .await
        .unwrap();
    stop.send(()).unwrap();
    tokio::time::timeout(Elapsed::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    eprintln!(
        "V2 client resource gate: real-connections=64 refused-before-connect=1 drained-handles-retained=64 replacement=1"
    );
}
