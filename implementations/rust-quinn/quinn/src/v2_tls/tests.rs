//! Real TLS/QUIC handshakes and the per-request credential guard. These are
//! transport security tests, not evidence of a complete V2 application endpoint.
use super::*;
use pipestream_core::v2::*;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const HANDSHAKE: Duration = Duration::from_secs(5);

mod alerts;
mod configuration;
mod core;
mod resumption;

#[derive(Debug)]
struct Clock(AtomicU64);
impl TimeProvider for Clock {
    fn current_time(&self) -> Option<rustls::pki_types::UnixTime> {
        let seconds = self.0.load(Ordering::SeqCst);
        (seconds != 0)
            .then(|| rustls::pki_types::UnixTime::since_unix_epoch(Duration::from_secs(seconds)))
    }
}
fn at(year: i32, month: u8, day: u8) -> u64 {
    rcgen::date_time_ymd(year, month, day).unix_timestamp() as u64
}
struct Certificate {
    der: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}
impl Certificate {
    fn identity(&self) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        (vec![self.der.clone()], self.key.clone_key())
    }
    fn fingerprint(&self) -> [u8; 32] {
        Sha256::digest(self.der.as_ref()).into()
    }
}
fn make_issuer() -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.not_before = rcgen::date_time_ymd(2000, 1, 1);
    params.not_after = rcgen::date_time_ymd(2100, 1, 1);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap()
}
fn certificate(issuer: &CertifiedIssuer<'static, KeyPair>, kind: &str) -> Certificate {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    if kind == "server" {
        params
            .subject_alt_names
            .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    }
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![if kind == "server" || kind == "wrong-usage" {
        ExtendedKeyUsagePurpose::ServerAuth
    } else {
        ExtendedKeyUsagePurpose::ClientAuth
    }];
    params.not_before = rcgen::date_time_ymd(if kind == "future" { 2040 } else { 2000 }, 1, 1);
    params.not_after = rcgen::date_time_ymd(
        match kind {
            "server" => 2100,
            "expired" => 2020,
            "future" => 2050,
            _ => 2031,
        },
        1,
        1,
    );
    let cert = params.signed_by(&key, issuer).unwrap();
    Certificate {
        der: cert.der().clone(),
        key: rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    }
}
fn roots(issuer: &CertifiedIssuer<'static, KeyPair>) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(issuer.der().clone()).unwrap();
    roots
}
struct Fixture {
    issuer: CertifiedIssuer<'static, KeyPair>,
    certificate: Certificate,
    clients: Vec<Certificate>,
    clock: Arc<Clock>,
    policy: Arc<ClientAuthentication>,
    security: ServerSecurity,
    endpoint: quinn::Endpoint,
}
impl Fixture {
    fn new() -> Self {
        let issuer = make_issuer();
        let server = certificate(&issuer, "server");
        let clients: Vec<_> = [
            "valid",
            "valid",
            "unmapped",
            "expired",
            "future",
            "wrong-usage",
        ]
        .iter()
        .map(|kind| certificate(&issuer, kind))
        .collect();
        let mut principals: BTreeMap<_, _> = clients
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 2)
            .map(|(_, c)| (c.fingerprint(), IdentityLabel("alice".into())))
            .collect();
        let other = make_issuer();
        let foreign = certificate(&other, "valid");
        // Even an explicitly mapped fingerprint must pass certificate validation.
        principals.insert(foreign.fingerprint(), IdentityLabel("alice".into()));
        let mut clients = clients;
        clients.push(foreign);
        let clock = Arc::new(Clock(AtomicU64::new(at(2030, 6, 1))));
        let policy = Arc::new(
            ClientAuthentication::new(
                IdentityLabel("issuer-a".into()),
                roots(&issuer),
                principals,
                clock.clone(),
            )
            .unwrap(),
        );
        let security = ServerSecurity::new(
            vec![server.der.clone()],
            server.key.clone_key(),
            Some(policy.clone()),
        )
        .unwrap();
        let endpoint =
            quinn::Endpoint::server(security.configuration(), "127.0.0.1:0".parse().unwrap())
                .unwrap();
        Self {
            issuer,
            certificate: server,
            clients,
            clock,
            policy,
            security,
            endpoint,
        }
    }
    fn config(&self, client: Option<usize>) -> quinn::ClientConfig {
        client_configuration(
            roots(&self.issuer),
            client.map(|i| self.clients[i].identity()),
        )
        .unwrap()
    }
    async fn connect(&self, config: quinn::ClientConfig, name: &str) -> Exchange {
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config);
        let connect = endpoint
            .connect(self.endpoint.local_addr().unwrap(), name)
            .unwrap();
        let accept = async {
            let incoming = tokio::time::timeout(HANDSHAKE, self.endpoint.accept())
                .await
                .unwrap()
                .unwrap();
            self.security.accept(incoming, HANDSHAKE).await
        };
        let (client, server) = tokio::join!(
            async { tokio::time::timeout(HANDSHAKE, connect).await.unwrap() },
            accept
        );
        Exchange {
            endpoint,
            client,
            server,
        }
    }
}
struct Exchange {
    endpoint: quinn::Endpoint,
    client: Result<quinn::Connection, quinn::ConnectionError>,
    server: Result<Peer>,
}
impl Drop for Exchange {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"test finished");
        if let Ok(peer) = &self.server {
            peer.connection().close(0u32.into(), b"test finished");
        }
    }
}
fn code(result: Result<Option<&Identity>, Error>, expected: ErrorCode) {
    assert_eq!(result.unwrap_err().code, expected);
}
fn offer(required: bool) -> Capabilities {
    Capabilities {
        response: ResponseFlag(0),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: if required {
            vec![ProfileId(DURABLE_WORK.into())]
        } else {
            vec![]
        },
        control_limit: ControlLimit(4096),
        stream_limit: ConcurrencyLimit(4),
        pending_limit: ConcurrencyLimit(4),
        object_limit: Number(4096),
        stream_idle_ms: IdleMs(1000),
        stream_lifetime_ms: LifetimeMs(5000),
    }
}
fn crypto(error: &quinn::ConnectionError) -> bool {
    let code = match error {
        quinn::ConnectionError::TransportError(error) => u64::from(error.code),
        quinn::ConnectionError::ConnectionClosed(error) => u64::from(error.error_code),
        _ => return false,
    };
    (0x100..0x200).contains(&code)
}

#[tokio::test]
async fn full_handshake_maps_rotated_certificates_to_the_same_stable_owner() {
    let fixture = Fixture::new();
    for client in [0, 1] {
        let exchange = fixture
            .connect(fixture.config(Some(client)), "localhost")
            .await;
        assert!(exchange.client.is_ok());
        let peer = exchange.server.as_ref().unwrap();
        let authenticated = peer.authorize(Some(&fixture.policy)).unwrap();
        let identity = authenticated.unwrap();
        assert_eq!(identity.authority.0, "issuer-a");
        assert_eq!(identity.owner.0, "alice");
        let selected =
            Capabilities::select(&offer(true), &offer(true), authenticated.is_some()).unwrap();
        assert!(selected.has(DURABLE_WORK) && selected.has(RESULT_DELIVERY));
        let data = peer
            .connection()
            .handshake_data()
            .unwrap()
            .downcast::<HandshakeData>()
            .unwrap();
        assert_eq!(data.protocol.as_deref(), Some(ALPN));
    }
}

#[tokio::test]
async fn anonymous_and_valid_unmapped_peers_only_authorize_core() {
    let fixture = Fixture::new();
    for client in [None, Some(2)] {
        let exchange = fixture.connect(fixture.config(client), "localhost").await;
        assert!(exchange.client.is_ok());
        let authenticated = exchange
            .server
            .as_ref()
            .unwrap()
            .authorize(Some(&fixture.policy))
            .unwrap()
            .is_some();
        assert!(!authenticated);
        assert_eq!(
            Capabilities::select(&offer(true), &offer(false), authenticated)
                .unwrap_err()
                .code,
            ErrorCode::Unauthorized
        );
        assert!(
            Capabilities::select(&offer(false), &offer(false), authenticated)
                .unwrap()
                .supported
                .is_empty()
        );
    }
}

#[tokio::test]
async fn mapped_expired_future_wrong_usage_and_untrusted_certificates_fail_tls() {
    let fixture = Fixture::new();
    for client in 3..=6 {
        let exchange = fixture
            .connect(fixture.config(Some(client)), "localhost")
            .await;
        let error = exchange
            .server
            .as_ref()
            .err()
            .expect("invalid certificate reached application boundary");
        let error = error
            .downcast_ref::<quinn::ConnectionError>()
            .expect("failure must remain a QUIC TLS error");
        assert!(crypto(error), "client={client}: {error:?}");
        if let Ok(connection) = &exchange.client {
            let error = tokio::time::timeout(HANDSHAKE, connection.closed())
                .await
                .unwrap();
            assert!(crypto(&error), "client={client}: {error:?}");
        }
    }
}

#[tokio::test]
async fn expired_live_credentials_never_resurrect_or_become_anonymous() {
    let fixture = Fixture::new();
    let exchange = fixture.connect(fixture.config(Some(0)), "localhost").await;
    let peer = exchange.server.as_ref().unwrap();
    assert!(peer.authorize(Some(&fixture.policy)).unwrap().is_some());
    fixture.clock.0.store(at(2032, 1, 1), Ordering::SeqCst);
    code(
        peer.authorize(Some(&fixture.policy)),
        ErrorCode::Unauthorized,
    );
    fixture.clock.0.store(at(2030, 6, 1), Ordering::SeqCst);
    code(
        peer.authorize(Some(&fixture.policy)),
        ErrorCode::Unauthorized,
    );
    code(peer.authorize(None), ErrorCode::Unauthorized);
}

#[tokio::test]
async fn certificate_validity_boundaries_follow_tls_verification_not_stream_deadlines() {
    let fixture = Fixture::new();
    fixture.clock.0.store(at(2040, 1, 1), Ordering::SeqCst);
    let exchange = fixture.connect(fixture.config(Some(4)), "localhost").await;
    let peer = exchange.server.as_ref().unwrap();
    assert!(peer.authorize(Some(&fixture.policy)).unwrap().is_some());
    fixture.clock.0.store(at(2050, 1, 1), Ordering::SeqCst);
    assert!(peer.authorize(Some(&fixture.policy)).unwrap().is_some());
    fixture.clock.0.store(at(2050, 1, 1) + 1, Ordering::SeqCst);
    code(
        peer.authorize(Some(&fixture.policy)),
        ErrorCode::Unauthorized,
    );
}

#[tokio::test]
async fn unavailable_or_regressed_credential_time_refuses_without_extending_validity() {
    let fixture = Fixture::new();
    let exchange = fixture.connect(fixture.config(Some(0)), "localhost").await;
    let peer = exchange.server.as_ref().unwrap();
    for seconds in [0, at(2030, 5, 31)] {
        fixture.clock.0.store(seconds, Ordering::SeqCst);
        code(
            peer.authorize(Some(&fixture.policy)),
            ErrorCode::ClockUnsafe,
        );
    }
    fixture.clock.0.store(at(2030, 6, 1), Ordering::SeqCst);
    assert!(peer.authorize(Some(&fixture.policy)).unwrap().is_some());
}

#[tokio::test]
async fn current_mapping_or_trust_changes_cannot_rebind_a_live_connection() {
    let fixture = Fixture::new();
    for fault in ["owner", "authority", "trust", "removed", "all-removed"] {
        let exchange = fixture.connect(fixture.config(Some(0)), "localhost").await;
        let mut principals = fixture.policy.principals.clone();
        if fault == "owner" {
            principals.insert(
                fixture.clients[0].fingerprint(),
                IdentityLabel("bob".into()),
            );
        }
        if fault == "removed" {
            principals.remove(&fixture.clients[0].fingerprint());
        }
        if fault == "all-removed" {
            principals.clear();
        }
        let authority = if fault == "authority" {
            "issuer-b"
        } else {
            "issuer-a"
        };
        let trust = if fault == "trust" {
            roots(&make_issuer())
        } else {
            roots(&fixture.issuer)
        };
        let current = ClientAuthentication::new(
            IdentityLabel(authority.into()),
            trust,
            principals,
            fixture.clock.clone(),
        )
        .unwrap();
        let peer = exchange.server.as_ref().unwrap();
        code(peer.authorize(Some(&current)), ErrorCode::Unauthorized);
        code(
            peer.authorize(Some(&fixture.policy)),
            ErrorCode::Unauthorized,
        );
    }
}

#[tokio::test]
async fn wrong_server_name_and_wrong_server_trust_fail_before_application_data() {
    let fixture = Fixture::new();
    let bad_trust =
        client_configuration(roots(&make_issuer()), Some(fixture.clients[0].identity())).unwrap();
    for (config, name) in [
        (fixture.config(Some(0)), "not-localhost.invalid"),
        (fixture.config(Some(0)), "127.0.0.2"),
        (bad_trust, "localhost"),
    ] {
        let exchange = fixture.connect(config, name).await;
        assert!(crypto(exchange.client.as_ref().unwrap_err()));
        assert!(exchange.server.is_err());
    }
}

#[tokio::test]
async fn legacy_alpn_is_never_negotiated_or_retried() {
    let fixture = Fixture::new();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots(&fixture.issuer))
        .with_no_client_auth();
    tls.alpn_protocols = vec![pipestream_core::ALPN.to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
    let exchange = fixture.connect(config, "localhost").await;
    assert!(crypto(exchange.client.as_ref().unwrap_err()));
    assert!(exchange.server.is_err());
}

#[tokio::test]
async fn operator_identity_and_certificate_limits_refuse_before_listening() {
    let fixture = Fixture::new();
    for authority in ["", "space label", "nonascii-é"] {
        assert!(
            ClientAuthentication::new(
                IdentityLabel(authority.into()),
                roots(&fixture.issuer),
                fixture.policy.principals.clone(),
                fixture.clock.clone()
            )
            .is_err()
        );
    }
    let principals = [(
        fixture.clients[0].fingerprint(),
        IdentityLabel("not/allowed".into()),
    )]
    .into();
    assert!(
        ClientAuthentication::new(
            IdentityLabel("issuer".into()),
            roots(&fixture.issuer),
            principals,
            fixture.clock.clone()
        )
        .is_err()
    );
    assert!(!bounded_chain(&[]));
    assert!(!bounded_chain(&vec![
        fixture.clients[0].der.clone();
        MAX_CERTIFICATES + 1
    ]));
    assert!(!bounded_chain(&[CertificateDer::from(vec![
        0;
        MAX_CERTIFICATE_BYTES
            + 1
    ])]));
    assert!(
        client_configuration(
            roots(&fixture.issuer),
            Some((vec![], fixture.clients[0].key.clone_key()))
        )
        .is_err()
    );
    assert!(ServerSecurity::new(vec![], fixture.certificate.key.clone_key(), None).is_err());
    let principals = (0u64..=4096)
        .map(|n| {
            let mut fingerprint = [0; 32];
            fingerprint[..8].copy_from_slice(&n.to_be_bytes());
            (fingerprint, IdentityLabel("alice".into()))
        })
        .collect();
    assert!(
        ClientAuthentication::new(
            IdentityLabel("issuer".into()),
            roots(&fixture.issuer),
            principals,
            fixture.clock.clone()
        )
        .is_err()
    );
}

#[tokio::test]
async fn peer_certificate_count_limit_is_enforced_after_real_tls_before_dispatch() {
    let fixture = Fixture::new();
    // Bypass the public client's local chain bound to exercise the receive side.
    // WebPKI can build a valid path while ignoring redundant supplied entries.
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots(&fixture.issuer))
        .with_client_auth_cert(
            vec![fixture.clients[0].der.clone(); MAX_CERTIFICATES + 1],
            fixture.clients[0].key.clone_key(),
        )
        .unwrap();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
    let exchange = fixture.connect(config, "localhost").await;
    let error = exchange
        .server
        .as_ref()
        .err()
        .expect("oversized certificate inventory reached dispatcher");
    assert_eq!(
        error.downcast_ref::<Error>().unwrap().code,
        ErrorCode::LimitExceeded
    );
    let client = exchange.client.as_ref().unwrap();
    let closed = tokio::time::timeout(HANDSHAKE, client.closed())
        .await
        .unwrap();
    assert!(
        matches!(closed, quinn::ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == ErrorCode::LimitExceeded.quic_error())
    );
}

#[tokio::test]
async fn core_only_tls_checks_server_ip_identity_without_creating_a_client_principal() {
    let mut fixture = Fixture::new();
    fixture.security = ServerSecurity::new(
        vec![fixture.certificate.der.clone()],
        fixture.certificate.key.clone_key(),
        None,
    )
    .unwrap();
    fixture.endpoint = quinn::Endpoint::server(
        fixture.security.configuration(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let exchange = fixture.connect(fixture.config(Some(0)), "127.0.0.1").await;
    assert!(exchange.client.is_ok());
    let peer = exchange.server.as_ref().unwrap();
    assert!(
        peer.certificates.is_none(),
        "unsolicited certificate must not establish identity"
    );
    assert!(peer.authorize(None).unwrap().is_none());
    code(
        peer.authorize(Some(&fixture.policy)),
        ErrorCode::Unauthorized,
    );
    code(peer.authorize(None), ErrorCode::Unauthorized);
}

#[tokio::test]
async fn unknown_tls_time_refuses_the_handshake_without_application_negotiation() {
    let fixture = Fixture::new();
    fixture.clock.0.store(0, Ordering::SeqCst);
    let exchange = fixture.connect(fixture.config(Some(0)), "localhost").await;
    let error = exchange
        .server
        .as_ref()
        .err()
        .expect("unknown time admitted a TLS peer");
    assert_eq!(
        error.downcast_ref::<Error>().unwrap().code,
        ErrorCode::ClockUnsafe
    );
    assert!(
        matches!(exchange.client.as_ref().unwrap_err(), quinn::ConnectionError::ConnectionClosed(close) if close.error_code == quinn::TransportErrorCode::CONNECTION_REFUSED)
    );
}

#[tokio::test]
async fn clock_loss_during_tls_uses_a_fatal_tls_close_without_an_application_peer() {
    #[derive(Debug)]
    struct LostClock(AtomicU64);
    impl TimeProvider for LostClock {
        fn current_time(&self) -> Option<rustls::pki_types::UnixTime> {
            (self.0.fetch_add(1, Ordering::SeqCst) == 0).then(|| {
                rustls::pki_types::UnixTime::since_unix_epoch(Duration::from_secs(at(2030, 6, 1)))
            })
        }
    }
    let mut fixture = Fixture::new();
    let clock = Arc::new(LostClock(AtomicU64::new(0)));
    fixture.policy = Arc::new(
        ClientAuthentication::new(
            IdentityLabel("issuer-a".into()),
            roots(&fixture.issuer),
            fixture.policy.principals.clone(),
            clock.clone(),
        )
        .unwrap(),
    );
    fixture.security = ServerSecurity::new(
        vec![fixture.certificate.der.clone()],
        fixture.certificate.key.clone_key(),
        Some(fixture.policy.clone()),
    )
    .unwrap();
    fixture.endpoint = quinn::Endpoint::server(
        fixture.security.configuration(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let exchange = fixture.connect(fixture.config(Some(0)), "localhost").await;
    let error = exchange
        .server
        .as_ref()
        .err()
        .expect("lost clock still created an application peer");
    let error = error.downcast_ref::<quinn::ConnectionError>().unwrap();
    let expected =
        quinn::TransportErrorCode::crypto(rustls::AlertDescription::HandshakeFailure.into());
    assert!(
        matches!(error, quinn::ConnectionError::TransportError(error) if error.code == expected),
        "local TLS failure must use a fatal TLS close: {error:?}"
    );
    let close = match &exchange.client {
        Ok(client) => tokio::time::timeout(HANDSHAKE, client.closed())
            .await
            .unwrap(),
        Err(error) => error.clone(),
    };
    assert!(
        matches!(&close, quinn::ConnectionError::ConnectionClosed(close) if close.error_code == expected),
        "remote peer must observe the same TLS close: {close:?}"
    );
    assert!(
        clock.0.load(Ordering::SeqCst) >= 2,
        "clock must fail after preflight"
    );
}
