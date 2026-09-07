//! Version-2 TLS boundary. This does not advertise an application profile or
//! provide a complete V2 endpoint. The dispatcher must authorize every request
//! and separately enforce connection/work/storage quotas.

use anyhow::{Result, bail};
use pipestream_core::v2::{ALPN, Error, ErrorCode, IdentityLabel};
use quinn::crypto::rustls::{HandshakeData, QuicClientConfig, QuicServerConfig};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::danger::ClientCertVerifier,
    time_provider::{DefaultTimeProvider, TimeProvider},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

const MAX_CERTIFICATE_BYTES: usize = 65_535;
const MAX_CERTIFICATES: usize = 16;

fn refusal(code: ErrorCode, detail: &'static str) -> Error {
    Error { code, detail }
}

fn bounded_chain(chain: &[CertificateDer<'_>]) -> bool {
    !chain.is_empty()
        && chain.len() <= MAX_CERTIFICATES
        && chain
            .iter()
            .try_fold(0usize, |n, c| n.checked_add(c.len()))
            .is_some_and(|n| n <= MAX_CERTIFICATE_BYTES)
}

/// Operator-owned trust and certificate mapping. Updating trust/mapping requires
/// a new value; live peers can be rechecked against that current value.
/// An empty mapping withdraws every durable identity without disabling TLS
/// certificate validation for peers that still use Core.
#[derive(Debug)]
pub struct ClientAuthentication {
    authority: IdentityLabel,
    principals: BTreeMap<[u8; 32], IdentityLabel>,
    verifier: Arc<dyn ClientCertVerifier>,
    clock: Arc<dyn TimeProvider>,
}

impl ClientAuthentication {
    pub fn new(
        authority: IdentityLabel,
        roots: rustls::RootCertStore,
        principals: BTreeMap<[u8; 32], IdentityLabel>,
        clock: Arc<dyn TimeProvider>,
    ) -> Result<Self> {
        authority.validate()?;
        if roots.is_empty() || principals.len() > 4096 {
            bail!("client trust roots and at most 4096 principal mappings are required");
        }
        for principal in principals.values() {
            principal.validate()?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()?;
        Ok(Self {
            authority,
            principals,
            verifier,
            clock,
        })
    }
}

/// Stable identity established by a completed handshake and current mapping.
/// It is not an authorization grant for future requests or application effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub authority: IdentityLabel,
    pub owner: IdentityLabel,
}

/// A completed server-side handshake. Identity cannot change through migration
/// or live map edits; a changed mapping requires a fresh handshake. An expired
/// or invalid credential never becomes usable again on this connection.
pub struct Peer {
    connection: quinn::Connection,
    certificates: Option<Vec<CertificateDer<'static>>>,
    identity: Option<Identity>,
    authentication_configured: bool,
    credential: Mutex<CredentialState>,
}

#[derive(Default)]
struct CredentialState {
    greatest_utc: u64,
    invalid: bool,
}

impl Peer {
    pub fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

    /// Recheck trust, credential validity and the current stable mapping before
    /// each new request. `None` authorizes only anonymous/unmapped Core, never
    /// a durable profile. Owner policy/revocation still belongs to the authority.
    pub fn authorize(
        &self,
        current: Option<&ClientAuthentication>,
    ) -> Result<Option<&Identity>, Error> {
        // Serialize the latching refusal with every successful check, including
        // callers that started before another thread invalidated the credential.
        let mut state = self
            .credential
            .lock()
            .map_err(|_| refusal(ErrorCode::InternalError, "credential state lock poisoned"))?;
        if state.invalid {
            return Err(refusal(
                ErrorCode::Unauthorized,
                "connection credential is no longer valid",
            ));
        }
        if current.is_some() != self.authentication_configured {
            state.invalid = true;
            return Err(refusal(
                ErrorCode::Unauthorized,
                "authentication policy changed; reconnect",
            ));
        }
        let Some(current) = current else {
            return Ok(None);
        };
        let now = current
            .clock
            .current_time()
            .ok_or_else(|| refusal(ErrorCode::ClockUnsafe, "credential clock unavailable"))?;
        if now.as_secs() < state.greatest_utc {
            return Err(refusal(
                ErrorCode::ClockUnsafe,
                "credential clock regressed",
            ));
        }
        state.greatest_utc = now.as_secs();
        let Some(certificates) = &self.certificates else {
            return Ok(None);
        };
        let leaf = &certificates[0];
        let hash: [u8; 32] = Sha256::digest(leaf.as_ref()).into();
        let mapped = current.principals.get(&hash).map(|owner| Identity {
            authority: current.authority.clone(),
            owner: owner.clone(),
        });
        if mapped != self.identity
            || current
                .verifier
                .verify_client_cert(leaf, &certificates[1..], now)
                .is_err()
        {
            state.invalid = true;
            return Err(refusal(
                ErrorCode::Unauthorized,
                "credential validity or mapping changed",
            ));
        }
        Ok(self.identity.as_ref())
    }
}

/// V2-only server crypto, with client certificates requested when configured.
/// Missing certificates may reach Core; presented invalid certificates fail TLS.
/// Tickets, resumption and early application data are disabled.
pub struct ServerSecurity {
    config: quinn::ServerConfig,
    authentication: Option<Arc<ClientAuthentication>>,
}

impl ServerSecurity {
    pub fn new(
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        authentication: Option<Arc<ClientAuthentication>>,
    ) -> Result<Self> {
        if !bounded_chain(&certificates) {
            bail!("server certificate chain exceeds its count/byte limit");
        }
        let builder =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13]);
        let builder = match &authentication {
            Some(policy) => builder.with_client_cert_verifier(policy.verifier.clone()),
            None => builder.with_no_client_auth(),
        };
        let mut tls = builder.with_single_cert(certificates, private_key)?;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        tls.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        tls.send_tls13_tickets = 0;
        tls.max_tls13_tickets = 0;
        tls.max_early_data_size = 0;
        tls.send_half_rtt_data = false;
        tls.time_provider = authentication.as_ref().map_or_else(
            || Arc::new(DefaultTimeProvider) as Arc<dyn TimeProvider>,
            |policy| policy.clock.clone(),
        );
        let config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
        Ok(Self {
            config,
            authentication,
        })
    }

    pub fn configuration(&self) -> quinn::ServerConfig {
        self.config.clone()
    }

    /// Await a full handshake, never a 0.5-RTT server connection. Certificate
    /// validation failures retain their TLS alerts/QUIC CRYPTO_ERRORs. Other
    /// local TLS-stack failures propagate without producing an application peer.
    /// The enclosing server must reserve a bounded connection slot first.
    pub async fn accept(&self, incoming: quinn::Incoming, timeout: Duration) -> Result<Peer> {
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            bail!("handshake timeout must be positive and at most 30 seconds");
        }
        if self
            .authentication
            .as_ref()
            .is_some_and(|policy| policy.clock.current_time().is_none())
        {
            incoming.refuse();
            return Err(refusal(
                ErrorCode::ClockUnsafe,
                "credential clock unavailable before handshake",
            )
            .into());
        }
        let connection = tokio::time::timeout(timeout, incoming).await??;
        let data = connection
            .handshake_data()
            .and_then(|d| d.downcast::<HandshakeData>().ok());
        if connection.side() != quinn::Side::Server
            || data.is_none_or(|d| d.protocol.as_deref() != Some(ALPN))
        {
            connection.close(
                ErrorCode::FrameError.quic_error().try_into()?,
                b"V2 TLS required",
            );
            return Err(refusal(ErrorCode::FrameError, "V2 server handshake required").into());
        }
        let certificates = connection
            .peer_identity()
            .map(|p| p.downcast::<Vec<CertificateDer<'static>>>())
            .transpose()
            .map_err(|_| refusal(ErrorCode::Unauthorized, "unexpected TLS identity"))?
            .map(|p| *p);
        if certificates
            .as_ref()
            .is_some_and(|chain| !bounded_chain(chain))
        {
            connection.close(
                ErrorCode::LimitExceeded.quic_error().try_into()?,
                b"certificate limit",
            );
            return Err(refusal(
                ErrorCode::LimitExceeded,
                "client certificate chain exceeds bound",
            )
            .into());
        }
        let identity = self.authentication.as_ref().and_then(|policy| {
            let leaf = certificates.as_ref()?.first()?;
            let hash: [u8; 32] = Sha256::digest(leaf.as_ref()).into();
            policy.principals.get(&hash).map(|owner| Identity {
                authority: policy.authority.clone(),
                owner: owner.clone(),
            })
        });
        let peer = Peer {
            connection,
            certificates,
            identity,
            authentication_configured: self.authentication.is_some(),
            credential: Mutex::new(CredentialState::default()),
        };
        if let Err(error) = peer.authorize(self.authentication.as_deref()) {
            peer.connection.close(
                error.code.quic_error().try_into()?,
                error.code.name().as_bytes(),
            );
            return Err(error.into());
        }
        Ok(peer)
    }
}

/// V2 client crypto with ordinary server DNS/IP identity verification. The
/// caller passes the configured server name to Quinn's connect operation.
/// No certificate, ALPN or resumption failure triggers an anonymous/V1 retry.
pub fn client_configuration(
    roots: rustls::RootCertStore,
    identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> Result<quinn::ClientConfig> {
    if roots.is_empty() {
        bail!("server trust roots are required");
    }
    let builder = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots);
    let mut tls = if let Some((certificates, private_key)) = identity {
        if !bounded_chain(&certificates) {
            bail!("client certificate chain exceeds its count/byte limit");
        }
        builder.with_client_auth_cert(certificates, private_key)?
    } else {
        builder.with_no_client_auth()
    };
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.resumption = rustls::client::Resumption::disabled();
    tls.enable_early_data = false;
    let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
    config.version(1);
    Ok(config)
}

#[cfg(test)]
mod tests;
