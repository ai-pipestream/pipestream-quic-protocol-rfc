//! Mutual TLS identity mapping for durable sessions.

use anyhow::{Context, Result, bail};
use pipestream_core::{
    ProtocolError,
    authorization::{PrincipalBinding, unauthorized},
};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Client certificate and its matching private key. No anonymous fallback is attempted.
#[derive(Debug, Clone)]
pub struct ClientIdentity {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

/// Explicit certificate-to-principal mapping under one issuing authority.
#[derive(Debug, Clone)]
pub struct AuthenticationPolicy {
    authority: String,
    roots: Arc<rustls::RootCertStore>,
    principals: BTreeMap<[u8; 32], String>,
}

impl AuthenticationPolicy {
    pub fn new(
        authority: String,
        roots: rustls::RootCertStore,
        principals: BTreeMap<[u8; 32], String>,
    ) -> Result<Self> {
        if roots.is_empty() || principals.is_empty() || principals.len() > 4096 {
            bail!("client trust roots and 1..4096 principal mappings are required");
        }
        for principal in principals.values() {
            PrincipalBinding::new(&authority, principal)?;
        }
        Ok(Self {
            authority,
            roots: Arc::new(roots),
            principals,
        })
    }

    /// Read a TSV file headed `sha256<TAB>principal`; fingerprints identify leaf certificates.
    pub fn from_files(authority: String, client_ca: &Path, principal_map: &Path) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        for certificate in CertificateDer::pem_file_iter(client_ca)? {
            roots.add(certificate?)?;
        }
        if fs::metadata(principal_map)?.len() > 1_048_576 {
            bail!("principal map exceeds 1 MiB");
        }
        let text = fs::read_to_string(principal_map).context("read principal map")?;
        let mut lines = text.lines();
        if lines.next() != Some("sha256\tprincipal") {
            bail!("principal map must start with sha256<TAB>principal");
        }
        let mut principals = BTreeMap::new();
        for line in lines {
            let Some((fingerprint, principal)) = line.split_once('\t') else {
                bail!("principal map row must contain fingerprint and principal");
            };
            if fingerprint.len() != 64 || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit()) {
                bail!("principal fingerprint must contain 64 hexadecimal digits");
            }
            let mut hash = [0; 32];
            for (index, pair) in fingerprint.as_bytes().chunks_exact(2).enumerate() {
                hash[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
            }
            if principals.insert(hash, principal.to_owned()).is_some() {
                bail!("duplicate principal certificate fingerprint");
            }
        }
        Self::new(authority, roots, principals)
    }

    pub(crate) fn verifier(&self) -> Result<Arc<dyn rustls::server::danger::ClientCertVerifier>> {
        Ok(rustls::server::WebPkiClientVerifier::builder(self.roots.clone()).build()?)
    }

    pub(crate) fn authenticate(
        &self,
        connection: &quinn::Connection,
    ) -> Result<PrincipalBinding, ProtocolError> {
        let certificates = connection
            .peer_identity()
            .ok_or_else(unauthorized)?
            .downcast::<Vec<CertificateDer<'static>>>()
            .map_err(|_| unauthorized())?;
        let leaf = certificates.first().ok_or_else(unauthorized)?;
        let hash: [u8; 32] = Sha256::digest(leaf.as_ref()).into();
        let principal = self.principals.get(&hash).ok_or_else(unauthorized)?;
        PrincipalBinding::new(&self.authority, principal)
    }

    pub(crate) fn permits_recovery(&self, binding: &PrincipalBinding) -> bool {
        binding.authority == self.authority
            && self.principals.values().any(|id| id == &binding.principal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_mapping_is_explicit_and_malformed_files_are_refused() {
        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("ca.crt");
        let map = directory.path().join("principals.tsv");
        let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
        fs::write(&ca, certified.cert.pem()).unwrap();
        let fingerprint = "01".repeat(32);
        fs::write(&map, format!("sha256\tprincipal\n{fingerprint}\talice\n")).unwrap();
        let policy = AuthenticationPolicy::from_files("issuer-a".into(), &ca, &map).unwrap();
        assert!(policy.permits_recovery(&PrincipalBinding::new("issuer-a", "alice").unwrap()));
        assert!(!policy.permits_recovery(&PrincipalBinding::new("issuer-b", "alice").unwrap()));
        assert!(!policy.permits_recovery(&PrincipalBinding::new("issuer-a", "bob").unwrap()));
        for text in [
            String::new(),
            "sha256\tprincipal\n".to_owned(),
            format!("sha256\tprincipal\n{fingerprint}\talice\n{fingerprint}\tbob\n"),
            format!("sha256\tprincipal\n{fingerprint}\t\n"),
            format!("sha256\tprincipal\n{fingerprint}\talice\tadmin\n"),
            "sha256\tprincipal\nzz\talice\n".to_owned(),
        ] {
            fs::write(&map, text).unwrap();
            assert!(AuthenticationPolicy::from_files("issuer-a".into(), &ca, &map).is_err());
        }
    }
}
