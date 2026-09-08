//! Per-run isolated mTLS material: rcgen EC P-256 CA, a server certificate
//! with SAN DNS:localhost and IP:127.0.0.1, per-principal client certificates
//! with the clientAuth EKU, and a `sha256<TAB>principal` principal map over
//! each client leaf DER. Follows the server/tests/v2_cli.rs fixture pattern.

use crate::hex;
use anyhow::{Context, Result, ensure};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, SanType,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
};

pub const AUTHORITY: &str = "issuer-a";

pub struct Identity {
    pub cert: PathBuf,
    pub key: PathBuf,
}

pub struct Material {
    /// CA certificate trusted by clients (`--ca`) and used to authenticate
    /// them on the server (`--client-ca`).
    pub ca_cert: PathBuf,
    pub server: Identity,
    principals: BTreeMap<String, Identity>,
    pub principal_map: PathBuf,
}

impl Material {
    pub fn principal(&self, name: &str) -> Result<&Identity> {
        self.principals
            .get(name)
            .with_context(|| format!("no generated client identity named {name:?}"))
    }
}

/// Generate a fresh CA and all identities under `directory`. `principals`
/// maps an identity name to its stable owner label; one client certificate is
/// minted per entry and each leaf DER SHA-256 is written into the map.
pub fn generate(directory: &Path, principals: &[(&str, &str)]) -> Result<Material> {
    fs::create_dir_all(directory)?;
    ensure!(!principals.is_empty(), "at least one principal is required");

    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "PipeStream-Durable-CA");
    let ca =
        CertifiedIssuer::self_signed(ca_params, KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?)?;
    let ca_cert = directory.join("ca.crt");
    fs::write(&ca_cert, ca.pem())?;

    let mut server_params = CertificateParams::new(vec!["localhost".to_owned()])?;
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into()?),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ];
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let server = server_params.signed_by(&server_key, &ca)?;
    let server_cert = directory.join("server.crt");
    let server_key_path = directory.join("server.key");
    fs::write(&server_cert, server.pem())?;
    fs::write(&server_key_path, server_key.serialize_pem())?;

    let mut map = String::from("sha256\tprincipal\n");
    let mut identities = BTreeMap::new();
    for (name, owner) in principals {
        ensure!(
            !owner.is_empty(),
            "principal owner labels must not be empty for identity {name:?}"
        );
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let mut params = CertificateParams::new(vec!["localhost".to_owned()])?;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.distinguished_name.push(DnType::CommonName, *name);
        let cert = params.signed_by(&key, &ca)?;
        let cert_path = directory.join(format!("{name}.crt"));
        let key_path = directory.join(format!("{name}.key"));
        fs::write(&cert_path, cert.pem())?;
        fs::write(&key_path, key.serialize_pem())?;
        map.push_str(&format!("{}\t{owner}\n", hex(&Sha256::digest(cert.der()))));
        identities.insert(
            (*name).to_owned(),
            Identity {
                cert: cert_path,
                key: key_path,
            },
        );
    }
    let principal_map = directory.join("principals.tsv");
    fs::write(&principal_map, map)?;

    Ok(Material {
        ca_cert,
        server: Identity {
            cert: server_cert,
            key: server_key_path,
        },
        principals: identities,
        principal_map,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_isolated_ca_server_and_principal_map() {
        let directory = tempfile::tempdir().unwrap();
        let material = generate(directory.path(), &[("alice", "alice"), ("bob", "bob")]).unwrap();
        assert!(material.ca_cert.is_file());
        assert!(material.server.cert.is_file());
        assert!(material.server.key.is_file());
        assert!(material.principal("alice").unwrap().cert.is_file());
        assert!(material.principal("alice").unwrap().key.is_file());
        let map = fs::read_to_string(&material.principal_map).unwrap();
        let lines = map.lines().collect::<Vec<_>>();
        assert_eq!(lines[0], "sha256\tprincipal");
        assert_eq!(lines.len(), 3);
        for line in &lines[1..] {
            let (fingerprint, owner) = line.split_once('\t').unwrap();
            assert_eq!(fingerprint.len(), 64);
            assert!(fingerprint.bytes().all(|b| b.is_ascii_hexdigit()));
            assert!(matches!(owner, "alice" | "bob"));
        }
        // A second generation under another directory is fully independent.
        let other = tempfile::tempdir().unwrap();
        let regenerated = generate(other.path(), &[("alice", "alice")]).unwrap();
        assert_ne!(
            fs::read(&material.ca_cert).unwrap(),
            fs::read(&regenerated.ca_cert).unwrap()
        );
        assert_ne!(
            fs::read(&material.principal("alice").unwrap().cert).unwrap(),
            fs::read(&regenerated.principal("alice").unwrap().cert).unwrap()
        );
    }
}
