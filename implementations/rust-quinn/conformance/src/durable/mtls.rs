//! Per-run isolated mTLS material: rcgen EC P-256 CA, a server certificate
//! with SAN DNS:localhost and IP:127.0.0.1, per-principal client certificates
//! with the clientAuth EKU, and a `sha256<TAB>principal` principal map over
//! each MAPPED client leaf DER. Follows the server/tests/v2_cli.rs fixture
//! pattern. G5 additionally mints same-CA unmapped certificates and
//! foreign-CA certificates for authentication-fault rows.

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

#[derive(Clone)]
pub struct Identity {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Clone)]
pub struct Material {
    /// CA certificate trusted by clients (`--ca`) and used to authenticate
    /// them on the server (`--client-ca`).
    pub ca_cert: PathBuf,
    pub server: Identity,
    principals: BTreeMap<String, Identity>,
    /// Same-CA client certificates deliberately ABSENT from the principal
    /// map (g5-unmapped-principal).
    pub unmapped: BTreeMap<String, Identity>,
    /// Client certificates signed by an unrelated, server-untrusted CA
    /// (g5-untrusted-identity).
    pub foreign: BTreeMap<String, Identity>,
    pub foreign_ca_cert: Option<PathBuf>,
    pub principal_map: PathBuf,
}

impl Material {
    /// Identity of a MAPPED principal (present in the principal map).
    pub fn principal(&self, name: &str) -> Result<&Identity> {
        self.principals
            .get(name)
            .with_context(|| format!("no generated mapped principal named {name:?}"))
    }

    /// Any minted client identity: mapped, unmapped, or foreign.
    pub fn identity(&self, name: &str) -> Result<&Identity> {
        self.principals
            .get(name)
            .or_else(|| self.unmapped.get(name))
            .or_else(|| self.foreign.get(name))
            .with_context(|| format!("no generated client identity named {name:?}"))
    }
}

fn mint_client(
    directory: &Path,
    ca: &CertifiedIssuer<KeyPair>,
    name: &str,
) -> Result<(Identity, Vec<u8>)> {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let mut params = CertificateParams::new(vec!["localhost".to_owned()])?;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.distinguished_name.push(DnType::CommonName, name);
    let cert = params.signed_by(&key, ca)?;
    let cert_path = directory.join(format!("{name}.crt"));
    let key_path = directory.join(format!("{name}.key"));
    fs::write(&cert_path, cert.pem())?;
    fs::write(&key_path, key.serialize_pem())?;
    Ok((
        Identity {
            cert: cert_path,
            key: key_path,
        },
        cert.der().to_vec(),
    ))
}

/// Generate a fresh CA and all identities under `directory`. `principals`
/// maps an identity name to its stable owner label; one client certificate is
/// minted per entry and each leaf DER SHA-256 is written into the map.
/// `unmapped` mints same-CA certificates that are NOT written into the map;
/// `foreign` mints certificates under a second, unrelated CA.
pub fn generate_full(
    directory: &Path,
    principals: &[(&str, &str)],
    unmapped: &[&str],
    foreign: &[&str],
) -> Result<Material> {
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
        let (identity, der) = mint_client(directory, &ca, name)?;
        map.push_str(&format!("{}\t{owner}\n", hex(&Sha256::digest(der))));
        identities.insert((*name).to_owned(), identity);
    }

    let mut unmapped_identities = BTreeMap::new();
    for name in unmapped {
        let (identity, _) = mint_client(directory, &ca, name)?;
        unmapped_identities.insert((*name).to_owned(), identity);
    }

    let mut foreign_ca_cert = None;
    let mut foreign_identities = BTreeMap::new();
    if !foreign.is_empty() {
        let mut foreign_params = CertificateParams::new(Vec::<String>::new())?;
        foreign_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        foreign_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        foreign_params
            .distinguished_name
            .push(DnType::CommonName, "PipeStream-Foreign-CA");
        let foreign_ca = CertifiedIssuer::self_signed(
            foreign_params,
            KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?,
        )?;
        let foreign_ca_path = directory.join("foreign-ca.crt");
        fs::write(&foreign_ca_path, foreign_ca.pem())?;
        for name in foreign {
            let (identity, _) = mint_client(directory, &foreign_ca, name)?;
            foreign_identities.insert((*name).to_owned(), identity);
        }
        foreign_ca_cert = Some(foreign_ca_path);
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
        unmapped: unmapped_identities,
        foreign: foreign_identities,
        foreign_ca_cert,
        principal_map,
    })
}

/// One CA with exactly the given mapped principals (no unmapped/foreign
/// identities).
pub fn generate(directory: &Path, principals: &[(&str, &str)]) -> Result<Material> {
    generate_full(directory, principals, &[], &[])
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

    #[test]
    fn unmapped_and_foreign_identities_stay_out_of_the_principal_map() {
        let directory = tempfile::tempdir().unwrap();
        let material = generate_full(
            directory.path(),
            &[("alice", "alice")],
            &["carol"],
            &["zed"],
        )
        .unwrap();
        assert!(material.principal("carol").is_err());
        assert!(material.principal("zed").is_err());
        assert!(material.identity("carol").unwrap().cert.is_file());
        assert!(material.identity("zed").unwrap().cert.is_file());
        let foreign_ca = material.foreign_ca_cert.as_ref().unwrap();
        assert!(foreign_ca.is_file());
        assert_ne!(
            fs::read(&material.ca_cert).unwrap(),
            fs::read(foreign_ca).unwrap()
        );
        // The map still lists exactly the mapped principals, nothing else.
        let map = fs::read_to_string(&material.principal_map).unwrap();
        let lines = map.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[1].ends_with("\talice"));
        assert!(!map.contains("carol"));
        assert!(!map.contains("zed"));
    }
}
