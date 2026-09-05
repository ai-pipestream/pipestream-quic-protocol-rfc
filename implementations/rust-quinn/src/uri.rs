//! Typed resource locators from Section 11.6. A locator grants no access rights.

use crate::{MAX_ENTITY_ID, ProtocolError};
use std::{net::Ipv6Addr, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    Session,
    Entity { scope_id: u32, entity_id: u32 },
    Claim { claim_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipeStreamUri {
    pub host: String,
    pub port: u16,
    pub session_id: String,
    pub resource: Resource,
}

fn invalid() -> ProtocolError {
    ProtocolError::frame("invalid pipestream resource URI")
}

fn decimal(value: &str) -> Result<u64, ProtocolError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid());
    }
    value.parse().map_err(|_| invalid())
}

impl FromStr for PipeStreamUri {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 1024 || !value.is_ascii() || value.contains(['@', '?', '#', '%']) {
            return Err(invalid());
        }
        let (scheme, remainder) = value.split_once("://").ok_or_else(invalid)?;
        if !scheme.eq_ignore_ascii_case("pipestream") {
            return Err(invalid());
        }
        let (authority, path) = remainder.split_once('/').ok_or_else(invalid)?;
        let (host, port) = authority.rsplit_once(':').ok_or_else(invalid)?;
        let host = if let Some(address) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            address
                .parse::<Ipv6Addr>()
                .map_err(|_| invalid())?
                .to_string()
        } else {
            if host.is_empty()
                || host.len() > 253
                || host.split('.').any(|label| {
                    label.is_empty()
                        || label.len() > 63
                        || label.starts_with('-')
                        || label.ends_with('-')
                        || !label
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                })
            {
                return Err(invalid());
            }
            host.to_ascii_lowercase()
        };
        let port = u16::try_from(decimal(port)?).map_err(|_| invalid())?;
        if port == 0 {
            return Err(invalid());
        }
        let parts: Vec<_> = path.split('/').collect();
        if parts.len() < 2 || parts[0] != "sessions" {
            return Err(invalid());
        }
        let session = parts[1];
        if session.is_empty()
            || session.len() > 128
            || !session
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(invalid());
        }
        let resource = match parts.as_slice() {
            [_, _] => Resource::Session,
            [_, _, "scopes", scope, "entities", entity] => {
                let scope_id = u32::try_from(decimal(scope)?).map_err(|_| invalid())?;
                let entity_id = u32::try_from(decimal(entity)?).map_err(|_| invalid())?;
                if entity_id == 0 || entity_id > MAX_ENTITY_ID {
                    return Err(invalid());
                }
                Resource::Entity {
                    scope_id,
                    entity_id,
                }
            }
            [_, _, "claims", claim] => {
                let claim_id = decimal(claim)?;
                if claim_id == 0 {
                    return Err(invalid());
                }
                Resource::Claim { claim_id }
            }
            _ => return Err(invalid()),
        };
        Ok(Self {
            host,
            port,
            session_id: session.to_owned(),
            resource,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_locators_have_unambiguous_namespaces() {
        let entity: PipeStreamUri =
            "pipestream://EXAMPLE.com:9443/sessions/job_1/scopes/0/entities/123"
                .parse()
                .unwrap();
        assert_eq!(entity.host, "example.com");
        assert_eq!(entity.port, 9443);
        assert_eq!(
            entity.resource,
            Resource::Entity {
                scope_id: 0,
                entity_id: 123
            }
        );
        let claim: PipeStreamUri =
            "PIPESTREAM://[2001:db8::1]:9443/sessions/job-1/claims/18446744073709551615"
                .parse()
                .unwrap();
        assert_eq!(claim.resource, Resource::Claim { claim_id: u64::MAX });
        assert_eq!(claim.host, "2001:db8::1");
    }

    #[test]
    fn ambiguous_or_credential_bearing_locators_are_refused() {
        for uri in [
            "pipestream://example.com:9443/session/123",
            "pipestream://user:password@example.com:9443/sessions/a",
            "pipestream://example.com/sessions/a",
            "pipestream://example.com:0/sessions/a",
            "pipestream://example.com:65536/sessions/a",
            "pipestream://example.com:09443/sessions/a",
            "pipestream://example.com:9443/sessions/a?token=secret",
            "pipestream://example.com:9443/sessions/%61",
            "pipestream://example.com:9443/sessions/a#b",
            "pipestream://example.com:9443/sessions/a/scopes/0/entities/0",
            "pipestream://example.com:9443/sessions/a/scopes/0/entities/4294967293",
            "pipestream://example.com:9443/sessions/a/scopes/4294967296/entities/1",
            "pipestream://example.com:9443/sessions/a/claims/18446744073709551616",
            "pipestream://example.com:9443/sessions/a/claims/01",
            "pipestream://[fe80::1%25eth0]:9443/sessions/a",
        ] {
            assert!(uri.parse::<PipeStreamUri>().is_err(), "{uri}");
        }
    }
}
