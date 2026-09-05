//! Durable caller and issuing-authority bindings. Identifiers are not credentials.

use crate::{ProtocolError, session::Session};
use serde::{Deserialize, Serialize};

pub const ERROR_UNAUTHORIZED: u32 = 0x10;
pub const EXTENSION_AUTHENTICATED_SESSIONS: u16 = 0xff02;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalBinding {
    pub authority: String,
    pub principal: String,
}

impl PrincipalBinding {
    pub fn new(
        authority: impl Into<String>,
        principal: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        let binding = Self {
            authority: authority.into(),
            principal: principal.into(),
        };
        for id in [&binding.authority, &binding.principal] {
            if id.is_empty()
                || id.len() > 128
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
            {
                return Err(unauthorized());
            }
        }
        Ok(binding)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOwner {
    pub binding: PrincipalBinding,
    pub revoked: bool,
}

pub fn unauthorized() -> ProtocolError {
    ProtocolError::new(
        ERROR_UNAUTHORIZED,
        "PIPESTREAM_UNAUTHORIZED",
        "session access denied",
    )
}

impl Session {
    /// Bind only a new, empty session. Existing anonymous work cannot acquire an owner.
    pub fn bind_owner(&mut self, binding: PrincipalBinding) -> Result<(), ProtocolError> {
        PrincipalBinding::new(&binding.authority, &binding.principal)?;
        if self.owner.is_some()
            || !self.entities.is_empty()
            || !self.checkpoints.is_empty()
            || !self.claims.is_empty()
            || self
                .work_sets
                .as_ref()
                .is_some_and(|w| !w.scopes.is_empty())
        {
            return Err(unauthorized());
        }
        self.owner = Some(SessionOwner {
            binding,
            revoked: false,
        });
        Ok(())
    }

    pub fn authorize(&self, caller: Option<&PrincipalBinding>) -> Result<(), ProtocolError> {
        match (&self.owner, caller) {
            (None, None) => Ok(()),
            (Some(owner), Some(caller)) if !owner.revoked && &owner.binding == caller => Ok(()),
            _ => Err(unauthorized()),
        }
    }

    /// Operator action, persisted by the caller's session-store transaction.
    pub fn revoke_access(&mut self) -> Result<(), ProtocolError> {
        self.owner.as_mut().ok_or_else(unauthorized)?.revoked = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{SessionStore, SqliteSessionStore};

    #[test]
    fn ownership_and_revocation_survive_reopen_and_do_not_allow_conversion() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("authorized.sqlite3");
        let alice = PrincipalBinding::new("authority-a", "alice").unwrap();
        let bob = PrincipalBinding::new("authority-a", "bob").unwrap();
        let elsewhere = PrincipalBinding::new("authority-b", "alice").unwrap();
        let mut session = Session::new("owned", 7, 100).unwrap();
        assert!(session.authorize(Some(&alice)).is_err());
        session.bind_owner(alice.clone()).unwrap();
        assert!(session.bind_owner(bob.clone()).is_err());
        let store = SqliteSessionStore::open(&path).unwrap();
        store.create(&session).unwrap();
        drop(store);
        let store = SqliteSessionStore::open(&path).unwrap();
        let retained = store.load("owned").unwrap().unwrap();
        assert!(retained.session.authorize(Some(&alice)).is_ok());
        for caller in [None, Some(&bob), Some(&elsewhere)] {
            assert_eq!(
                retained.session.authorize(caller).unwrap_err().code,
                ERROR_UNAUTHORIZED
            );
        }
        store.transact("owned", Session::revoke_access).unwrap();
        drop(store);
        let retained = SqliteSessionStore::open(&path)
            .unwrap()
            .load("owned")
            .unwrap()
            .unwrap();
        assert_eq!(
            retained.session.authorize(Some(&alice)).unwrap_err().code,
            ERROR_UNAUTHORIZED
        );
        assert_eq!(retained.session.owner.unwrap().binding, alice);
    }
}
