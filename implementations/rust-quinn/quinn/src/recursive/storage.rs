//! Physical storage slots remain charged after the connection waiting for them ends.

use super::*;
use std::sync::{Mutex, OnceLock, Weak};

const WORKERS: usize = 8;
const PER_PRINCIPAL: usize = 4;
type Principal = Option<(String, String)>;

#[derive(Default)]
struct Active {
    total: usize,
    principals: BTreeMap<Principal, usize>,
}

pub(super) struct StoragePool(Mutex<Active>);

impl StoragePool {
    pub(super) fn open(path: &Path) -> Result<Arc<Self>, ProtocolError> {
        static POOLS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<StoragePool>>>> = OnceLock::new();
        let path = fs::canonicalize(path).map_err(storage_error)?;
        let mut pools = POOLS
            .get_or_init(Default::default)
            .lock()
            .map_err(storage_error)?;
        pools.retain(|_, pool| pool.strong_count() != 0);
        if let Some(pool) = pools.get(&path).and_then(Weak::upgrade) {
            return Ok(pool);
        }
        let pool = Arc::new(Self(Mutex::default()));
        pools.insert(path, Arc::downgrade(&pool));
        Ok(pool)
    }

    fn acquire(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
    ) -> Result<Permit, ProtocolError> {
        let principal = principal.map(|p| (p.authority.clone(), p.principal.clone()));
        let mut active = self.0.lock().map_err(storage_error)?;
        if active.total >= WORKERS
            || active.principals.get(&principal).copied().unwrap_or(0) >= PER_PRINCIPAL
        {
            return Err(limit_error("connection storage capacity exhausted"));
        }
        active.total += 1;
        *active.principals.entry(principal.clone()).or_default() += 1;
        Ok(Permit {
            pool: self.clone(),
            principal,
        })
    }

    pub(super) async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
        operation: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let permit = self.acquire(principal)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
        .map_err(storage_error)?
    }
}

struct Permit {
    pool: Arc<StoragePool>,
    principal: Principal,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.pool.0.lock() {
            active.total -= 1;
            let count = active
                .principals
                .get_mut(&self.principal)
                .expect("storage principal is charged");
            *count -= 1;
            if *count == 0 {
                active.principals.remove(&self.principal);
            }
        }
    }
}

impl<P: EntityProcessor, E: EntityStore> RecursiveService<P, E> {
    pub(super) async fn storage<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Self) -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let service = self.clone();
        self.storage_pool
            .run(self.caller.as_ref(), move || operation(service))
            .await
    }

    pub(super) async fn load_session(
        &self,
        id: &str,
    ) -> Result<Option<pipestream_core::persistence::VersionedSession>, ProtocolError> {
        let id = id.to_owned();
        self.storage(move |service| {
            let state = service.store.load(&id).map_err(store_error)?;
            if let Some(state) = &state {
                state.session.authorize(service.caller.as_ref())?;
            }
            Ok(state)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_share_global_and_authority_principal_storage_bounds() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let first = StoragePool::open(file.path()).unwrap();
        let second = StoragePool::open(file.path()).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let alice = PrincipalBinding::new("authority", "alice").unwrap();
        let other_authority = PrincipalBinding::new("other", "alice").unwrap();
        let mut held = Vec::new();
        for _ in 0..PER_PRINCIPAL {
            held.push(first.acquire(Some(&alice)).unwrap());
        }
        assert_eq!(
            second.acquire(Some(&alice)).err().unwrap().code,
            ERROR_LIMIT_EXCEEDED
        );
        for _ in 0..PER_PRINCIPAL {
            held.push(second.acquire(Some(&other_authority)).unwrap());
        }
        assert_eq!(
            first.acquire(None).err().unwrap().code,
            ERROR_LIMIT_EXCEEDED
        );
        held.pop();
        held.push(first.acquire(None).unwrap());
        drop(held);
        assert_eq!(second.0.lock().unwrap().total, 0);
        assert!(second.0.lock().unwrap().principals.is_empty());
    }

    #[tokio::test]
    async fn cancellation_keeps_physical_storage_credit_until_operation_returns() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let pool = StoragePool::open(file.path()).unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let running = pool.clone();
        let waiter = tokio::spawn(async move {
            running
                .run(None, move || {
                    entered.send(()).unwrap();
                    held.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
                .await
        });
        started.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let reopened = StoragePool::open(file.path()).unwrap();
        let mut others = Vec::new();
        for _ in 1..PER_PRINCIPAL {
            others.push(reopened.acquire(None).unwrap());
        }
        assert_eq!(
            reopened.acquire(None).err().unwrap().code,
            ERROR_LIMIT_EXCEEDED
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.0.lock().unwrap().total == PER_PRINCIPAL {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let _last = reopened.acquire(None).unwrap();
    }
}
