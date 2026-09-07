use super::*;
use pipestream_quic::{
    v2_authority::{
        Authority,
        server::{Options, Server},
    },
    v2_tls::{ClientAuthentication, ServerSecurity},
};
use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt, sync::Arc};

pub struct Network {
    pub bind: SocketAddr,
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: PathBuf,
    pub result_authority: String,
    pub ready_file: Option<PathBuf>,
    pub object_limit: u64,
}
pub async fn serve(storage: configuration::Storage, network: Network) -> Result<()> {
    // Register signals before accepting work, including process-test shutdown.
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let (store, payloads, principals) = storage.open(false)?;
    let authentication = ClientAuthentication::new(
        IdentityLabel(storage.authority),
        configuration::roots(&network.client_ca)?,
        principals,
        Arc::new(rustls::time_provider::DefaultTimeProvider),
    )?;
    let security = ServerSecurity::new(
        configuration::certificates(&network.cert)?,
        configuration::key(&network.key)?,
        Some(Arc::new(authentication)),
    )?;
    let authority = Authority::new(store, payloads, 4)?;
    let mut options = Options::default();
    options.offer.object_limit = Number(network.object_limit);
    let server = Server::bind(
        network.bind,
        security,
        authority,
        applications::registry()?,
        authority::execution::ResultEndpoint::new(network.result_authority)?,
        options,
    )?;
    let address = server.local_addr()?;
    if let Some(path) = network.ready_file {
        // A stale readiness marker must never be silently replaced.
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        writeln!(file, "{address}")?;
        file.sync_all()?;
    }
    println!("LISTENING {address}");
    let report = server
        .run(async {
            tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        })
        .await?;
    if !report.drained() || report.fault.is_some() {
        bail!("server shutdown did not fully drain: {report:?}");
    }
    println!("DRAINED");
    Ok(())
}
