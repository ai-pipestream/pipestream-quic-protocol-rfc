//! Reusable Quinn transport for the PipeStream Layer 0 reference contract.

use anyhow::{Context, Result, bail};
use pipestream_core::{
    CHECKPOINT_ACK, CONNECTION_LEVEL, Capabilities, Checkpoint, ERROR_FRAME, ERROR_NO_ERROR,
    FRAME_CAPABILITIES, FRAME_CHECKPOINT, FRAME_GOAWAY, FRAME_STATUS, MAX_CONTROL_FRAME,
    MAX_PAYLOAD, ProtocolError, STATUS_COMPLETE, STATUS_PENDING, STATUS_PROCESSING,
    STATUS_UNSPECIFIED, Status, decode_capabilities, decode_checkpoint, decode_entity,
    decode_goaway, decode_status, decode_ucf, encode_capabilities, encode_checkpoint,
    encode_goaway, encode_status, entity_with_parent, next_entity_id,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

/// Configuration for a reusable Layer 0 server.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub output_directory: PathBuf,
    pub ready_file: Option<PathBuf>,
    pub once: bool,
}

/// Configuration for a reusable one-entity Layer 0 client session.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub remote: SocketAddr,
    pub ca_certificate: PathBuf,
    pub server_name: String,
    pub entity_id: u32,
    pub input: PathBuf,
    pub content_type: String,
    pub parent_id: Option<u32>,
}

/// Serve Layer 0 sessions until one completes when `once` is set.
pub async fn serve(options: ServerOptions) -> Result<()> {
    fs::create_dir_all(&options.output_directory).context("create output directory")?;
    let certs = CertificateDer::pem_file_iter(&options.certificate)
        .context("read certificate PEM")?
        .collect::<Result<Vec<_>, _>>()
        .context("parse certificate PEM")?;
    let key =
        PrivateKeyDer::from_pem_file(&options.private_key).context("parse private-key PEM")?;
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("configure server certificate")?;
    tls.alpn_protocols = vec![pipestream_core::ALPN.to_vec()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    let transport = Arc::get_mut(&mut config.transport).expect("new transport config is unique");
    transport
        .max_concurrent_bidi_streams(1u32.into())
        .max_concurrent_uni_streams(128u32.into())
        .max_idle_timeout(Some(Duration::from_secs(30).try_into()?));
    let endpoint = quinn::Endpoint::server(config, options.bind)?;
    let address = endpoint.local_addr()?;
    if let Some(path) = options.ready_file {
        fs::write(path, format!("{address}\n")).context("write ready file")?;
    }
    println!("READY {address}");
    let mut handled = 0usize;
    while let Some(incoming) = endpoint.accept().await {
        let connection = incoming.await.context("accept QUIC connection")?;
        if let Err(error) = handle_connection(&connection, &options.output_directory).await {
            close_for_error(&connection, &error);
            return Err(error.into());
        }
        handled += 1;
        if options.once && handled == 1 {
            break;
        }
    }
    endpoint.wait_idle().await;
    Ok(())
}

async fn handle_connection(
    connection: &quinn::Connection,
    output_directory: &Path,
) -> Result<(), ProtocolError> {
    let (mut control_send, mut control_recv) = connection.accept_bi().await.map_err(|error| {
        ProtocolError::new(ERROR_FRAME, "PIPESTREAM_CONTROL_RESET", error.to_string())
    })?;
    let (frame_type, payload) = read_control(&mut control_recv).await?;
    if frame_type != FRAME_CAPABILITIES {
        return Err(ProtocolError::frame_for_transport(
            "first frame must be CAPABILITIES",
        ));
    }
    let peer = decode_capabilities(&payload)?;
    let negotiated = Capabilities::default().negotiate(&peer)?;
    write_all(&mut control_send, &encode_capabilities(&negotiated)?).await?;
    write_all(
        &mut control_send,
        &encode_status(Status {
            state: STATUS_UNSPECIFIED,
            entity_id: CONNECTION_LEVEL,
            scope_id: 0,
            cursor: None,
            depth: 0,
        })?,
    )
    .await?;

    let (frame_type, payload) = read_control(&mut control_recv).await?;
    if frame_type != FRAME_STATUS {
        return Err(ProtocolError::frame_for_transport(
            "entity announcement must be STATUS",
        ));
    }
    let pending = decode_status(&payload)?;
    if pending.state != STATUS_PENDING {
        return Err(ProtocolError::entity_for_transport(
            "first entity status must be PENDING",
        ));
    }

    let mut entity_stream = connection
        .accept_uni()
        .await
        .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))?;
    let bytes = entity_stream
        .read_to_end(MAX_PAYLOAD + (1 << 16) + 4)
        .await
        .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))?;
    let (header, body) = decode_entity(&bytes)?;
    if header.entity_id != pending.entity_id {
        return Err(ProtocolError::entity_for_transport(
            "PENDING and EntityHeader IDs differ",
        ));
    }
    write_all(
        &mut control_send,
        &encode_status(Status {
            state: STATUS_PROCESSING,
            entity_id: header.entity_id,
            scope_id: 0,
            cursor: None,
            depth: 0,
        })?,
    )
    .await?;
    fs::write(
        output_directory.join(format!("{}.bin", header.entity_id)),
        body,
    )
    .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))?;
    if let Some(parent_id) = header.parent_id {
        fs::write(
            output_directory.join(format!("{}.parent", header.entity_id)),
            format!("{parent_id}\n"),
        )
        .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))?;
    }
    write_all(
        &mut control_send,
        &encode_status(Status {
            state: STATUS_COMPLETE,
            entity_id: header.entity_id,
            scope_id: 0,
            cursor: None,
            depth: 0,
        })?,
    )
    .await?;

    let next_entity_id = next_entity_id(header.entity_id)?;
    let (mut frame_type, mut payload) = read_control(&mut control_recv).await?;
    if frame_type == FRAME_CHECKPOINT {
        let mut checkpoint = decode_checkpoint(&payload)?;
        if checkpoint.flags != 0 || checkpoint.checkpoint_entity_id != next_entity_id {
            return Err(ProtocolError::entity_for_transport(
                "checkpoint barrier is not satisfied",
            ));
        }
        checkpoint.flags = CHECKPOINT_ACK;
        write_all(&mut control_send, &encode_checkpoint(&checkpoint)?).await?;
        (frame_type, payload) = read_control(&mut control_recv).await?;
    }
    if frame_type != FRAME_STATUS {
        return Err(ProtocolError::frame_for_transport(
            "terminal status must be followed by cursor update",
        ));
    }
    let cursor = decode_status(&payload)?;
    if cursor.state != STATUS_UNSPECIFIED
        || cursor.entity_id != CONNECTION_LEVEL
        || cursor.cursor != Some(next_entity_id)
    {
        return Err(ProtocolError::entity_for_transport(
            "invalid connection-level cursor update",
        ));
    }
    let (frame_type, payload) = read_control(&mut control_recv).await?;
    if frame_type != FRAME_GOAWAY || decode_goaway(&payload)? != header.entity_id {
        return Err(ProtocolError::frame_for_transport("invalid GOAWAY"));
    }
    write_all(&mut control_send, &encode_goaway(header.entity_id)?).await?;
    control_send
        .finish()
        .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))?;
    println!("RECEIVED {} {}", header.entity_id, body.len());
    let _ = tokio::time::timeout(Duration::from_secs(5), connection.closed()).await;
    Ok(())
}

/// Send one entity and wait for its terminal status and GOAWAY acknowledgement.
pub async fn send(options: ClientOptions) -> Result<()> {
    let payload = fs::read(&options.input).context("read input")?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&options.ca_certificate)
        .context("read CA PEM")?
        .collect::<Result<Vec<_>, _>>()
        .context("parse CA PEM")?
    {
        roots.add(cert).context("add CA certificate")?;
    }
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![pipestream_core::ALPN.to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(options.remote, &options.server_name)?
        .await
        .context("connect QUIC")?;
    let (mut control_send, mut control_recv) =
        connection.open_bi().await.context("open control stream")?;
    write_all(
        &mut control_send,
        &encode_capabilities(&Capabilities::default())?,
    )
    .await?;
    let (frame_type, response) = read_control(&mut control_recv).await?;
    if frame_type != FRAME_CAPABILITIES {
        bail!("PIPESTREAM_FRAME_ERROR: server did not answer capabilities");
    }
    decode_capabilities(&response)?;
    write_all(
        &mut control_send,
        &encode_status(Status {
            state: STATUS_UNSPECIFIED,
            entity_id: CONNECTION_LEVEL,
            scope_id: 0,
            cursor: None,
            depth: 0,
        })?,
    )
    .await?;
    write_all(
        &mut control_send,
        &encode_status(Status {
            state: STATUS_PENDING,
            entity_id: options.entity_id,
            scope_id: 0,
            cursor: None,
            depth: 0,
        })?,
    )
    .await?;
    let mut data = connection.open_uni().await.context("open entity stream")?;
    write_all(
        &mut data,
        &entity_with_parent(
            options.entity_id,
            options.parent_id,
            &payload,
            &options.content_type,
        )?,
    )
    .await?;
    data.finish().context("finish entity stream")?;
    for expected in [STATUS_PROCESSING, STATUS_COMPLETE] {
        let (frame_type, response) = read_control(&mut control_recv).await?;
        if frame_type != FRAME_STATUS {
            bail!("PIPESTREAM_FRAME_ERROR: expected STATUS");
        }
        let observed = decode_status(&response)?;
        if observed.entity_id != options.entity_id || observed.state != expected {
            bail!("PIPESTREAM_ENTITY_INVALID: unexpected status response");
        }
    }
    let next_entity_id = next_entity_id(options.entity_id)?;
    let checkpoint = Checkpoint {
        checkpoint_id: format!("entity-{}", options.entity_id),
        sequence_number: 1,
        checkpoint_entity_id: next_entity_id,
        scope_id: None,
        flags: 0,
        timeout_ms: None,
    };
    write_all(&mut control_send, &encode_checkpoint(&checkpoint)?).await?;
    let (frame_type, response) = read_control(&mut control_recv).await?;
    if frame_type != FRAME_CHECKPOINT {
        bail!("PIPESTREAM_FRAME_ERROR: expected CHECKPOINT acknowledgement");
    }
    let observed = decode_checkpoint(&response)?;
    if observed.checkpoint_id != checkpoint.checkpoint_id
        || observed.sequence_number != checkpoint.sequence_number
        || observed.checkpoint_entity_id != checkpoint.checkpoint_entity_id
        || observed.flags != CHECKPOINT_ACK
    {
        bail!("PIPESTREAM_ENTITY_INVALID: invalid checkpoint acknowledgement");
    }
    write_all(
        &mut control_send,
        &encode_status(Status {
            state: STATUS_UNSPECIFIED,
            entity_id: CONNECTION_LEVEL,
            scope_id: 0,
            cursor: Some(next_entity_id),
            depth: 0,
        })?,
    )
    .await?;
    write_all(&mut control_send, &encode_goaway(options.entity_id)?).await?;
    let (frame_type, response) = read_control(&mut control_recv).await?;
    if frame_type != FRAME_GOAWAY || decode_goaway(&response)? != options.entity_id {
        bail!("PIPESTREAM_FRAME_ERROR: invalid GOAWAY acknowledgement");
    }
    control_send.finish().context("finish control stream")?;
    connection.close(ERROR_NO_ERROR.into(), b"complete");
    endpoint.wait_idle().await;
    println!("SENT {} {}", options.entity_id, payload.len());
    Ok(())
}

async fn read_control(stream: &mut quinn::RecvStream) -> Result<(u8, Vec<u8>), ProtocolError> {
    loop {
        let mut header = [0u8; 5];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))?;
        let length = u32::from_be_bytes(header[1..5].try_into().expect("slice length")) as usize;
        if length > MAX_CONTROL_FRAME {
            return Err(ProtocolError::new(
                pipestream_core::ERROR_LIMIT_EXCEEDED,
                "PIPESTREAM_LIMIT_EXCEEDED",
                "control frame exceeds local limit",
            ));
        }
        let mut payload = vec![0; length];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))?;
        let mut complete = Vec::with_capacity(5 + length);
        complete.extend_from_slice(&header);
        complete.extend_from_slice(&payload);
        let (frame_type, parsed) = decode_ucf(&complete)?;
        if frame_type == FRAME_STATUS {
            let status = decode_status(parsed)?;
            if status.state == STATUS_UNSPECIFIED
                && status.entity_id == CONNECTION_LEVEL
                && status.cursor.is_none()
            {
                continue;
            }
        }
        return Ok((frame_type, parsed.to_vec()));
    }
}

async fn write_all(stream: &mut quinn::SendStream, bytes: &[u8]) -> Result<(), ProtocolError> {
    stream
        .write_all(bytes)
        .await
        .map_err(|error| ProtocolError::frame_for_transport(error.to_string()))
}

fn close_for_error(connection: &quinn::Connection, error: &ProtocolError) {
    connection.close(error.code.into(), error.to_string().as_bytes());
}

trait TransportErrors {
    fn frame_for_transport(detail: impl Into<String>) -> Self;
    fn entity_for_transport(detail: impl Into<String>) -> Self;
}

impl TransportErrors for ProtocolError {
    fn frame_for_transport(detail: impl Into<String>) -> Self {
        ProtocolError::new(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", detail)
    }

    fn entity_for_transport(detail: impl Into<String>) -> Self {
        ProtocolError::new(
            pipestream_core::ERROR_ENTITY_INVALID,
            "PIPESTREAM_ENTITY_INVALID",
            detail,
        )
    }
}
