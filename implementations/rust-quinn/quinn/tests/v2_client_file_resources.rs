//! Isolated count/lifetime gate, not a whole-process heap/RSS measurement.
use pipestream_quic::{
    v2::*,
    v2_client::session::{Failure, files::FileInput},
};

#[tokio::test]
async fn sixty_four_open_inputs_hold_capacity_until_descriptor_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input");
    std::fs::write(&path, b"bounded file").unwrap();
    let mut inputs = Vec::new();
    for _ in 0..64 {
        inputs.push(FileInput::open(path.clone(), 1024).await.unwrap());
    }
    assert!(matches!(
        FileInput::open(path.clone(), 1024).await,
        Err(Failure::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    inputs.pop().unwrap().close().await.unwrap();
    let replacement = FileInput::open(path, 1024).await.unwrap();
    replacement.close().await.unwrap();
    for input in inputs {
        input.close().await.unwrap();
    }
    eprintln!("V2 client file gate: open-files=64 refused=1 cleanup-before-replacement=1");
}
