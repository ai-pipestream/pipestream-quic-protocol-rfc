use super::*;

fn owner() -> Vec<String> {
    ["--authority", "issuer-a", "--owner", "alice"]
        .map(String::from)
        .into()
}
fn copies(fixture: &Fixture) -> Vec<String> {
    vec![
        "--result-dir".into(),
        fixture.path("copies"),
        "--result-objects".into(),
        "1".into(),
        "--result-bytes".into(),
        "1048576".into(),
    ]
}
fn exports(fixture: &Fixture) -> Vec<String> {
    vec![
        "--export-dir".into(),
        fixture.path("exports"),
        "--export-objects".into(),
        "1".into(),
        "--export-bytes".into(),
        "1048576".into(),
    ]
}
fn maintenance(fixture: &Fixture, kind: &str, operation: &[&str], success: bool) -> String {
    let mut args = vec!["v2".into(), kind.into()];
    args.extend(owner());
    args.extend(if kind.contains("exports") {
        exports(fixture)
    } else {
        copies(fixture)
    });
    args.extend(operation.iter().map(|v| v.to_string()));
    run(fixture.directory.path(), &args, success)
}
fn local(fixture: &Fixture, operation: &[&str], success: bool) -> String {
    let mut args = vec!["v2".into(), "local".into()];
    args.extend(fixture.journal(1, true));
    args.extend(copies(fixture));
    args.extend(operation.iter().map(|v| v.to_string()));
    run(fixture.directory.path(), &args, success)
}
fn export(fixture: &Fixture, work: &str, id: &str, success: bool) -> String {
    let mut args = vec![
        "export".into(),
        "--work".into(),
        work.into(),
        "--attempt".into(),
        "1".into(),
        "--index".into(),
        "0".into(),
        "--export-id".into(),
        id.into(),
    ];
    args.extend(exports(fixture));
    local(
        fixture,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        success,
    )
}

#[test]
fn managed_cli_download_and_offline_export_preserve_original_selection_and_fail_without_fallback() {
    let mut fixture = Fixture::new();
    fixture.initialize();
    let mut server = fixture.start();
    fixture.init_client(1, true);
    fixture.client(
        &server,
        1,
        true,
        "alice",
        &[
            "declare",
            "--operation",
            DECLARE,
            "--entities",
            "1,2",
            "--seal",
        ],
        true,
    );
    let bytes = vec![0xd3; 262145];
    let source = fixture.path("source");
    fs::write(&source, &bytes).unwrap();
    for (work, id) in [("0:0:1", ADMIT), ("0:0:2", CANCEL)] {
        fixture.client(
            &server,
            1,
            true,
            "alice",
            &[
                "admit",
                "--operation",
                id,
                "--declaration",
                DECLARE,
                "--work",
                work,
                "--input",
                &source,
                "--application",
                "copy/v2",
            ],
            true,
        );
        fixture.succeeded(&server, 1, true, work);
        fixture.client(
            &server,
            1,
            true,
            "alice",
            &["select", "--work", work, "--attempt", "1", "--index", "0"],
            true,
        );
    }
    let mut download = vec![
        "download".into(),
        "--work".into(),
        "0:0:1".into(),
        "--attempt".into(),
        "1".into(),
        "--index".into(),
        "0".into(),
    ];
    download.extend(copies(&fixture));
    let download: Vec<_> = download.iter().map(String::as_str).collect();
    assert!(
        fixture
            .client(&server, 1, true, "alice", &download, false)
            .contains("NOT_FOUND")
    );
    assert!(!Path::new(&fixture.path("copies")).exists());
    maintenance(&fixture, "init-results", &[], true);
    maintenance(&fixture, "init-results", &[], false);
    let saved = fixture.client(&server, 1, true, "rotated", &download, true);
    let copy_key = saved
        .lines()
        .find_map(|line| line.strip_prefix("DOWNLOADED "))
        .unwrap()
        .to_owned();
    assert!(
        fixture
            .client(&server, 1, true, "alice", &download, false)
            .contains("LIMIT_EXCEEDED")
    );
    let receipt = fixture.client(
        &server,
        1,
        true,
        "alice",
        &["lookup", "--operation", ADMIT],
        true,
    );
    let manifest = fixture.client(
        &server,
        1,
        true,
        "alice",
        &["manifest", "--work", "0:0:1", "--attempt", "1"],
        true,
    );
    server.stop();
    // No network configuration or credentials are supplied to any local command.
    assert!(
        local(
            &fixture,
            &[
                "verify",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0"
            ],
            true
        )
        .contains("LOCAL_VERIFIED")
    );
    assert!(export(&fixture, "0:0:1", DECLARE, false).contains("NOT_FOUND"));
    assert!(!Path::new(&fixture.path("exports")).exists());
    maintenance(&fixture, "init-exports", &[], true);
    maintenance(&fixture, "init-exports", &[], false);
    assert!(export(&fixture, "0:0:1", DECLARE, true).contains("replayed=false"));
    let raw = Path::new(&fixture.path("exports")).join(format!("file-{DECLARE}"));
    assert_eq!(fs::read(&raw).unwrap(), bytes);
    assert!(export(&fixture, "0:0:1", DECLARE, true).contains("replayed=true"));
    // Equal bytes under a different saved work identity may share a local copy,
    // but may not replace this export's immutable manifest/index commitment.
    assert!(export(&fixture, "0:0:2", DECLARE, false).contains("CONFLICT"));
    assert!(export(&fixture, "0:0:1", ADMIT, false).contains("LIMIT_EXCEEDED"));
    assert_eq!(fs::read(&raw).unwrap(), bytes);
    assert!(
        local(
            &fixture,
            &[
                "verify",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "1"
            ],
            false
        )
        .contains("NOT_FOUND")
    );
    maintenance(
        &fixture,
        "results",
        &["remove", "--copy-key", &copy_key],
        true,
    );
    assert!(
        local(
            &fixture,
            &[
                "verify",
                "--work",
                "0:0:1",
                "--attempt",
                "1",
                "--index",
                "0"
            ],
            false
        )
        .contains("NOT_FOUND")
    );
    assert!(export(&fixture, "0:0:1", DECLARE, false).contains("NOT_FOUND"));
    assert_eq!(fs::read(&raw).unwrap(), bytes);
    // Export cleanup does not depend on having the source copy or a live server.
    assert!(
        maintenance(
            &fixture,
            "exports",
            &["remove", "--export-id", DECLARE],
            true
        )
        .contains("REMOVED true")
    );
    assert!(
        maintenance(
            &fixture,
            "exports",
            &["remove", "--export-id", DECLARE],
            true
        )
        .contains("REMOVED false")
    );
    assert!(!raw.exists());
    assert!(maintenance(&fixture, "exports", &["usage"], true).contains("charged_bytes: 0"));
    let mut server = fixture.start();
    assert_eq!(
        receipt,
        fixture.client(
            &server,
            1,
            true,
            "alice",
            &["lookup", "--operation", ADMIT],
            true
        )
    );
    assert_eq!(
        manifest,
        fixture.client(
            &server,
            1,
            true,
            "alice",
            &["manifest", "--work", "0:0:1", "--attempt", "1"],
            true
        )
    );
    assert!(
        fixture
            .client(&server, 1, true, "alice", &["unresolved"], true)
            .contains("UNRESOLVED []")
    );
    server.stop();
}

#[test]
fn local_storage_commands_refuse_changed_owner_policy_and_unknown_files_without_cleanup() {
    let fixture = Fixture::new();
    for kind in ["results", "exports"] {
        assert!(maintenance(&fixture, kind, &["usage"], false).contains("NOT_FOUND"));
        maintenance(&fixture, &format!("init-{kind}"), &[], true);
        for changed_owner in [false, true] {
            let mut args = vec!["v2".into(), kind.into()];
            let mut labels = owner();
            if changed_owner {
                *labels.last_mut().unwrap() = "bob".into();
            }
            args.extend(labels);
            let mut storage = if kind == "exports" {
                exports(&fixture)
            } else {
                copies(&fixture)
            };
            if !changed_owner {
                *storage.last_mut().unwrap() = "1048577".into();
            }
            args.extend(storage);
            args.push("usage".into());
            assert!(run(fixture.directory.path(), &args, false).contains("INTEGRITY_ERROR"));
        }
        let root = fixture.path(if kind == "exports" {
            "exports"
        } else {
            "copies"
        });
        let unrelated = Path::new(&root).join("unrelated");
        fs::write(&unrelated, b"keep").unwrap();
        assert!(maintenance(&fixture, kind, &["usage"], false).contains("INTEGRITY_ERROR"));
        assert_eq!(fs::read(&unrelated).unwrap(), b"keep");
    }
}
