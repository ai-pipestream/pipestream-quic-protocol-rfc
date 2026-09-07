use super::*;
use crate::v2::authority::results::{ReadCursor, ResultRead, ResultService};

pub(super) fn service(fixture: &Fixture) -> ResultService {
    ResultService::new(fixture.store.clone(), fixture.payloads.clone()).unwrap()
}
pub(super) fn request(fixture: &Fixture) -> ResultMessage {
    ResultMessage::Read {
        request: Id(7),
        work: fixture.key(),
        attempt: Id(1),
        index: OutputIndex(0),
        expected_sha256: Digest(Sha256::digest(b"abc").into()),
    }
}
pub(super) fn published() -> Fixture {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    fixture
}
pub(super) fn drain(mut read: ResultRead, now: Instant) -> Vec<u8> {
    let header = read.start(now).unwrap();
    assert_eq!(header.request, Id(7));
    assert_eq!(header.length, Number(3));
    let mut bytes = [0; 2];
    let mut output = Vec::new();
    loop {
        let n = read.read_chunk(&mut bytes, now).unwrap();
        if n == 0 {
            break;
        }
        output.extend_from_slice(&bytes[..n]);
        read.sent(n, now).unwrap();
    }
    read.finish(now).unwrap();
    output
}

#[test]
fn result_reads_replay_exact_committed_bytes_without_executing_or_changing_work() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::new(Arc::new(CountCopy(calls.clone())));
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    let results = service(&fixture);
    let before = fixture.view();
    let job_before = fixture.job();
    let identity = &fixture.binding.identity;
    assert_eq!(
        results
            .manifest(identity, &fixture.key(), Id(1), &caps())
            .unwrap(),
        before.manifest.clone().unwrap()
    );
    let now = Instant::now();
    let mut interrupted = results
        .begin_read(identity, &request(&fixture), &caps(), now)
        .unwrap();
    interrupted.start(now).unwrap();
    assert_eq!(interrupted.read_chunk(&mut [0; 1], now).unwrap(), 1);
    drop(interrupted); // Reset/lost connection is only an aborted delivery.
    assert_eq!(results.pending().unwrap(), 0);
    for _ in 0..3 {
        assert_eq!(
            drain(
                results
                    .begin_read(identity, &request(&fixture), &caps(), now)
                    .unwrap(),
                now
            ),
            b"abc"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.view(), before);
    assert_eq!(fixture.job(), job_before);
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn result_lookup_and_requests_preserve_named_identity_state_and_commitment_refusals() {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(0, 1, 3);
    let results = service(&fixture);
    let identity = &fixture.binding.identity;
    let now = Instant::now();
    refuse(
        results.manifest(identity, &fixture.key(), Id(1), &caps()),
        ErrorCode::NotReady,
    );
    refuse(
        results.begin_read(identity, &request(&fixture), &caps(), now),
        ErrorCode::NotReady,
    );
    fixture.run().unwrap();
    for case in [
        "attempt",
        "work",
        "index",
        "digest",
        "profile",
        "object",
        "generation",
        "owner",
        "authority",
        "message",
    ] {
        let mut parameters = request(&fixture);
        let mut supplied = identity.clone();
        let mut selected = caps();
        let ResultMessage::Read {
            attempt,
            work,
            index,
            expected_sha256,
            ..
        } = &mut parameters
        else {
            unreachable!()
        };
        let expected = match case {
            "attempt" => {
                *attempt = Id(2);
                ErrorCode::NotFound
            }
            "work" => {
                work.entity = Id(2);
                ErrorCode::NotFound
            }
            "index" => {
                *index = OutputIndex(1);
                ErrorCode::NotFound
            }
            "digest" => {
                *expected_sha256 = Digest([0; 32]);
                ErrorCode::IntegrityError
            }
            "profile" => {
                selected.supported = vec![ProfileId(DURABLE_WORK.into())];
                ErrorCode::ExtensionUnsupported
            }
            "object" => {
                selected.object_limit = Number(2);
                ErrorCode::LimitExceeded
            }
            "generation" => {
                supplied.generation = Id(2);
                ErrorCode::NotFound
            }
            "owner" => {
                supplied.owner = IdentityLabel("bob".into());
                ErrorCode::Unauthorized
            }
            "authority" => {
                supplied.authority = IdentityLabel("elsewhere".into());
                ErrorCode::Unauthorized
            }
            "message" => {
                parameters = ResultMessage::GetManifest {
                    request: Id(7),
                    work: fixture.key(),
                    attempt: Id(1),
                };
                ErrorCode::FrameError
            }
            _ => unreachable!(),
        };
        refuse(
            results.begin_read(&supplied, &parameters, &selected, now),
            expected,
        );
        assert_eq!(results.pending().unwrap(), 0, "{case}");
    }
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn existing_read_uses_monotonic_lifetime_not_output_expiry_or_later_unsafe_utc() {
    let fixture = published();
    let results = service(&fixture);
    let identity = &fixture.binding.identity;
    let until = fixture.view().output_until.unwrap().0;
    fixture.clock.0.store(until - 1, Ordering::SeqCst);
    let now = Instant::now();
    let read = results
        .begin_read(identity, &request(&fixture), &caps(), now)
        .unwrap();
    fixture.clock.0.store(until, Ordering::SeqCst);
    refuse(
        results.begin_read(identity, &request(&fixture), &caps(), now),
        ErrorCode::Expired,
    );
    let manifest = results
        .manifest(identity, &fixture.key(), Id(1), &caps())
        .unwrap();
    assert_eq!(manifest.available_until, Number(until));
    fixture.clock.0.store(999, Ordering::SeqCst);
    refuse(
        results.begin_read(identity, &request(&fixture), &caps(), now),
        ErrorCode::ClockUnsafe,
    );
    assert_eq!(
        results
            .manifest(identity, &fixture.key(), Id(1), &caps())
            .unwrap(),
        manifest
    );
    assert_eq!(drain(read, now + Elapsed::from_millis(1)), b"abc");
    fixture.clock.0.store(until, Ordering::SeqCst);
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn zero_outputs_are_a_real_manifest_but_no_object_and_failed_work_has_no_manifest() {
    for failed in [false, true] {
        let fixture = Fixture::new(if failed {
            Arc::new(Broken(0))
        } else {
            Arc::new(EmptyOutputs(0))
        });
        fixture.admit(0, 0, 0);
        fixture.run().unwrap();
        let results = service(&fixture);
        let manifest = results.manifest(&fixture.binding.identity, &fixture.key(), Id(1), &caps());
        if failed {
            refuse(manifest, ErrorCode::NotReady);
        } else {
            assert!(manifest.unwrap().outputs.is_empty());
        }
        refuse(
            results.begin_read(
                &fixture.binding.identity,
                &request(&fixture),
                &caps(),
                Instant::now(),
            ),
            if failed {
                ErrorCode::NotReady
            } else {
                ErrorCode::NotFound
            },
        );
    }
}

#[test]
fn result_deadlines_include_pending_time_empty_progress_and_fin_at_equality() {
    for case in ["pending", "empty", "buffered", "lifetime", "regression"] {
        let fixture = published();
        let results = service(&fixture);
        let mut selected = caps();
        selected.stream_idle_ms = IdleMs(2000);
        selected.stream_lifetime_ms = LifetimeMs(5000);
        let now = Instant::now();
        let mut read = results
            .begin_read(
                &fixture.binding.identity,
                &request(&fixture),
                &selected,
                now,
            )
            .unwrap();
        if case == "pending" {
            refuse(
                read.start(now + Elapsed::from_secs(2)),
                ErrorCode::LimitExceeded,
            );
        } else {
            read.start(now).unwrap();
            read.read_chunk(&mut [0; 2], now).unwrap();
            match case {
                "empty" => {
                    read.sent(0, now + Elapsed::from_secs(1)).unwrap();
                    refuse(
                        read.sent(1, now + Elapsed::from_secs(2)),
                        ErrorCode::LimitExceeded,
                    );
                }
                "buffered" => {
                    // Reading bytes from disk is not evidence of network progress.
                    refuse(
                        read.sent(2, now + Elapsed::from_secs(2)),
                        ErrorCode::LimitExceeded,
                    );
                }
                "lifetime" => {
                    read.sent(1, now + Elapsed::from_secs(1)).unwrap();
                    read.sent(1, now + Elapsed::from_secs(2)).unwrap();
                    read.read_chunk(&mut [0; 2], now + Elapsed::from_secs(2))
                        .unwrap();
                    read.sent(1, now + Elapsed::from_secs(3)).unwrap();
                    assert_eq!(
                        read.read_chunk(&mut [0; 2], now + Elapsed::from_secs(4))
                            .unwrap(),
                        0
                    );
                    refuse(
                        read.finish(now + Elapsed::from_secs(5)),
                        ErrorCode::LimitExceeded,
                    );
                }
                "regression" => {
                    read.sent(1, now + Elapsed::from_secs(1)).unwrap();
                    refuse(read.sent(1, now), ErrorCode::ClockUnsafe);
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(results.pending().unwrap(), 0);
        assert_eq!(fixture.view().state, State::SUCCEEDED);
    }
}

#[test]
fn misuse_cannot_buffer_multiple_chunks_overstate_send_progress_or_skip_eof() {
    for case in [
        "unstarted",
        "twice",
        "pending",
        "overreport",
        "finish",
        "buffer",
        "empty",
    ] {
        let fixture = published();
        let results = service(&fixture);
        let now = Instant::now();
        let mut read = results
            .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
            .unwrap();
        if case == "unstarted" {
            refuse(read.read_chunk(&mut [0; 2], now), ErrorCode::Conflict);
        } else {
            read.start(now).unwrap();
            match case {
                "twice" => refuse(read.start(now), ErrorCode::Conflict),
                "pending" => {
                    read.read_chunk(&mut [0; 2], now).unwrap();
                    refuse(read.read_chunk(&mut [0; 2], now), ErrorCode::Conflict);
                }
                "overreport" => {
                    read.read_chunk(&mut [0; 2], now).unwrap();
                    refuse(read.sent(3, now), ErrorCode::IntegrityError);
                }
                "finish" => refuse(read.finish(now), ErrorCode::IntegrityError),
                "buffer" => {
                    let mut bytes = vec![0; read.buffer_limit().unwrap() + 1];
                    refuse(read.read_chunk(&mut bytes, now), ErrorCode::LimitExceeded);
                }
                "empty" => refuse(read.read_chunk(&mut [], now), ErrorCode::LimitExceeded),
                _ => unreachable!(),
            }
        }
        assert_eq!(results.pending().unwrap(), 0);
    }
}

#[test]
fn maintenance_expires_held_reads_in_bounded_batches_and_reclaims_their_handles() {
    let fixture = published();
    let first = service(&fixture);
    let second = service(&fixture);
    let now = Instant::now();
    let identity = &fixture.binding.identity;
    let mut held = Vec::new();
    for _ in 0..8 {
        held.push(
            first
                .begin_read(identity, &request(&fixture), &caps(), now)
                .unwrap(),
        );
    }
    refuse(
        second.begin_read(identity, &request(&fixture), &caps(), now),
        ErrorCode::LimitExceeded,
    );
    let mut cursor = ReadCursor::default();
    for left in (0..8).rev() {
        let report = first
            .maintain(&mut cursor, 1, now + Elapsed::from_millis(1000))
            .unwrap();
        assert_eq!((report.inspected, report.closed, report.busy), (1, 1, 0));
        assert_eq!(first.pending().unwrap(), left);
    }
    for read in &mut held {
        refuse(
            read.start(now + Elapsed::from_millis(1000)),
            ErrorCode::LimitExceeded,
        );
    }
    assert_eq!(
        drain(
            second
                .begin_read(identity, &request(&fixture), &caps(), now)
                .unwrap(),
            now
        ),
        b"abc"
    );
    for invalid in [0, 257] {
        refuse(
            first.maintain(&mut cursor, invalid, now),
            ErrorCode::LimitExceeded,
        );
    }
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn revocation_and_authorization_withdrawal_stop_pending_and_live_result_reads() {
    for revoke in [false, true] {
        let fixture = published();
        let results = service(&fixture);
        let now = Instant::now();
        let identity = &fixture.binding.identity;
        let mut pending = results
            .begin_read(identity, &request(&fixture), &caps(), now)
            .unwrap();
        let mut live = results
            .begin_read(identity, &request(&fixture), &caps(), now)
            .unwrap();
        live.start(now).unwrap();
        live.read_chunk(&mut [0; 1], now).unwrap();
        if revoke {
            fixture.store.revoke_session(identity).unwrap();
        } else {
            fixture.auth.0.store(false, Ordering::SeqCst);
        }
        let report = results
            .maintain(&mut ReadCursor::default(), 2, now)
            .unwrap();
        assert_eq!(report.closed, 2);
        refuse(pending.start(now), ErrorCode::Unauthorized);
        refuse(live.check_deadline(now), ErrorCode::Unauthorized);
        refuse(live.sent(1, now), ErrorCode::Unauthorized);
        refuse(
            results.manifest(identity, &fixture.key(), Id(1), &caps()),
            ErrorCode::Unauthorized,
        );
        refuse(
            results.begin_read(identity, &request(&fixture), &caps(), now),
            ErrorCode::Unauthorized,
        );
        fixture.auth.0.store(true, Ordering::SeqCst);
        fixture.reopen().integrity_check().unwrap();
    }
}

#[test]
fn dropping_last_result_service_aborts_even_retained_transfer_handles() {
    let fixture = published();
    let results = service(&fixture);
    let clone = results.clone();
    let now = Instant::now();
    let mut read = results
        .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
        .unwrap();
    drop(results);
    read.start(now).unwrap();
    drop(clone);
    refuse(read.read_chunk(&mut [0; 2], now), ErrorCode::Cancelled);
    let replacement = service(&fixture);
    let held: Vec<_> = (0..8)
        .map(|_| {
            replacement
                .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
                .unwrap()
        })
        .collect();
    assert_eq!(held.len(), 8);
}

#[test]
fn delayed_maintenance_snapshot_does_not_misdiagnose_newer_send_progress_as_clock_regression() {
    let fixture = published();
    let results = service(&fixture);
    let now = Instant::now();
    let mut read = results
        .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
        .unwrap();
    read.start(now).unwrap();
    read.read_chunk(&mut [0; 2], now).unwrap();
    read.sent(2, now + Elapsed::from_millis(500)).unwrap();
    // The timer captured its timestamp before being descheduled; the foreground
    // writer made progress before maintenance acquired this lease's lock.
    let report = results
        .maintain(&mut ReadCursor::default(), 1, now)
        .unwrap();
    assert_eq!(report.closed, 0);
    assert_eq!(results.pending().unwrap(), 1);
    assert_eq!(
        read.read_chunk(&mut [0; 2], now + Elapsed::from_millis(501))
            .unwrap(),
        1
    );
}

struct ReadPolicy {
    calls: AtomicUsize,
    allowed_calls: usize,
}
impl Authorization for ReadPolicy {
    fn permits(&self, owner: &IdentityLabel, permission: Permission) -> bool {
        owner.0 == "alice"
            && (permission != Permission::ReadResult
                || self.calls.fetch_add(1, Ordering::SeqCst) < self.allowed_calls)
    }
}
#[test]
fn result_permission_is_distinct_and_late_withdrawal_rolls_back_the_read_grant() {
    for allowed_calls in [0, 1] {
        let fixture = published();
        let restricted = AuthorityStore::open(
            &fixture.directory.path().join("authority.sqlite"),
            fixture.store.authority.clone(),
            fixture.store.policy.clone(),
            PhysicalLimits::default(),
            fixture.clock.clone(),
            Arc::new(ReadPolicy {
                calls: AtomicUsize::new(0),
                allowed_calls,
            }),
        )
        .unwrap();
        let results = ResultService::new(restricted.clone(), fixture.payloads.clone()).unwrap();
        fixture.clock.0.store(1001, Ordering::SeqCst);
        refuse(
            results.begin_read(
                &fixture.binding.identity,
                &request(&fixture),
                &caps(),
                Instant::now(),
            ),
            ErrorCode::Unauthorized,
        );
        assert_eq!(results.pending().unwrap(), 0);
        // Metadata inspection remains authorized; no output-read permission is
        // inferred from the ability to inspect work or execute its application.
        restricted
            .work_view(&fixture.binding.identity, &fixture.key(), Number(0))
            .unwrap();
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        let (_, greatest): (_, Number) = records::read(&tx, records::CLOCK).unwrap();
        assert_eq!(greatest, Number(1000));
        drop(tx);
        let open = service(&fixture);
        let held: Vec<_> = (0..8)
            .map(|_| {
                open.begin_read(
                    &fixture.binding.identity,
                    &request(&fixture),
                    &caps(),
                    Instant::now(),
                )
                .unwrap()
            })
            .collect();
        assert_eq!(held.len(), 8); // Refusal did not retain a pending file pin.
        fixture.reopen().integrity_check().unwrap();
    }
}

fn output_path(fixture: &Fixture) -> std::path::PathBuf {
    let input_name = format!("object-{}", fixture.job().input_key.0);
    let mut paths: Vec<_> = std::fs::read_dir(fixture.directory.path().join("objects"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            name.starts_with("object-") && name != input_name
        })
        .collect();
    assert_eq!(paths.len(), 1);
    paths.pop().unwrap()
}
#[test]
fn missing_or_corrupt_output_never_reexecutes_or_replaces_the_committed_manifest() {
    use std::io::{Seek, SeekFrom, Write};
    for case in ["missing", "header", "body", "truncated", "trailing"] {
        let calls = Arc::new(AtomicUsize::new(0));
        let fixture = Fixture::new(Arc::new(CountCopy(calls.clone())));
        fixture.admit(0, 1, 3);
        fixture.run().unwrap();
        let results = service(&fixture);
        let before = fixture.view();
        let path = output_path(&fixture);
        let now = Instant::now();
        let active = if matches!(case, "body" | "truncated" | "trailing") {
            Some(
                results
                    .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
                    .unwrap(),
            )
        } else {
            None
        };
        if case == "missing" {
            std::fs::remove_file(path).unwrap();
        } else {
            let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            match case {
                "header" => {
                    file.write_all(&[0; 8]).unwrap();
                }
                "body" => {
                    file.seek(SeekFrom::End(-1)).unwrap();
                    file.write_all(b"x").unwrap();
                }
                "truncated" => {
                    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
                }
                "trailing" => {
                    file.seek(SeekFrom::End(0)).unwrap();
                    file.write_all(b"x").unwrap();
                }
                _ => unreachable!(),
            }
            file.sync_all().unwrap();
        }
        if let Some(mut read) = active {
            read.start(now).unwrap();
            loop {
                match read.read_chunk(&mut [0; 2], now) {
                    Ok(0) => panic!("corrupt object was verified"),
                    Ok(n) => read.sent(n, now).unwrap(),
                    Err(error) => {
                        refuse::<()>(Err(error), ErrorCode::OutputUnavailable);
                        break;
                    }
                }
            }
            refuse(read.finish(now), ErrorCode::OutputUnavailable);
        } else {
            refuse(
                results.begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now),
                ErrorCode::OutputUnavailable,
            );
        }
        assert_eq!(results.pending().unwrap(), 0);
        assert_eq!(
            results
                .manifest(&fixture.binding.identity, &fixture.key(), Id(1), &caps())
                .unwrap(),
            before.manifest.clone().unwrap()
        );
        assert_eq!(fixture.view(), before);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        fixture.reopen().integrity_check().unwrap();
    }
}

#[test]
fn result_crash_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_RESULT_CHILD_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let store = AuthorityStore::open(
        &path.join("authority.sqlite"),
        IdentityLabel("test-authority".into()),
        super::super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(TestClock(AtomicU64::new(1001))),
        Arc::new(Auth(AtomicBool::new(true))),
    )
    .unwrap();
    let payloads = PayloadStore::open(
        &path.join("objects"),
        store.payload_identity().unwrap(),
        payload_policy(),
    )
    .unwrap();
    let identity = SessionIdentity {
        authority: store.authority.clone(),
        owner: IdentityLabel("alice".into()),
        generation: Id(1),
    };
    let results = ResultService::new(store, payloads).unwrap();
    let request = ResultMessage::Read {
        request: Id(7),
        work: WorkKey {
            scope: Number(0),
            producer: Producer(0),
            entity: Id(1),
        },
        attempt: Id(1),
        index: OutputIndex(0),
        expected_sha256: Digest(Sha256::digest(b"abc").into()),
    };
    let now = Instant::now();
    let mut read = results
        .begin_read(&identity, &request, &caps(), now)
        .unwrap();
    read.start(now).unwrap();
    assert_eq!(read.read_chunk(&mut [0; 2], now).unwrap(), 2);
    super::super::super::tests::crash_boundary("result-stream", "after-chunk");
    panic!("result crash boundary did not fire");
}
#[test]
fn result_process_death_drops_only_delivery_and_reopens_exact_published_output() {
    for boundary in [
        "result-read:before",
        "result-read:after",
        "result-stream:after-chunk",
    ] {
        let fixture = published();
        let before = fixture.view();
        let request = request(&fixture);
        let Fixture {
            directory,
            store,
            payloads,
            binding,
            clock,
            auth,
            executor,
        } = fixture;
        drop(executor);
        drop(payloads);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::execution::tests::result_tests::result_crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_RESULT_CHILD_DIRECTORY", directory.path())
            .env("PIPESTREAM_TEST_AUTHORITY_CRASH", boundary)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{boundary}");
        clock.0.store(1001, Ordering::SeqCst);
        let store = AuthorityStore::open(
            &directory.path().join("authority.sqlite"),
            store.authority.clone(),
            store.policy.clone(),
            PhysicalLimits::default(),
            clock,
            auth,
        )
        .unwrap();
        let payloads = PayloadStore::open(
            &directory.path().join("objects"),
            store.payload_identity().unwrap(),
            payload_policy(),
        )
        .unwrap();
        let results = ResultService::new(store.clone(), payloads).unwrap();
        assert_eq!(results.pending().unwrap(), 0);
        assert_eq!(
            store
                .work_view(&binding.identity, &request_work(&request), Number(0))
                .unwrap()
                .1,
            before
        );
        let now = Instant::now();
        assert_eq!(
            drain(
                results
                    .begin_read(&binding.identity, &request, &caps(), now)
                    .unwrap(),
                now
            ),
            b"abc"
        );
        store.integrity_check().unwrap();
    }
}
fn request_work(request: &ResultMessage) -> WorkKey {
    let ResultMessage::Read { work, .. } = request else {
        unreachable!()
    };
    work.clone()
}

#[test]
fn per_owner_read_limits_apply_before_the_global_handle_ceiling() {
    let mut policy = payload_policy();
    policy.handles = Id(16);
    policy.owner_handles = Id(4);
    let fixture = Fixture::setup(
        Arc::new(CopyApplication),
        caps(),
        PhysicalLimits::default(),
        1,
        Policy {
            execution_limit_ms: Duration(10000),
            output_retention_ms: Duration(20000),
            receipt_retention_ms: Duration(30000),
        },
        policy,
    );
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    let results = service(&fixture);
    let now = Instant::now();
    let held: Vec<_> = (0..4)
        .map(|_| {
            results
                .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
                .unwrap()
        })
        .collect();
    refuse(
        service(&fixture).begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now),
        ErrorCode::LimitExceeded,
    );
    drop(held);
    assert_eq!(results.pending().unwrap(), 0);
}

#[test]
fn admitted_result_delivery_needs_no_further_database_writes_or_publication_credit() {
    let physical = PhysicalLimits {
        wal_bytes: 4 << 20,
        ..PhysicalLimits::default()
    };
    let fixture = Fixture::configured(Arc::new(CopyApplication), caps(), physical);
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    let results = service(&fixture);
    let now = Instant::now();
    let read = results
        .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
        .unwrap();
    let original = fixture.view();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    records::protect(&tx, 0, 0).unwrap();
    tx.execute_batch("CREATE TABLE result_fill(body BLOB);
        CREATE TRIGGER forbid_work_update BEFORE UPDATE ON work BEGIN SELECT RAISE(ABORT,'work row replacement'); END;
        CREATE TRIGGER forbid_job_update BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT,'job row replacement'); END;
        CREATE TRIGGER forbid_clock_update BEFORE UPDATE ON authority BEGIN SELECT RAISE(ABORT,'clock row replacement'); END;").unwrap();
    tx.commit().unwrap();
    let mut reader = fixture.store.connect().unwrap();
    let snapshot = reader.transaction().unwrap();
    snapshot
        .query_row("SELECT count(*) FROM work", [], |r| r.get::<_, i64>(0))
        .unwrap();
    let mut filled = 0;
    loop {
        let result = (|| -> Result<()> {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            records::protect(&tx, 0, 0)?;
            tx.execute("INSERT INTO result_fill VALUES(zeroblob(4096))", [])?;
            tx.commit()?;
            Ok(())
        })();
        if result.is_err() {
            refuse(result, ErrorCode::LimitExceeded);
            break;
        }
        filled += 1;
        assert!(filled < 4096);
    }
    // A refused table insert can leave enough ordinary headroom for a smaller
    // clock rewrite. Exhaust that exact write shape too, without spending a
    // reserved transition credit, before testing refusal of a fresh read lease.
    let mut ticks = 1000;
    loop {
        ticks += 1;
        fixture.clock.0.store(ticks, Ordering::SeqCst);
        let result = (|| -> Result<()> {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            fixture.store.remember_clock(&tx, Number(ticks))?;
            tx.commit()?;
            Ok(())
        })();
        if result.is_err() {
            refuse(result, ErrorCode::LimitExceeded);
            break;
        }
        assert!(ticks < 10000);
    }
    let pages: i64 = connection
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let before = fixture.store.physical_usage().unwrap();
    refuse(
        results.begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now),
        ErrorCode::LimitExceeded,
    );
    assert_eq!(drain(read, now), b"abc");
    assert_eq!(results.pending().unwrap(), 0);
    assert_eq!(fixture.view(), original);
    let after = fixture.store.physical_usage().unwrap();
    assert_eq!(before.wal_bytes, after.wal_bytes);
    assert_eq!(
        connection
            .query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        pages
    );
    eprintln!(
        "result delivery: ordinary_fill={filled} pages={pages} WAL={} -> {} cap={}",
        before.wal_bytes, after.wal_bytes, physical.wal_bytes
    );
    fixture.store.integrity_check().unwrap();
}

#[test]
fn a_zero_byte_output_is_delivered_with_verified_empty_digest_and_fin() {
    let fixture = Fixture::new(Arc::new(EmptyOutputs(1)));
    fixture.admit(0, 1, 0);
    fixture.run().unwrap();
    let results = service(&fixture);
    let now = Instant::now();
    let mut parameters = request(&fixture);
    let ResultMessage::Read {
        expected_sha256, ..
    } = &mut parameters
    else {
        unreachable!()
    };
    *expected_sha256 = Digest(Sha256::digest([]).into());
    let mut read = results
        .begin_read(&fixture.binding.identity, &parameters, &caps(), now)
        .unwrap();
    let header = read.start(now).unwrap();
    assert_eq!(header.length, Number(0));
    assert_eq!(header.sha256, Digest(Sha256::digest([]).into()));
    assert_eq!(read.read_chunk(&mut [0; 1], now).unwrap(), 0);
    read.check_deadline(now).unwrap();
    read.finish(now).unwrap();
    assert_eq!(results.pending().unwrap(), 0);
}

#[test]
fn cancelling_unresolved_scope_work_does_not_revoke_already_committed_results() {
    let fixture = published();
    let before = fixture.view();
    fixture
        .store
        .cancel_scope(&fixture.binding.identity, OperationId([88; 16]), Number(0))
        .unwrap();
    let results = service(&fixture);
    let now = Instant::now();
    assert_eq!(
        drain(
            results
                .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
                .unwrap(),
            now
        ),
        b"abc"
    );
    assert_eq!(fixture.view(), before);
}

#[test]
fn continuous_new_reads_cannot_starve_expiry_of_an_older_held_lease() {
    let fixture = published();
    let results = service(&fixture);
    let now = Instant::now();
    let mut old = results
        .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
        .unwrap();
    let mut cursor = ReadCursor::default();
    assert_eq!(results.maintain(&mut cursor, 1, now).unwrap().closed, 0);
    for second in 1..=4 {
        let tick = now + Elapsed::from_secs(second);
        let fresh = results
            .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), tick)
            .unwrap();
        results.maintain(&mut cursor, 1, tick).unwrap();
        drop(fresh);
    }
    assert_eq!(
        results.pending().unwrap(),
        0,
        "new arrivals kept the cursor past the expired lease"
    );
    refuse(
        old.start(now + Elapsed::from_secs(4)),
        ErrorCode::LimitExceeded,
    );
}
