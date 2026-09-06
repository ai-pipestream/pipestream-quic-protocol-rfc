use super::*;

fn unhex(s: &str) -> Vec<u8> {
    assert_eq!(s.len() % 2, 0);
    s.as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}

fn rows() -> impl Iterator<Item = Vec<&'static str>> {
    include_str!("../../../../test-vectors/v2/wire.tsv")
        .lines()
        .skip(1)
        .map(|s| s.split('\t').collect())
}

fn fixture(name: &str) -> Vec<u8> {
    unhex(rows().find(|r| r[0] == name).expect("frozen fixture")[7])
}

#[test]
fn every_frozen_wire_case_has_exact_typed_roundtrip_or_named_refusal() {
    let mut count = 0;
    for row in rows() {
        let bytes = unhex(row[7]);
        let result = match row[2] {
            "input-header" => InputHeader::decode_framed(&bytes).and_then(|v| v.encode_framed()),
            "result-header" => ResultHeader::decode_framed(&bytes).and_then(|v| v.encode_framed()),
            "record" if row[1] == "v2-result-manifest" => {
                Manifest::decode(&bytes).and_then(|v| v.encode())
            }
            "record" => ScopeSummary::decode(&bytes).and_then(|v| v.encode()),
            _ => {
                Control::decode(&bytes, MAX_CONTROL_LIMIT).and_then(|v| v.encode(MAX_CONTROL_LIMIT))
            }
        };
        if row[4] == "accept" {
            assert_eq!(
                result.unwrap_or_else(|e| panic!("{}: {e}", row[0])),
                bytes,
                "{}",
                row[0]
            );
        } else {
            assert_eq!(result.expect_err(row[0]).code.name(), row[5], "{}", row[0]);
        }
        count += 1;
    }
    assert_eq!(count, 70);
}

fn caps(name: &str) -> Capabilities {
    let Control::Capabilities(c) = Control::decode(&fixture(name), MAX_CONTROL_LIMIT).unwrap()
    else {
        panic!("capabilities fixture")
    };
    c
}

fn core_selection() -> Capabilities {
    let offer = caps("core-only-capabilities");
    Capabilities::select(&offer, &offer, false).unwrap()
}

#[test]
fn every_accepted_frame_rejects_all_truncations_and_trailing_bytes() {
    for row in rows().filter(|r| r[4] == "accept") {
        let bytes = unhex(row[7]);
        let check = |b: &[u8]| -> Result<(), Error> {
            match row[2] {
                "input-header" => InputHeader::decode_framed(b).map(|_| ()),
                "result-header" => ResultHeader::decode_framed(b).map(|_| ()),
                "record" if row[1] == "v2-result-manifest" => Manifest::decode(b).map(|_| ()),
                "record" => ScopeSummary::decode(b).map(|_| ()),
                _ => Control::decode(b, MAX_CONTROL_LIMIT).map(|_| ()),
            }
        };
        for cut in 0..bytes.len() {
            assert!(check(&bytes[..cut]).is_err(), "{} cut {cut}", row[0]);
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(check(&extra).is_err(), "{} trailing", row[0]);
    }
}

#[test]
fn decoder_rejects_forbidden_types_and_untrusted_allocation_lengths() {
    for body in [
        vec![0xa0],
        vec![0xc0, 0x80],
        vec![0xf6],
        vec![0xf7],
        vec![0xf9, 0, 0],
        vec![0x9f, 3, 1, 0xff],
        vec![0x82, 3, 0x18, 1],
        vec![0x82, 3, 0x20],
        vec![0x82, 3, 0x61, 0xff],
        vec![0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        vec![0x98, 2, 3, 1],
    ] {
        let mut frame = vec![2];
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend(body);
        assert_eq!(
            Control::decode(&frame, INITIAL_CONTROL_LIMIT)
                .unwrap_err()
                .code,
            ErrorCode::FrameError
        );
    }
    assert!(Control::decode(&[2, 0xff, 0xff, 0xff, 0xff], MAX_CONTROL_LIMIT).is_err());
    for length in [0, 4097, u32::MAX] {
        assert!(object_header_length(length.to_be_bytes()).is_err());
    }
    assert_eq!(object_header_length(4096u32.to_be_bytes()).unwrap(), 4096);
    assert_eq!(control_body_length([1, 0, 0, 0x10, 0], None).unwrap(), 4096);
    assert!(control_body_length([1, 0, 0, 0x10, 1], Some(MAX_CONTROL_LIMIT)).is_err());
    assert!(control_body_length([2, 0, 0, 0, 2], None).is_err());
    assert!(control_body_length([2, 0xff, 0xff, 0xff, 0xff], Some(MAX_CONTROL_LIMIT)).is_err());
}

#[test]
fn unknown_control_classes_and_negotiation_phase_are_distinct() {
    for kind in 0..=255 {
        let frame = [kind, 0, 0, 0, 0];
        let result = Control::decode(&frame, INITIAL_CONTROL_LIMIT);
        match kind {
            0x80..=0xbf => {
                let parsed = result.unwrap();
                assert_eq!(parsed.encode(INITIAL_CONTROL_LIMIT).unwrap(), frame);
                assert!(parsed.validate_context(true, None).is_err());
                parsed
                    .validate_context(true, Some(&core_selection()))
                    .unwrap();
            }
            0xc0..=0xff => assert_eq!(result.unwrap_err().code, ErrorCode::ExtensionUnsupported),
            _ => assert_eq!(result.unwrap_err().code, ErrorCode::FrameError),
        }
    }
    let offer = Control::Capabilities(caps("capabilities-offer"));
    offer.validate_context(true, None).unwrap();
    assert!(offer.validate_context(false, None).is_err());
    assert!(
        Control::decode(&fixture("session-open"), MAX_CONTROL_LIMIT)
            .unwrap()
            .validate_context(true, Some(&caps("capabilities-offer")))
            .is_err()
    );
    assert!(
        offer
            .validate_context(true, Some(&caps("capabilities-response")))
            .is_err()
    );
    let request = Control::decode(&fixture("session-open"), MAX_CONTROL_LIMIT).unwrap();
    assert_eq!(
        request
            .validate_context(true, Some(&core_selection()))
            .unwrap_err()
            .code,
        ErrorCode::ExtensionUnsupported
    );
    assert_eq!(
        request
            .validate_context(false, Some(&caps("capabilities-response")))
            .unwrap_err()
            .code,
        ErrorCode::FrameError
    );
}

#[test]
fn negotiation_checks_authentication_required_sets_dependencies_and_every_limit() {
    let offer = caps("capabilities-offer");
    let selected = Capabilities::select(&offer, &offer, true).unwrap();
    offer.validate_selection(&selected).unwrap();
    assert_eq!(
        Capabilities::select(&offer, &offer, false)
            .unwrap_err()
            .code,
        ErrorCode::Unauthorized
    );
    let mut optional = offer.clone();
    optional.required.clear();
    let anonymous = Capabilities::select(&optional, &optional, false).unwrap();
    assert!(anonymous.supported.is_empty());
    let mut unknown = optional.clone();
    unknown.supported.insert(0, ProfileId(12));
    assert!(
        !Capabilities::select(&unknown, &unknown, true)
            .unwrap()
            .supported
            .contains(&ProfileId(12))
    );
    unknown.required.push(ProfileId(12));
    assert_eq!(
        Capabilities::select(&unknown, &unknown, true)
            .unwrap_err()
            .code,
        ErrorCode::ExtensionUnsupported
    );
    let mut omitted = selected.clone();
    omitted.required.clear();
    assert!(offer.validate_selection(&omitted).is_err());
    for field in 0..6 {
        let mut increased = selected.clone();
        match field {
            0 => increased.control_limit.0 += 1,
            1 => increased.stream_limit.0 += 1,
            2 => increased.pending_limit.0 += 1,
            3 => increased.object_limit.0 += 1,
            4 => increased.stream_idle_ms.0 += 1,
            _ => increased.stream_lifetime_ms.0 += 1,
        }
        assert!(
            offer.validate_selection(&increased).is_err(),
            "limit {field}"
        );
    }
    let mut legacy = selected;
    legacy.supported.insert(0, ProfileId(65281));
    assert!(offer.validate_selection(&legacy).is_err());
}

#[test]
fn output_locators_preserve_bytes_but_parse_case_and_all_numeric_boundaries() {
    let path = "/v2/sessions/9223372036854775807/scopes/9223372036854775807/producers/1/entities/9223372036854775807/attempts/9223372036854775807/outputs/255";
    let locator = ResultLocator(format!("PIPESTREAM://[2001:db8::1]:65535{path}"));
    let target = locator.target().unwrap();
    assert_eq!(target.generation.0, MAX_NUMBER);
    assert_eq!(target.port, 65535);
    assert_eq!(target.index.0, 255);
    let base = "pipestream://Processor.example:9443/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0";
    assert_eq!(
        ResultLocator(base.into()).target().unwrap().host,
        "processor.example"
    );
    for (from, to) in [
        (":9443", ""),
        (":9443", ":09443"),
        (":9443", ":0"),
        ("Processor.example", "user@Processor.example"),
        ("Processor.example", "[fe80::1%25eth0]"),
        ("Processor.example", "bad..name"),
        ("Processor.example", "-bad.example"),
        ("/v2/", "/v1/"),
        ("/sessions/1", "/sessions/0"),
        ("/sessions/1", "/sessions/9223372036854775808"),
        ("/producers/0", "/producers/2"),
        ("/entities/1", "/entities/01"),
        ("/attempts/1", "/attempts/0"),
        ("/outputs/0", "/outputs/256"),
        ("/outputs/0", "/outputs/0?token=x"),
        ("/outputs/0", "/outputs/0/"),
    ] {
        assert!(
            ResultLocator(base.replace(from, to)).target().is_err(),
            "{from} -> {to}"
        );
    }
}

#[test]
fn work_view_cross_fields_and_profile_requirements_cannot_be_fabricated() {
    let Control::Work(Work::View { work, .. }) =
        Control::decode(&fixture("work-success-view"), MAX_CONTROL_LIMIT).unwrap()
    else {
        panic!("work view")
    };
    work.validate_profiles(true).unwrap();
    assert!(work.validate_profiles(false).is_err());
    for mutation in 0..11 {
        let mut bad = work.clone();
        match mutation {
            0 => bad.attempt = Number(0),
            1 => bad.deadline = bad.admitted_at,
            2 => bad.receipt_until = bad.terminal_at,
            3 => bad.state = State::ACTIVE,
            4 => bad.state = State::FAILED,
            5 => bad.output_until = None,
            6 => bad.manifest.as_mut().unwrap().attempt = Id(2),
            7 => {
                bad.manifest.as_mut().unwrap().outputs[0].locator.0 =
                    bad.manifest.as_ref().unwrap().outputs[0]
                        .locator
                        .0
                        .replace("/entities/1", "/entities/2")
            }
            8 => bad.manifest.as_mut().unwrap().input_sha256 = Digest([0; 32]),
            9 => bad.manifest.as_mut().unwrap().committed_at.0 += 1,
            _ => {
                bad.child = Some(ChildScope {
                    scope: Id(0),
                    producer: Producer(0),
                })
            }
        }
        assert!(bad.validate_profiles(true).is_err(), "mutation {mutation}");
    }
    let mut no_results = work;
    no_results.manifest = None;
    no_results.output_until = None;
    no_results.validate_profiles(false).unwrap();
    assert!(no_results.validate_profiles(true).is_err());
}

#[test]
fn typed_receipts_reject_wrong_opcodes_attempts_and_fence_dispositions() {
    let Control::Work(Work::Retried { request, receipt }) =
        Control::decode(&fixture("retry-receipt"), MAX_CONTROL_LIMIT).unwrap()
    else {
        panic!("retry receipt")
    };
    assert!(
        Control::Work(Work::Cancelled {
            request,
            receipt: receipt.clone()
        })
        .encode(MAX_CONTROL_LIMIT)
        .is_err()
    );
    let mut bad = receipt;
    let Outcome::Retried {
        replacement_attempt,
        ..
    } = &mut bad.body
    else {
        panic!("retry outcome")
    };
    replacement_attempt.0 += 1;
    assert!(
        Control::Work(Work::Retried {
            request,
            receipt: bad
        })
        .encode(MAX_CONTROL_LIMIT)
        .is_err()
    );
    let Control::Work(Work::Cancelled { request, receipt }) =
        Control::decode(&fixture("cancel-receipt"), MAX_CONTROL_LIMIT).unwrap()
    else {
        panic!("cancel receipt")
    };
    for disposition in 0..=1 {
        for state in 0..=8 {
            let mut candidate = receipt.clone();
            let Outcome::Cancelled {
                disposition: d,
                state_at_commit: s,
                ..
            } = &mut candidate.body
            else {
                panic!("cancel outcome")
            };
            *d = Disposition(disposition);
            *s = State(state);
            let expected = if disposition == 0 {
                state == 4 || state == 7
            } else {
                state >= 5
            };
            assert_eq!(
                Control::Work(Work::Cancelled {
                    request,
                    receipt: candidate
                })
                .encode(MAX_CONTROL_LIMIT)
                .is_ok(),
                expected
            );
        }
    }
}

#[test]
fn every_refusal_code_maps_to_the_v2_quic_namespace_and_reserved_codes_fail() {
    for code in 0..=32 {
        let frame = [7, 0, 0, 0, 6, 0x83, 0x82, 0, 1, code, 0x60];
        // 24..32 require the two-octet integer representation, tested below.
        if code < 24 {
            let decoded = Control::decode(&frame, INITIAL_CONTROL_LIMIT);
            if (1..=18).contains(&code) {
                let Control::Refusal(r) = decoded.unwrap() else {
                    panic!("refusal")
                };
                assert_eq!(r.code.quic_error(), 0x200 + u64::from(code));
            } else {
                assert_eq!(decoded.unwrap_err().code, ErrorCode::FrameError);
            }
        } else {
            assert!(
                Control::decode(
                    &[7, 0, 0, 0, 7, 0x83, 0x82, 0, 1, 0x18, code, 0x60],
                    INITIAL_CONTROL_LIMIT
                )
                .is_err()
            );
        }
    }
}

#[test]
fn all_frozen_commitments_are_computed_from_typed_fields() {
    let session = SessionIdentity {
        authority: IdentityLabel("authority-1".into()),
        owner: IdentityLabel("owner-1".into()),
        generation: Id(1),
    };
    let input = InputHeader::decode_framed(&fixture("input-header")).unwrap();
    let work = input.parameters.work.clone();
    let manifest = Manifest::decode(&fixture("result-manifest")).unwrap();
    let leaf = StatusLeaf {
        work: work.clone(),
        state: State::SUCCEEDED,
        attempt: Number(1),
        manifest_digest: Some(manifest.digest().unwrap()),
        child_status_root: None,
    };
    let mutations = [
        ("admission-operation", Mutation::Admit(input.parameters), 1),
        (
            "declaration-operation",
            Mutation::Declare {
                scope: Number(0),
                entity_ids: vec![Id(1)],
                seal: true,
            },
            2,
        ),
        (
            "retry-operation",
            Mutation::Retry {
                work: work.clone(),
                expected_attempt: Id(1),
            },
            3,
        ),
        (
            "cancel-operation",
            Mutation::Cancel { work: work.clone() },
            4,
        ),
        ("skip-operation", Mutation::Skip { work }, 5),
        (
            "scope-cancel-operation",
            Mutation::ScopeCancel { scope: Number(0) },
            6,
        ),
    ];
    let mut count = 0;
    for row in include_str!("../../../../test-vectors/v2/commitments.tsv")
        .lines()
        .skip(1)
    {
        let fields: Vec<_> = row.split('\t').collect();
        let actual = if let Some((_, mutation, id)) = mutations.iter().find(|m| m.0 == fields[0]) {
            let mut operation = [0; 16];
            operation[15] = *id;
            assert_eq!(
                mutation
                    .commitment_bytes(&session, Producer(0), OperationId(operation))
                    .unwrap(),
                unhex(fields[2]),
                "{} preimage",
                fields[0]
            );
            mutation
                .digest(&session, Producer(0), OperationId(operation))
                .unwrap()
        } else {
            match fields[0] {
                "one-member-seal" => {
                    scope_seal(&session, Number(0), Producer(0), None, Number(1), [Id(1)]).unwrap()
                }
                "empty-scope-seal" => {
                    scope_seal(&session, Number(0), Producer(0), None, Number(0), []).unwrap()
                }
                "result-manifest" => {
                    assert_eq!(manifest.encode().unwrap(), unhex(fields[2]));
                    manifest.digest().unwrap()
                }
                "success-status-leaf" => {
                    assert_eq!(leaf.commitment_bytes().unwrap(), unhex(fields[2]));
                    leaf.digest().unwrap()
                }
                "empty-status-root" => empty_status_root(),
                "two-status-nodes" => status_node(leaf.digest().unwrap(), leaf.digest().unwrap()),
                _ => panic!("unknown commitment"),
            }
        };
        assert_eq!(
            actual.0.as_slice(),
            unhex(fields[3]),
            "{} digest",
            fields[0]
        );
        count += 1;
    }
    assert_eq!(count, 12);
}

#[test]
fn incremental_commitments_cover_batch_boundaries_odd_merkle_trees_and_namespace_separation() {
    let session = SessionIdentity {
        authority: IdentityLabel("a".into()),
        owner: IdentityLabel("o".into()),
        generation: Id(1),
    };
    let many = scope_seal(
        &session,
        Number(0),
        Producer(0),
        None,
        Number(1000),
        (1..=1000).map(Id),
    )
    .unwrap();
    assert_ne!(
        many,
        scope_seal(&session, Number(0), Producer(0), None, Number(0), []).unwrap()
    );
    for ids in [
        vec![Id(1)],
        vec![Id(1), Id(1)],
        vec![Id(2), Id(1)],
        vec![Id(1), Id(2), Id(3)],
    ] {
        assert!(scope_seal(&session, Number(0), Producer(0), None, Number(2), ids).is_err());
    }
    let mutation = Mutation::Cancel {
        work: WorkKey {
            scope: Number(3),
            producer: Producer(1),
            entity: Id(1),
        },
    };
    assert_ne!(
        mutation
            .digest(&session, Producer(0), OperationId([1; 16]))
            .unwrap(),
        mutation
            .digest(&session, Producer(1), OperationId([1; 16]))
            .unwrap()
    );
    for size in 0..=1000usize {
        let mut incremental = StatusRoot::default();
        let mut naive = Vec::new();
        for n in 0..size {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&(n as u64).to_be_bytes());
            let digest = Digest(bytes);
            incremental.push(digest).unwrap();
            naive.push(digest);
        }
        while naive.len() > 1 {
            naive = naive
                .chunks(2)
                .map(|p| status_node(p[0], *p.get(1).unwrap_or(&p[0])))
                .collect();
        }
        assert_eq!(
            incremental.finish(),
            naive.first().copied().unwrap_or_else(empty_status_root),
            "size {size}"
        );
    }
}

#[test]
fn payload_verification_requires_exact_bytes_fin_and_bounded_monotonic_progress() {
    use std::time::{Duration as Elapsed, Instant};
    let header = ResultHeader::decode_framed(&fixture("result-header")).unwrap();
    let mut limits = caps("capabilities-response");
    limits.stream_idle_ms = IdleMs(1000);
    limits.stream_lifetime_ms = LifetimeMs(2000);
    let start = Instant::now();
    let new_receiver =
        || PayloadReceiver::new(header.length, header.sha256, &limits, start).unwrap();
    let mut receiver = new_receiver();
    receiver
        .receive(b"A", start + Elapsed::from_millis(500))
        .unwrap();
    receiver
        .receive(b"BC", start + Elapsed::from_millis(1000))
        .unwrap();
    let proof = receiver.finish(start + Elapsed::from_millis(1500)).unwrap();
    assert_eq!(proof.length(), Number(3));
    assert_eq!(proof.sha256(), header.sha256);
    assert!(receiver.receive(b"", start).is_err());
    assert!(receiver.finish(start).is_err());
    for bytes in [b"AB".as_slice(), b"abc", b"ABCD"] {
        let mut receiver = new_receiver();
        let result = receiver
            .receive(bytes, start)
            .and_then(|_| receiver.finish(start));
        assert_eq!(result.unwrap_err().code, ErrorCode::IntegrityError);
        assert!(receiver.receive(b"ABC", start).is_err());
    }
    let mut idle = new_receiver();
    idle.receive(b"", start + Elapsed::from_millis(999))
        .unwrap();
    assert_eq!(
        idle.check_deadline(start + Elapsed::from_millis(1000))
            .unwrap_err()
            .code,
        ErrorCode::LimitExceeded
    );
    let mut lifetime = new_receiver();
    lifetime
        .receive(b"A", start + Elapsed::from_millis(800))
        .unwrap();
    lifetime
        .receive(b"B", start + Elapsed::from_millis(1600))
        .unwrap();
    assert_eq!(
        lifetime
            .receive(b"C", start + Elapsed::from_millis(2000))
            .unwrap_err()
            .code,
        ErrorCode::LimitExceeded
    );
    let mut regressed = new_receiver();
    regressed
        .receive(b"A", start + Elapsed::from_millis(1))
        .unwrap();
    assert_eq!(
        regressed.receive(b"B", start).unwrap_err().code,
        ErrorCode::ClockUnsafe
    );
    let empty = InputHeader::decode_framed(&fixture("empty-input-header")).unwrap();
    let mut receiver =
        PayloadReceiver::new(Number(0), empty.parameters.input.sha256, &limits, start).unwrap();
    assert_eq!(receiver.finish(start).unwrap().length(), Number(0));
    limits.object_limit = Number(2);
    assert_eq!(
        PayloadReceiver::new(header.length, header.sha256, &limits, start)
            .err()
            .unwrap()
            .code,
        ErrorCode::LimitExceeded
    );
}

#[test]
fn correlation_is_bounded_out_of_order_and_rejects_reuse_wrong_kind_and_duplicate() {
    let mut selected = caps("capabilities-response");
    selected.pending_limit = ConcurrencyLimit(2);
    let mut book = Correlation::new(selected).unwrap();
    let request = |id| Control::Session(Session::NextSequence { request: Id(id) });
    let reply = |id| {
        Control::Session(Session::Sequence {
            request: Id(id),
            next_creation_sequence: Id(1),
        })
    };
    assert!(book.register(&request(2), None).is_err());
    book.register(&request(1), None).unwrap();
    assert!(book.register(&request(1), None).is_err());
    book.register(&request(3), None).unwrap(); // increasing does not mean contiguous
    assert_eq!(
        book.register(&request(4), None).unwrap_err().code,
        ErrorCode::LimitExceeded
    );
    assert!(
        book.accept(&Control::Drain(Drain::Detached { request: Id(1) }))
            .is_err()
    );
    assert_eq!(book.pending(), 2);
    book.accept(&reply(3)).unwrap();
    book.register(&request(4), None).unwrap();
    assert!(book.accept(&reply(3)).is_err());
    book.accept(&reply(1)).unwrap();
    assert!(book.register(&request(2), None).is_err());
    book.abort(&RequestTag::Control { request: Id(4) }).unwrap();
    assert_eq!(book.pending(), 0);
    assert!(book.accept(&reply(4)).is_err());
    book.register(&request(MAX_NUMBER), None).unwrap();
    book.accept(&reply(MAX_NUMBER)).unwrap();
    assert!(book.register(&request(MAX_NUMBER), None).is_err());
    assert!(book.register(&request(MAX_NUMBER + 1), None).is_err());
}

fn result_request_and_header(id: u64) -> (Control, ResultHeader, Manifest) {
    let mut header = ResultHeader::decode_framed(&fixture("result-header")).unwrap();
    header.request = Id(id);
    let manifest = Manifest::decode(&fixture("result-manifest")).unwrap();
    let request = Control::Result(ResultMessage::Read {
        request: Id(id),
        work: header.work.clone(),
        attempt: header.attempt,
        index: header.index,
        expected_sha256: header.sha256,
    });
    (request, header, manifest)
}

#[test]
fn result_correlation_stays_pending_until_verified_fin_and_rejects_second_response() {
    let now = std::time::Instant::now();
    let (request, header, manifest) = result_request_and_header(1);
    let mut book = Correlation::new(caps("capabilities-response")).unwrap();
    assert!(book.register(&request, None).is_err());
    book.register(&request, Some(&manifest)).unwrap();
    let mut changed = header.clone();
    changed.length.0 += 1;
    assert!(book.start_result(&changed, now).is_err());
    let mut stream = book.start_result(&header, now).unwrap();
    assert!(book.start_result(&header, now).is_err());
    stream.receive(b"ABC", now).unwrap();
    assert_eq!(book.pending(), 1);
    let refusal = Control::Refusal(Refusal {
        request: RequestTag::Control { request: Id(1) },
        code: ErrorCode::OutputUnavailable,
        detail: Detail("missing".into()),
    });
    assert!(book.accept(&refusal).is_err());
    let verified = stream.finish(now).unwrap();
    book.finish_result(verified).unwrap();
    assert_eq!(book.pending(), 0);
    assert!(book.start_result(&header, now).is_err());
}

#[test]
fn result_proof_cannot_cross_connections_or_outlive_aborted_correlation() {
    let now = std::time::Instant::now();
    let (request, header, manifest) = result_request_and_header(1);
    let mut a = Correlation::new(caps("capabilities-response")).unwrap();
    let mut b = Correlation::new(caps("capabilities-response")).unwrap();
    a.register(&request, Some(&manifest)).unwrap();
    b.register(&request, Some(&manifest)).unwrap();
    let mut from_a = a.start_result(&header, now).unwrap();
    let mut from_b = b.start_result(&header, now).unwrap();
    from_a.receive(b"ABC", now).unwrap();
    from_b.receive(b"ABC", now).unwrap();
    assert!(b.finish_result(from_a.finish(now).unwrap()).is_err());
    assert_eq!(b.pending(), 1);
    b.abort(&RequestTag::Control { request: Id(1) }).unwrap();
    assert!(b.finish_result(from_b.finish(now).unwrap()).is_err());
    let (request, header, _) = result_request_and_header(2);
    b.register(&request, Some(&manifest)).unwrap();
    let mut fresh = b.start_result(&header, now).unwrap();
    fresh.receive(b"ABC", now).unwrap();
    b.finish_result(fresh.finish(now).unwrap()).unwrap();
}

#[test]
fn slow_result_stream_does_not_block_control_progress_and_stream_limit_is_enforced() {
    let now = std::time::Instant::now();
    let mut selected = caps("capabilities-response");
    selected.stream_limit = ConcurrencyLimit(1);
    let mut book = Correlation::new(selected).unwrap();
    let (one, first_header, manifest) = result_request_and_header(1);
    let (two, second_header, _) = result_request_and_header(2);
    book.register(&one, Some(&manifest)).unwrap();
    book.register(&two, Some(&manifest)).unwrap();
    let mut slow = book.start_result(&first_header, now).unwrap();
    assert_eq!(
        book.start_result(&second_header, now).err().unwrap().code,
        ErrorCode::LimitExceeded
    );
    assert_eq!(
        book.start_result(&first_header, now).err().unwrap().code,
        ErrorCode::FrameError
    );
    book.register(
        &Control::Session(Session::NextSequence { request: Id(3) }),
        None,
    )
    .unwrap();
    book.accept(&Control::Session(Session::Sequence {
        request: Id(3),
        next_creation_sequence: Id(1),
    }))
    .unwrap();
    assert_eq!(book.pending(), 2);
    slow.receive(b"ABC", now).unwrap();
    book.finish_result(slow.finish(now).unwrap()).unwrap();
    let mut next = book.start_result(&second_header, now).unwrap();
    next.receive(b"ABC", now).unwrap();
    book.finish_result(next.finish(now).unwrap()).unwrap();
    assert_eq!(book.pending(), 0);
}

#[test]
fn admission_correlation_uses_actual_input_streams_and_checks_immutable_parameters() {
    let header = InputHeader::decode_framed(&fixture("input-header")).unwrap();
    let response = Control::decode(&fixture("admission-receipt"), MAX_CONTROL_LIMIT).unwrap();
    let Control::Work(Work::Admitted {
        request: RequestTag::Input { stream },
        ..
    }) = &response
    else {
        panic!("admission fixture")
    };
    let mut book = Correlation::new(caps("capabilities-response")).unwrap();
    for wrong in [0, 1, 3, (1u64 << 62) + 2] {
        assert!(book.register_input(StreamId(wrong), &header).is_err());
    }
    book.register_input(*stream, &header).unwrap();
    assert!(book.register_input(*stream, &header).is_err());
    let mut wrong = response.clone();
    let Control::Work(Work::Admitted { receipt, .. }) = &mut wrong else {
        panic!("admission")
    };
    let Outcome::Admitted { work, .. } = &mut receipt.body else {
        panic!("admission outcome")
    };
    work.entity.0 += 1;
    assert!(book.accept(&wrong).is_err());
    assert_eq!(book.pending(), 1);
    book.accept(&response).unwrap();
    assert!(book.accept(&response).is_err());
    assert!(book.register_input(*stream, &header).is_err());
    book.register_input(StreamId(stream.0 + 4), &header)
        .unwrap();
}
