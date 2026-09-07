use super::*;
use crate::v2::authority::payload::{PayloadPolicy, PayloadStore};

struct Time;
impl Clock for Time {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(31000),
            trusted: true,
        }
    }
}

#[test]
fn corrupted_retirement_proof_never_authorizes_deletion_or_successful_reopen() {
    for case in [
        "owner",
        "authority",
        "generation",
        "creation",
        "root",
        "cutoff",
        "future",
        "revision",
        "credit",
        "owner-history",
        "generation-history",
        "missing-proof",
        "missing-flag",
        "format",
    ] {
        let fixture = super::super::tests::Fixture::new();
        let binding = fixture.create();
        fixture
            .store
            .declare(
                &binding.identity,
                OperationId([1; 16]),
                Number(0),
                &[],
                true,
            )
            .unwrap();
        let mut store = fixture.store.clone();
        store.clock = Arc::new(Time);
        let payloads = PayloadStore::initialize(
            &fixture.directory.path().join("objects"),
            store.payload_identity().unwrap(),
            PayloadPolicy {
                objects: Id(3),
                bytes: Number(4096),
                owner_objects: Id(3),
                owner_bytes: Number(4096),
                chunk_bytes: Id(1024),
                handles: Id(2),
                owner_handles: Id(2),
            },
        )
        .unwrap();
        store.bind_payloads(&payloads).unwrap();
        assert!(
            store
                .retire(&payloads, &mut RetirementCursor::default(), 1)
                .unwrap()
                .started
        );
        let mut connection = store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let mut proof = load(&tx, binding.identity.generation).unwrap().unwrap();
        let target = records::Target {
            table: records::Table::Retirement,
            row: 1,
        };
        match case {
            "missing-proof" => {
                tx.execute("DELETE FROM retirements", []).unwrap();
            }
            "missing-flag" => {
                tx.execute("UPDATE sessions SET retiring=0", []).unwrap();
            }
            "owner-history" => {
                tx.execute("UPDATE owners SET last_creation=0", []).unwrap();
            }
            "generation-history" => {
                tx.execute("UPDATE authority SET last_generation=0", [])
                    .unwrap();
            }
            "revision" => {
                assert!(records::replace(&tx, target, Id(1), &proof, false).is_err());
                tx.rollback().unwrap();
                store.integrity_check().unwrap();
                continue;
            }
            "credit" => {
                records::initialize(&tx, target, &proof, CAPACITY, 1).unwrap();
            }
            "format" => {
                tx.pragma_update(None, "user_version", 9).unwrap();
            }
            _ => {
                match case {
                    "owner" => proof.identity.owner = IdentityLabel("bob".into()),
                    "authority" => proof.identity.authority = IdentityLabel("other".into()),
                    "generation" => proof.identity.generation = Id(2),
                    "creation" => proof.creation = Id(2),
                    "root" => proof.root.status_root = Digest([9; 32]),
                    "cutoff" => proof.cutoff = Number(30999),
                    "future" => proof.at = Number(31001),
                    _ => unreachable!(),
                }
                // Preserve the genuine fixed-record checksum and initial
                // revision so semantic binding checks must find the corruption.
                records::initialize(&tx, target, &proof, CAPACITY, 0).unwrap();
            }
        }
        tx.commit().unwrap();
        if case != "format" {
            assert!(
                matches!(
                    store.retire(&payloads, &mut RetirementCursor::default(), 1),
                    Err(StoreError::Corrupt(_))
                ),
                "{case}"
            );
        }
        let remaining: u64 = connection
            .query_row("SELECT count(*) FROM sessions", [], |r| number(r, 0))
            .unwrap();
        assert_eq!(remaining, 1);
        assert!(
            matches!(
                AuthorityStore::open(
                    &fixture.directory.path().join("authority.sqlite"),
                    store.authority.clone(),
                    store.policy.clone(),
                    store.physical.limits,
                    store.clock.clone(),
                    store.authorization.clone()
                ),
                Err(StoreError::Corrupt(_))
            ),
            "{case}"
        );
    }
}

#[test]
fn maximum_retirement_identity_and_summary_fit_the_immutable_slot() {
    let proof = Proof {
        identity: SessionIdentity {
            authority: IdentityLabel("a".repeat(128)),
            owner: IdentityLabel("b".repeat(128)),
            generation: Id(MAX_NUMBER),
        },
        creation: Id(MAX_NUMBER),
        root: ScopeSummary {
            scope: Number(0),
            producer: Producer(0),
            parent: None,
            seal: Digest([1; 32]),
            declared: Number(MAX_NUMBER),
            counts: Counts {
                success: Number(MAX_NUMBER),
                failure: Number(0),
                cancelled: Number(0),
                skipped: Number(0),
            },
            status_root: Digest([2; 32]),
            closed_at: Number(MAX_NUMBER),
        },
        cutoff: Number(MAX_NUMBER),
        at: Number(MAX_NUMBER),
    };
    let bytes = pack(&proof).unwrap();
    assert!(bytes.len() <= CAPACITY);
    assert_eq!(unpack::<Proof>(&bytes).unwrap(), proof);
}
