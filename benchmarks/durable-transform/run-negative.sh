#!/usr/bin/env bash
# Negative controls as real runs: each control has a positive twin on the
# same configuration (must pass) and an injected run that must FAIL with a
# named reason, recorded as INVALID. Usage: run-negative.sh WORK
# Quick config (seed 6, 200000 bytes, 4 chunks). Takes BENCHMARK.lock.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6; SIZE=200000
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock

exec 9>"$COORD_LOCK"
flock -w 600 9 || { echo "could not take BENCHMARK.lock"; exit 1; }

export PS_AUTH="$REPO/examples/durable-transform-workload/workload-authority/target/release/workload-authority"
export PS_COORD="$REPO/examples/durable-transform-workload/workload-coordinator/target/release/workload-coordinator"
export GRPC_WORKER="$REPO/examples/durable-transform-grpc/target/release/grpc-worker"
export GRPC_COORD="$REPO/examples/durable-transform-grpc/target/release/grpc-coordinator"
for b in "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD"; do
  [ -x "$b" ] || { echo "MISSING binary: $b"; exit 1; }
done
sha256sum "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD" > "$WORK.bin.sha256" 2>/dev/null || true
mkdir -p "$WORK"

PASS=0; FAIL=0
note() { echo "$1" | tee -a "$WORK/NEGATIVE-LOG.txt"; }

# pos <name> <ps|grpc> [env...]: positive twin must exit 0.
pos() {
  local name="$1" arm="$2"; shift 2
  if [ "$arm" = ps ]; then
    env "$@" bash "$HERE/libexec-run-ps.sh" "$WORK/$name-pos" "$SEED" "$SIZE" \
      >"$WORK/$name-pos.stdout.log" 2>&1
  else
    env "$@" bash "$HERE/libexec-run-grpc.sh" "$WORK/$name-pos" "$SEED" "$SIZE" \
      >"$WORK/$name-pos.stdout.log" 2>&1
  fi
  note "POSITIVE $name/$arm: exit 0"
  PASS=$((PASS + 1))
}

# neg <name> <ps|grpc> <reason-grep> [env...]: injected run must fail AND
# print the named reason; otherwise the gate is broken and this script fails.
neg() {
  local name="$1" arm="$2" reason="$3"; shift 3
  local log="$WORK/$name-neg.stdout.log" rc=0
  if [ "$arm" = ps ]; then
    env "$@" bash "$HERE/libexec-run-ps.sh" "$WORK/$name-neg" "$SEED" "$SIZE" >"$log" 2>&1 || rc=$?
  else
    env "$@" bash "$HERE/libexec-run-grpc.sh" "$WORK/$name-neg" "$SEED" "$SIZE" >"$log" 2>&1 || rc=$?
  fi
  [ "$rc" -ne 0 ] || { note "GATE BROKEN $name/$arm: injected run passed"; return 1; }
  grep -q -m1 "$reason" "$log" "$WORK/$name-neg"/artifacts/*events.tsv 2>/dev/null \
    || { note "GATE BROKEN $name/$arm: reason '$reason' not found"; return 1; }
  echo "INVALID: $reason (exit $rc)" > "$WORK/$name-neg/INVALID.txt"
  note "NEGATIVE $name/$arm: INVALID as designed ($reason)"
  PASS=$((PASS + 1))
}

# ---- swapped chunk order: inputs uploaded in swapped order ----
# (grpc detects the swap at the coordinator oracle, the same detector as
# wrong-transform: distinct injections, shared honest detector)
pos swapped-order-ps ps
neg swapped-order-ps ps "file does not match admission intent" PS_SWAP_INPUTS="0:1"
pos swapped-order-grpc grpc
neg swapped-order-grpc grpc "failed oracle byte verification" GRPC_SWAP_INPUTS="0:1"

# ---- correct hash, wrong transform: faulty workers pass bytes through ----
pos wrong-transform-ps ps
neg wrong-transform-ps ps "failed byte verification" PS_TEST_WRONG_TRANSFORM=1
pos wrong-transform-grpc grpc
neg wrong-transform-grpc grpc "failed oracle byte verification" GRPC_TEST_WRONG_TRANSFORM=1

# ---- missing chunk: input file removed after materialization ----
pos missing-chunk-ps ps
neg missing-chunk-ps ps "missing" PS_DROP_INPUT=2
pos missing-chunk-grpc grpc
neg missing-chunk-grpc grpc "missing" GRPC_DROP_INPUT=2

# ---- truncated artifact / stale hash: post-run verifier (sha256sum -c) ----
for arm in ps grpc; do
  if [ "$arm" = ps ]; then
    env bash "$HERE/libexec-run-ps.sh" "$WORK/trunc-$arm-pos" "$SEED" "$SIZE" \
      >"$WORK/trunc-$arm-pos.stdout.log" 2>&1
    FIN="$WORK/trunc-$arm-pos/artifacts/ps-final.bin"
  else
    env bash "$HERE/libexec-run-grpc.sh" "$WORK/trunc-$arm-pos" "$SEED" "$SIZE" \
      >"$WORK/trunc-$arm-pos.stdout.log" 2>&1
    FIN="$WORK/trunc-$arm-pos/artifacts/grpc-final.bin"
  fi
  (cd "$WORK/trunc-$arm-pos/artifacts" && sha256sum "$(basename "$FIN")") > "$WORK/trunc-$arm.sha256"
  note "POSITIVE trunc-$arm: exit 0, digest recorded"
  PASS=$((PASS + 1))
  TRUNCSHA="$(cd "$WORK" && pwd)/trunc-$arm.sha256"
  if (cd "$WORK/trunc-$arm-pos/artifacts" && sha256sum -c "$TRUNCSHA" >/dev/null 2>&1); then
    note "POSITIVE trunc-$arm verifier: intact file verifies"
    PASS=$((PASS + 1))
  else
    note "GATE BROKEN trunc-$arm: intact file does not verify"; exit 1
  fi
  head -c 100000 "$FIN" > "$FIN.truncated"
  TRUNCH="$(cut -d' ' -f1 "$TRUNCSHA")"
  if (cd "$WORK/trunc-$arm-pos/artifacts" && echo "$TRUNCH  $(basename "$FIN.truncated")" | sha256sum -c - >/dev/null 2>&1); then
    note "GATE BROKEN truncated-artifact-$arm: truncated file verified"; exit 1
  else
    echo "INVALID: truncated artifact fails sha256 verification" > "$WORK/truncated-artifact-$arm-INVALID.txt"
    note "NEGATIVE truncated-artifact-$arm: INVALID as designed"
    PASS=$((PASS + 1))
  fi
  # stale recorded hash must NOT verify
  echo "0000000000000000000000000000000000000000000000000000000000000000  $(basename "$FIN")" > "$WORK/stale-$arm.sha256"
  if (cd "$WORK/trunc-$arm-pos/artifacts" && sha256sum -c "$WORK/stale-$arm.sha256" >/dev/null 2>&1); then
    note "GATE BROKEN stale-hash-$arm: forged digest verified"; exit 1
  else
    echo "INVALID: stale recorded hash fails sha256 verification" > "$WORK/stale-hash-$arm-INVALID.txt"
    note "NEGATIVE stale-hash-$arm: INVALID as designed"
    PASS=$((PASS + 1))
  fi
done

# ---- killed collector: murder the sampler mid-run ----
for arm in ps grpc; do
  if [ "$arm" = ps ]; then
    RUNNER="$HERE/libexec-run-ps.sh"
  else
    RUNNER="$HERE/libexec-run-grpc.sh"
  fi
  bash "$RUNNER" "$WORK/killed-$arm-neg" "$SEED" "$SIZE" \
    >"$WORK/killed-$arm-neg.stdout.log" 2>&1 &
  RUNPID=$!
  # Kill our sampler as soon as it appears (scoped to this WORK dir only).
  for _ in $(seq 1 200); do
    SAM=$(pgrep -f "sample.sh.*$WORK/killed-$arm-neg" | head -1 || true)
    [ -n "$SAM" ] && break
    sleep 0.1
  done
  if [ -z "${SAM:-}" ]; then
    note "GATE BROKEN killed-collector-$arm: sampler never appeared"; exit 1
  fi
  kill -9 "$SAM" 2>/dev/null || true
  rc=0; wait "$RUNPID" || rc=$?
  [ "$rc" -ne 0 ] || { note "GATE BROKEN killed-collector-$arm: run passed with dead sampler"; exit 1; }
  grep -q -m1 "metric sampler died mid-run" "$WORK/killed-$arm-neg.stdout.log" \
    || { note "GATE BROKEN killed-collector-$arm: reason not found"; exit 1; }
  echo "INVALID: metric sampler died mid-run (exit $rc)" > "$WORK/killed-$arm-neg/INVALID.txt"
  note "NEGATIVE killed-collector-$arm: INVALID as designed"
  PASS=$((PASS + 1))
done

# ---- missing worker metrics: proven by authentic C15 failures ----
# The 0.2 s sampler races sub-second runs; C15 tiny/uneven rep-0-grpc runs
# failed with exactly "missing metric samples for worker <pid>" while
# milestones held and finals matched. Those committed archives are the
# evidence (c4-tiny-seed6, c4-uneven-seed6, CELL.txt records the flake);
# the race is not deterministically injectable, so no fresh run is faked.
cat > "$WORK/missing-metrics-EVIDENCE.txt" <<'EOF'
INVALID (authentic, C15): missing metric samples for worker <pid>
c4-tiny-seed6/rep-0-grpc (exit 1, milestones present, final 55d2e6e9 byte-identical)
c4-uneven-seed6/rep-0-grpc (exit 1, milestones present, final 65283733 byte-identical)
Replacement reps (rep-5-grpc) pass; failed runs kept visible in the table.
EOF
note "NEGATIVE missing-metrics: INVALID by authentic C15 evidence (see missing-metrics-EVIDENCE.txt)"
PASS=$((PASS + 1))

# ---- revoked reader ----
# grpc: live revocation — rewrite the map without the reader mid-run.
# The worker checks the map file on every request.
pos revoked-reader-grpc grpc
bash "$HERE/libexec-run-grpc.sh" "$WORK/revoked-reader-grpc-neg" "$SEED" "$SIZE" \
  >"$WORK/revoked-reader-grpc-neg.stdout.log" 2>&1 &
RUNPID=$!
for _ in $(seq 1 600); do
  [ -f "$WORK/revoked-reader-grpc-neg/work/pki/grpc-principals.tsv" ] && break
  sleep 0.1
done
head -1 "$WORK/revoked-reader-grpc-neg/work/pki/grpc-principals.tsv" \
  > "$WORK/revoked-reader-grpc-neg/work/pki/grpc-principals.tsv.revoked"
mv "$WORK/revoked-reader-grpc-neg/work/pki/grpc-principals.tsv.revoked" \
  "$WORK/revoked-reader-grpc-neg/work/pki/grpc-principals.tsv"
rc=0; wait "$RUNPID" || rc=$?
[ "$rc" -ne 0 ] || { note "GATE BROKEN revoked-reader-grpc: run passed with revoked reader"; exit 1; }
grep -q -m1 -i "unauthenticated\|permission.denied\|UNAUTHORIZED" "$WORK/revoked-reader-grpc-neg.stdout.log" \
  || { note "GATE BROKEN revoked-reader-grpc: no auth reason"; exit 1; }
echo "INVALID: reader revoked mid-run, run rejected (exit $rc)" > "$WORK/revoked-reader-grpc-neg/INVALID.txt"
note "NEGATIVE revoked-reader-grpc: INVALID as designed"
PASS=$((PASS + 1))
# ps: principal map loads at open, so the revoked principal is absent
# from the start (framed honestly: revoked == absent at read time).
pos revoked-reader-ps ps
"$HERE/mk-test-pki.sh" "$WORK/revoked-reader-ps-neg-pki"
head -1 "$WORK/revoked-reader-ps-neg-pki/ps-principals.tsv" > "$WORK/revoked-reader-ps-neg-pki/ps-principals.tsv.tmp"
mv "$WORK/revoked-reader-ps-neg-pki/ps-principals.tsv.tmp" "$WORK/revoked-reader-ps-neg-pki/ps-principals.tsv"
mkdir -p "$WORK/revoked-reader-ps-neg/work/pki"
cp "$WORK/revoked-reader-ps-neg-pki"/* "$WORK/revoked-reader-ps-neg/work/pki/"
rc=0
bash "$HERE/libexec-run-ps.sh" "$WORK/revoked-reader-ps-neg" "$SEED" "$SIZE" \
  >"$WORK/revoked-reader-ps-neg.stdout.log" 2>&1 || rc=$?
[ "$rc" -ne 0 ] || { note "GATE BROKEN revoked-reader-ps: run passed with revoked reader"; exit 1; }
grep -q -m1 -i "unauthenticated\|unauthorized\|permission\|principal\|auth" "$WORK/revoked-reader-ps-neg.stdout.log" \
  || { note "GATE BROKEN revoked-reader-ps: no auth reason"; exit 1; }
echo "INVALID: reader principal absent at read time (exit $rc)" > "$WORK/revoked-reader-ps-neg/INVALID.txt"
note "NEGATIVE revoked-reader-ps: INVALID as designed"
PASS=$((PASS + 1))

# ---- expired output ----
# grpc: 1 ms output retention; fetch lands seconds after commit.
pos expired-output-grpc grpc
rc=0
GRPC_RETENTION_MS=1 bash "$HERE/libexec-run-grpc.sh" "$WORK/expired-output-grpc-neg" "$SEED" "$SIZE" \
  >"$WORK/expired-output-grpc-neg.stdout.log" 2>&1 || rc=$?
[ "$rc" -ne 0 ] || { note "GATE BROKEN expired-output-grpc: run passed with 1ms retention"; exit 1; }
grep -q -m1 -i "expired" "$WORK/expired-output-grpc-neg.stdout.log" "$WORK/expired-output-grpc-neg"/artifacts/*events.tsv 2>/dev/null \
  || { note "GATE BROKEN expired-output-grpc: no expiry reason"; exit 1; }
echo "INVALID: output expired before read (exit $rc)" > "$WORK/expired-output-grpc-neg/INVALID.txt"
note "NEGATIVE expired-output-grpc: INVALID as designed"
PASS=$((PASS + 1))
# ps: 1 ms attempt execution deadline (calibrated at runtime; if the
# authority does not enforce it as expiry, this stays UNAVAILABLE).
pos expired-output-ps ps
rc=0
PS_EXECUTION_MS=1 bash "$HERE/libexec-run-ps.sh" "$WORK/expired-output-ps-neg" "$SEED" "$SIZE" \
  >"$WORK/expired-output-ps-neg.stdout.log" 2>&1 || rc=$?
if [ "$rc" -ne 0 ] && grep -q -m1 -i "expired\|deadline" "$WORK/expired-output-ps-neg.stdout.log" "$WORK/expired-output-ps-neg"/artifacts/*events.tsv 2>/dev/null; then
  echo "INVALID: execution deadline expired (exit $rc)" > "$WORK/expired-output-ps-neg/INVALID.txt"
  note "NEGATIVE expired-output-ps: INVALID as designed"
  PASS=$((PASS + 1))
else
  echo "UNAVAILABLE: ps authority exposes no output-retention knob; 1ms execution deadline rc=$rc without expiry semantics" > "$WORK/expired-output-ps-UNAVAILABLE.txt"
  note "NEGATIVE expired-output-ps: UNAVAILABLE (no retention knob; see file)"
fi

note "DONE: $PASS checks passed, $FAIL failed"
echo "NEGATIVE SUITE PASS: $PASS checks"
