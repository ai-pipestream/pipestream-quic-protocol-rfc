#!/usr/bin/env bash
# Stopped-consumer arm, both arms, five repeats, same inputs/placement.
# The no-fetch gate demonstration refuses final assembly with a named
# reason (coordinator bails before touching absent staged outputs).
# Usage: run-stopped.sh WORK   (takes BENCHMARK.lock itself)
# Per arm per rep: a positive twin (must pass, pin holds) and a delay run
# (FLAG-armed 5000 ms consumer hold after full admission, then resume and
# complete byte-exact). Per arm: five stall-read probes (FLAG-armed 5000 ms
# hold of the first read on every chunk; the peer's behavior is recorded
# verbatim) and one no-fetch gate demonstration (arrival without completion
# must NOT pass). Quick config (seed 6, 200000, 4 chunks).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6; SIZE=200000; CHUNKS=4; REPS=5
HOLD_MS=5000
QUICK_PIN=3cde15aae8059eb0d8cfbb62fa897bbf4df1e1dfd5e3de30a4784faa314ad192
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
note() { echo "$1" | tee -a "$WORK/STOPPED-LOG.txt"; }
wall_of() { grep -o -m1 "done in [0-9]* ms" "$1" | grep -o "[0-9]*"; }

# run_arm <arm> <dir> <env...>: one libexec run, prints wall ms.
run_arm() {
  local arm="$1" dir="$2"; shift 2
  if [ "$arm" = ps ]; then
    env "$@" bash "$HERE/libexec-run-ps.sh" "$dir" "$SEED" "$SIZE" >"$dir.stdout.log" 2>&1
  else
    env "$@" bash "$HERE/libexec-run-grpc.sh" "$dir" "$SEED" "$SIZE" >"$dir.stdout.log" 2>&1
  fi
}

ev_of() { # ev_of <arm> <dir>: events path
  if [ "$1" = ps ]; then echo "$2/artifacts/ps-events.tsv"; else echo "$2/artifacts/grpc-events.tsv"; fi
}

fin_of() { # fin_of <arm> <dir>: final path
  if [ "$1" = ps ]; then echo "$2/artifacts/ps-final.bin"; else echo "$2/artifacts/grpc-final.bin"; fi
}

# exactly_one_verification_per_chunk <arm> <dir>: no lost or duplicated output.
check_no_loss_dup() {
  local arm="$1" ev; ev=$(ev_of "$arm" "$2")
  local o c
  for o in 0 1 2 3; do
    c=$(awk -F'\t' -v o="$o" '$4==o && $5=="chunk-verified"' "$ev" | wc -l)
    [ "$c" = 1 ] || { note "STOPPED $arm $2: ordinal $o verified $c times"; return 1; }
  done
}

check_pin() {
  local d; d=$(sha256sum "$(fin_of "$1" "$2")" | cut -d' ' -f1)
  [ "$d" = "$QUICK_PIN" ] || { note "STOPPED $1 $2: digest $d != pin"; return 1; }
}

check_no_refusal() { # capacity reconciled: no backpressure/refusal rows.
  grep -q -i -m1 "refus\|LIMIT_EXCEEDED\|admit-refused" "$1" \
    && { note "STOPPED $2 $3: unexpected refusal row"; return 1; } || true
}

for arm in ps grpc; do
  if [ "$arm" = ps ]; then DENV=PS_FETCH_DELAY_MS; SENV=PS_STALL_READ_MS; NENV=PS_NO_FETCH
  else DENV=GRPC_FETCH_DELAY_MS; SENV=GRPC_STALL_READ_MS; NENV=GRPC_NO_FETCH; fi
  : > "$WORK/$arm-delay-walls.txt"; : > "$WORK/$arm-pos-walls.txt"
  for i in $(seq 1 $REPS); do
    # Positive twin.
    run_arm "$arm" "$WORK/$arm-pos-rep$i"
    check_pin "$arm" "$WORK/$arm-pos-rep$i" || exit 1
    wall_of "$WORK/$arm-pos-rep$i.stdout.log" >> "$WORK/$arm-pos-walls.txt"
    # Delay run: hold then resume, must complete byte-exact.
    D="$WORK/$arm-delay-rep$i"
    run_arm "$arm" "$D" "$DENV=$HOLD_MS"
    EV=$(ev_of "$arm" "$D")
    grep -q "consumer-stopped" "$EV" || { note "STOPPED $arm $D: no stop row"; exit 1; }
    grep -q "consumer-resumed" "$EV" || { note "STOPPED $arm $D: no resume row"; exit 1; }
    grep -q "first-usable-output" "$EV" || { note "STOPPED $arm $D: no first-usable"; exit 1; }
    grep -q "final-verified" "$EV" || { note "STOPPED $arm $D: no final-verified"; exit 1; }
    [ "$(awk -F'\t' '$5=="admitted"' "$EV" | wc -l)" = "$CHUNKS" ] \
      || { note "STOPPED $arm $D: admitted != $CHUNKS"; exit 1; }
    check_no_loss_dup "$arm" "$D" || exit 1
    check_pin "$arm" "$D" || exit 1
    check_no_refusal "$EV" "$arm" "$D" || exit 1
    wall_of "$D.stdout.log" >> "$WORK/$arm-delay-walls.txt"
    note "STOPPED $arm delay rep$i: held ${HOLD_MS}ms, resumed, byte-exact, no loss/dup/refusal"
  done
  note "STOPPED $arm positive: 5/5 PASS, digest pin holds"
  # Stall-read probes: peer behavior verbatim.
  : > "$WORK/$arm-stall-peer.txt"
  for i in $(seq 1 $REPS); do
    D="$WORK/$arm-stall-rep$i"
    run_arm "$arm" "$D" "$SENV=$HOLD_MS"
    EV=$(ev_of "$arm" "$D")
    N=$(grep -c "stalled-read" "$EV" || true)
    [ "$N" = "$CHUNKS" ] || { note "STOPPED $arm $D: $N stalled-read rows, want $CHUNKS"; exit 1; }
    check_pin "$arm" "$D" || exit 1
    check_no_loss_dup "$arm" "$D" || exit 1
    echo "rep$i: $N stalled reads, exit 0, final pin holds" >> "$WORK/$arm-stall-peer.txt"
  done
  if grep -q -i -m1 "refus\|LIMIT_EXCEEDED\|denied\|unauthenticated\|expired\|timeout\|cancelled" "$WORK/$arm-stall-rep1.stdout.log" "$WORK/$arm-stall-rep1"/artifacts/*events.tsv 2>/dev/null; then
    grep -i -h -m4 "refus\|LIMIT_EXCEEDED\|denied\|unauthenticated\|expired\|timeout\|cancelled" "$WORK/$arm-stall-rep1.stdout.log" "$WORK/$arm-stall-rep1"/artifacts/*events.tsv >> "$WORK/$arm-stall-peer.txt"
    note "STOPPED $arm stall: peer refused (see $arm-stall-peer.txt)"
  else
    echo "peer: quiet patience at ${HOLD_MS}ms -- no refusal, no error; connection survived, all finals pin-exact" >> "$WORK/$arm-stall-peer.txt"
    note "STOPPED $arm stall: peer quiet at ${HOLD_MS}ms, connection survived"
  fi
  # No-fetch gate: arrival without completion must NOT pass.
  D="$WORK/$arm-stopped"
  rc=0
  run_arm "$arm" "$D" "$NENV=1" || rc=$?
  EV=$(ev_of "$arm" "$D")
  [ "$rc" -ne 0 ] || { note "STOPPED $arm: gate passed a completion-less run"; exit 1; }
  [ "$(awk -F'\t' '$5=="admitted"' "$EV" | wc -l)" = "$CHUNKS" ] \
    || { note "STOPPED $arm: admitted != $CHUNKS"; exit 1; }
  grep -q "consumer-stopped" "$EV" || { note "STOPPED $arm: no consumer-stopped row"; exit 1; }
  grep -q "final-verified" "$EV" && { note "STOPPED $arm: unexpected final-verified"; exit 1; } || true
  grep -q "refusing final assembly" "$D.stdout.log" || { note "STOPPED $arm: gate reason not named"; exit 1; }
  echo "STOPPED (exit $rc): $CHUNKS admitted, consumer-stopped, no completion" > "$D/STOPPED.txt"
  note "STOPPED $arm no-fetch: gate holds (arrival without completion)"
done
note "STOPPED SUITE PASS"
