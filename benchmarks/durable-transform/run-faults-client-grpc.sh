#!/usr/bin/env bash
# A1-grpc: coordinator-boundary kill suite (quick config).
# Usage: run-faults-client-grpc.sh WORK   (takes BENCHMARK.lock per run)
#
# Runnable boundaries on this arm: INTENT_JOURNALED (submit intent row
# committed), RECEIPT_JOURNALED (commit response received and journaled),
# RESULT_VERIFIED, RESULT_INSTALLED. Chunk ordinals 0 and 2 of the 4-chunk
# quick corpus (seed 6, 200000 bytes), 5 reps each: arm --test-kill-at,
# assert the run dies at the boundary (boundary row present with the right
# ordinal, no lifecycle row for the killed ordinal after it; rare trailing
# appends from other worker tasks racing the abort are recorded), restart
# workers on the same state, --resume without flags on the same db, assert
# byte-exact final against the quick pin on the same db file. Recovery =
# final-verified epoch - kill epoch. Frozen identities: same db file (PK
# prevents duplicate commits), deterministic frozen submit ids by
# construction, post-kill resubmits legal (same-identity resend, as on ps).
# REQUEST_SENT, RECEIPT_VALIDATED, OBSERVATION_JOURNALED, REFUSAL_RECEIVED
# are UNAVAILABLE (exact reasons in the UNAVAILABLE file, enforced at
# startup by the binary too).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6; SIZE=200000; REPS=5
QUICK_PIN=3cde15aae8059eb0d8cfbb62fa897bbf4df1e1dfd5e3de30a4784faa314ad192
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock

export GRPC_WORKER="$REPO/examples/durable-transform-grpc/target/release/grpc-worker"
export GRPC_COORD="$REPO/examples/durable-transform-grpc/target/release/grpc-coordinator"
for b in "$GRPC_WORKER" "$GRPC_COORD"; do [ -x "$b" ] || { echo "MISSING binary: $b" >&2; exit 1; }; done
mkdir -p "$WORK"
sha256sum "$GRPC_WORKER" "$GRPC_COORD" > "$WORK.bin.sha256" 2>/dev/null || true

note() { echo "$1" | tee -a "$WORK/CLIENT-GRPC-LOG.txt"; }
[ -f "$WORK/CLIENT-GRPC-LOG.txt" ] || : > "$WORK/CLIENT-GRPC-LOG.txt"
if [ ! -f "$WORK/CELL.txt" ]; then
  { echo "cell: A1-grpc coordinator-boundary kills, quick (SIZE=200000, 4 chunks, seed 6)";
    echo "binaries: grpc-worker C16g pin, grpc-coordinator WITH --test-kill-at + always-on submit-intent journal row (re-pinned, see commit)";
    echo "funding: defaults (quick corpus); stores: $WORK on $(df -T "$WORK" | awk 'NR==2{print $2}')";
    echo "occurrence rule: N = chunk ordinal (deterministic across workers); ordinals 0 and 2";
    echo "REQUEST_SENT/RECEIPT_VALIDATED/OBSERVATION_JOURNALED/REFUSAL_RECEIVED UNAVAILABLE (see UNAVAILABLE file)";
    echo "frozen identities = same db file + pin (post-kill resubmits are legal same-identity resends; PK blocks duplicate commits)"; } > "$WORK/CELL.txt"
fi

# restart_grpc_workers <repdir>: restart a/b/c on same state dirs.
restart_grpc_workers() {
  local d="$1"
  local g="$d/work" pids=""
  for _ in $(seq 1 600); do pgrep -f "$g/grpc-.\.sqlite" >/dev/null || break; sleep 0.1; done
  pgrep -f "$g/grpc-.\.sqlite" >/dev/null && { note "$d: old workers never drained"; return 1; }
  for i in 0 1 2; do
    local w port
    w=$(printf '%s' abc | cut -c $((i + 1))); port=$((18443 + i))
    rm -f "$g/grpc-$w.ready"
    "$GRPC_WORKER" --bind "127.0.0.1:$port" --cert "$g/pki/grpc-server-$w.pem" \
      --key "$g/pki/grpc-server-$w.key" --client-ca "$g/pki/grpc-ca.pem" \
      --principal-map "$g/pki/grpc-principals.tsv" --authority "workload-$w" \
      --db "$g/grpc-$w.sqlite" --object-dir "$g/grpc-$w.obj" \
      --ready-file "$g/grpc-$w.ready" > "$d/grpc-$w-resume.log" 2>&1 &
    pids="$pids $!"
  done
  for _ in $(seq 1 200); do
    [ -f "$g/grpc-a.ready" ] && [ -f "$g/grpc-b.ready" ] && [ -f "$g/grpc-c.ready" ] && break
    sleep 0.1
  done
  [ -f "$g/grpc-a.ready" ] && [ -f "$g/grpc-b.ready" ] && [ -f "$g/grpc-c.ready" ] \
    || { note "$d: workers never restarted"; kill $pids 2>/dev/null || true; return 1; }
  echo "$pids"
}

kill_leftovers() {
  local d="$1"
  local pat="[${d:0:1}]${d:1}"
  pkill -9 -f "$pat" 2>/dev/null || true
  sleep 1
  if pgrep -f "$pat" >/dev/null; then
    note "$d: leftovers survive SIGKILL"
    return 1
  fi
}

stop_grpc_workers() { # repdir pids...
  local d="$1"; shift
  kill "$@" 2>/dev/null || true
  for _ in $(seq 1 600); do pgrep -f "$d/work/grpc-.\.sqlite" >/dev/null || return 0; sleep 0.1; done
  kill_leftovers "$d/work" || return 1
  for _ in $(seq 1 600); do pgrep -f "$d/work/grpc-.\.sqlite" >/dev/null || return 0; sleep 0.1; done
  note "$d: resumed workers never drained"; return 1
}

# armed_run <boundary> <ordinal> <repname>
armed_run() {
  local boundary="$1" ord="$2" rep="$3"
  local d="$WORK/$rep"
  local g="$d/work" ev="artifacts/grpc-events.tsv"
  if [ -f "$d/DONE" ]; then note "A1-grpc $boundary:$ord $rep: SKIP"; return 0; fi
  rm -rf "$d"
  local rc=0
  env GRPC_KILL_AT="$boundary:$ord" \
    flock -w 3600 "$COORD_LOCK" timeout -s KILL 3600 bash "$HERE/libexec-run-grpc.sh" "$d" "$SEED" "$SIZE" \
    >"$d.stdout.log" 2>&1 || rc=$?
  [ "$rc" -ne 0 ] || { note "A1-grpc $boundary:$ord $rep: armed run survived"; return 1; }
  kill_leftovers "$g" || return 1
  [ -f "$g/grpc-staging/kill-armed" ] || { note "A1-grpc $boundary:$ord $rep: kill sentinel missing"; return 1; }
  grep -q "test-kill-at: $boundary firing for ordinal $ord" "$d.stdout.log" \
    || { note "A1-grpc $boundary:$ord $rep: abort line missing"; return 1; }
  local bnum kill_epoch w trailing
  bnum=$(grep -n -P "\t$ord\tboundary\t$boundary reached, killing" "$d/$ev" | tail -1 | cut -d: -f1)
  [ -n "$bnum" ] || { note "A1-grpc $boundary:$ord $rep: no boundary row"; return 1; }
  kill_epoch=$(sed -n "${bnum}p" "$d/$ev" | cut -f2)
  w=$((ord % 3))
  tail -n +"$((bnum + 1))" "$d/$ev" | awk -F'\t' -v w="$w" -v o="$ord" \
    '$3==w && $4==o && $5!="boundary" {print "post-kill lifecycle: "$0; bad=1} END{exit bad}' \
    || { note "A1-grpc $boundary:$ord $rep: killed ordinal progressed after boundary"; return 1; }
  trailing=$(tail -n +"$((bnum + 1))" "$d/$ev" | wc -l)
  [ "$trailing" -eq 0 ] || note "A1-grpc $boundary:$ord $rep: $trailing trailing rows (abort race, recorded)"
  note "A1-grpc $boundary:$ord $rep: died at boundary epoch $kill_epoch (rc=$rc)"
  # Snapshot the coordinator db pre-resume (stable: coordinator dead).
  cp "$g/grpc-coord.sqlite" "$d/artifacts/grpc-coord-pre-resume.sqlite" 2>/dev/null || true
  for f in "$g/grpc-coord.sqlite-wal" "$g/grpc-coord.sqlite-shm"; do
    [ -f "$f" ] && cp "$f" "$d/artifacts/" 2>/dev/null || true
  done
  local pids
  pids="$(restart_grpc_workers "$d")" || return 1
  local rrc=0
  flock -w 3600 "$COORD_LOCK" timeout -s KILL 3600 \
    "$GRPC_COORD" run --ca "$g/pki/grpc-ca.pem" --cert "$g/pki/grpc-client.pem" \
    --key "$g/pki/grpc-client.key" --owner workload --db "$g/grpc-coord.sqlite" \
    --endpoint-a https://127.0.0.1:18443 --endpoint-b https://127.0.0.1:18444 \
    --endpoint-c https://127.0.0.1:18445 \
    --seed "$SEED" --size "$SIZE" --staging "$g/grpc-staging" \
    --output "$d/artifacts/grpc-final.bin" --events "$d/$ev" \
    --resume >>"$d.stdout.log" 2>&1 || rrc=$?
  # shellcheck disable=SC2086
  stop_grpc_workers "$d" $pids
  [ "$rrc" -eq 0 ] || { note "A1-grpc $boundary:$ord $rep: resume failed rc=$rrc"; return 1; }
  grep -q "final-verified" "$d/$ev" || { note "A1-grpc $boundary:$ord $rep: no final-verified"; return 1; }
  local digest final_epoch
  digest=$(sha256sum "$d/artifacts/grpc-final.bin" | cut -d' ' -f1)
  [ "$digest" = "$QUICK_PIN" ] || { note "A1-grpc $boundary:$ord $rep: digest $digest != pin"; return 1; }
  final_epoch=$(grep "final-verified" "$d/$ev" | tail -1 | cut -f2)
  echo "$rep kill_epoch=$kill_epoch final_epoch=$final_epoch recovery_ms=$((final_epoch - kill_epoch)) digest=$digest" >> "$WORK/A1-GRPC-RECOVERY.txt"
  date -u +%FT%TZ > "$d/DONE"
  note "A1-grpc $boundary:$ord $rep: PASS recovery $((final_epoch - kill_epoch)) ms"
}

[ -f "$WORK/A1-GRPC-RECOVERY.txt" ] || : > "$WORK/A1-GRPC-RECOVERY.txt"
for boundary in INTENT_JOURNALED RECEIPT_JOURNALED RESULT_VERIFIED RESULT_INSTALLED; do
  for ord in 0 2; do
    for i in $(seq 1 $REPS); do
      armed_run "$boundary" "$ord" "a1-grpc-$boundary-ord$ord-rep$i" || exit 1
    done
  done
done
cat > "$WORK/REQUEST_SENT-et-al-UNAVAILABLE.txt" <<'EOF'
UNAVAILABLE on the gRPC arm, with the exact check that fails for each:
- REQUEST_SENT: tonic submit is one atomic request/response call from the
  caller, so there is no inter-commit seam between request-sent and
  response-received (the durable boundary is RECEIPT_JOURNALED).
- RECEIPT_VALIDATED: receipt validation is a pure in-memory check with no
  journal commit (the commit is RECEIPT_JOURNALED).
- OBSERVATION_JOURNALED: manifest polling never journals (wait_manifest
  only reads the worker), so there is no observation commit to kill at.
- REFUSAL_RECEIVED: this arm has no backpressure-refusal vocabulary
  (submit errors fail fast with no retry rows), so no refusal row exists.
The binary rejects arming any of these at startup with the same reasons.
No run faked.
EOF
note "A1-GRPC SUITE PASS (40 kill+resume, 4 UNAVAILABLE)"
