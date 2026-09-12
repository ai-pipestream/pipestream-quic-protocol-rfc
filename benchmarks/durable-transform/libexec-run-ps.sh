#!/usr/bin/env bash
# One PipeStream arm run into REP. Env: PS_AUTH, PS_COORD. Args: REP SEED SIZE.
# Serialized by the caller (BENCHMARK.lock). Fails loudly on any gate miss.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REP="$1"; SEED="$2"; SIZE="$3"
mkdir -p "$REP"
ART="$REP/artifacts"; mkdir -p "$ART"
W="$REP/work"; mkdir -p "$W"

[ -x "${PS_AUTH:?}" ] || { echo "MISSING $PS_AUTH"; exit 1; }
[ -x "${PS_COORD:?}" ] || { echo "MISSING $PS_COORD"; exit 1; }
sha256sum "$PS_AUTH" "$PS_COORD" > "$ART/bin.sha256"

[ -d "$W/pki" ] || "$HERE/mk-test-pki.sh" "$W/pki"
lo_before=$(awk -F: '/lo:/{split($2,f," "); print f[1]":"f[9]}' /proc/net/dev)

PIDS=""
SAMPLER=""
COORD_PID=""
cleanup() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true
  [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true
  [ -n "$COORD_PID" ] && kill "$COORD_PID" 2>/dev/null || true
}
trap cleanup EXIT
# Physical storage funding for the authority DBs (MiB). Defaults preserve
# historical funding; larger corpora raise these via env and record them.
PHYS_DB_MIB="${PS_PHYS_DB_MIB:-256}"
PHYS_WAL_MIB="${PS_PHYS_WAL_MIB:-64}"
# TEST-ONLY negative-control plumbing. Defaults preserve measured behavior.
AUTH_FAULT=()
[ "${PS_TEST_WRONG_TRANSFORM:-0}" = 1 ] && AUTH_FAULT+=(--test-wrong-transform)
[ -n "${PS_WORK_DELAY_MS:-}" ] && AUTH_FAULT+=(--test-work-delay-ms "$PS_WORK_DELAY_MS")
COORD_NOFETCH=()
[ "${PS_NO_FETCH:-0}" = 1 ] && COORD_NOFETCH+=(--test-no-fetch)
COORD_STOP=()
[ -n "${PS_FETCH_DELAY_MS:-}" ] && COORD_STOP+=(--test-fetch-delay-ms "$PS_FETCH_DELAY_MS")
[ -n "${PS_STALL_READ_MS:-}" ] && COORD_STOP+=(--test-stall-read-ms "$PS_STALL_READ_MS")
# Coordinator pipelining (C16e): pipelined by default; PS_SERIAL=1 keeps the
# old serial order; PS_PENDING_LIMIT overrides the 16 in-flight cap.
[ "${PS_SERIAL:-0}" = 1 ] && COORD_STOP+=(--serial)
[ -n "${PS_PENDING_LIMIT:-}" ] && COORD_STOP+=(--pending-limit "$PS_PENDING_LIMIT")
COORD_KILL=()
[ "${PS_KILL_AFTER_FIRST_VERIFIED:-0}" = 1 ] && COORD_KILL+=(--test-kill-after-first-verified)
COORD_SWAP=()
[ -n "${PS_SWAP_INPUTS:-}" ] && COORD_SWAP+=(--test-swap-inputs "$PS_SWAP_INPUTS")
COORD_EXEC=()
[ -n "${PS_EXECUTION_MS:-}" ] && COORD_EXEC+=(--execution-ms "$PS_EXECUTION_MS")
COORD_DROP=()
[ -n "${PS_DROP_INPUT:-}" ] && COORD_DROP+=(--test-drop-input "$PS_DROP_INPUT")
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1))); port=$((17443 + i))
  "$PS_AUTH" init-authority --state-db "$W/ps-$w.sqlite" --object-dir "$W/ps-$w.obj" \
    --authority "workload-$w" --principal-map "$W/pki/ps-principals.tsv" \
    --trust-system-clock --db-mib "$PHYS_DB_MIB" --wal-mib "$PHYS_WAL_MIB"
  # TEST-ONLY F3 arming: the commit-time kill flag lands on worker-c only.
  KILL_C=()
  [ "$w" = c ] && [ -n "${PS_KILL_C_AFTER_OUTPUT:-}" ] \
    && KILL_C=(--test-kill-after-output-installed "$PS_KILL_C_AFTER_OUTPUT")
  # TEST-ONLY slow worker: per-chunk work delay lands on worker-c only.
  DELAY_C=()
  [ "$w" = c ] && [ -n "${PS_WORK_DELAY_C_MS:-}" ] \
    && DELAY_C=(--test-work-delay-ms "$PS_WORK_DELAY_C_MS")
  "$PS_AUTH" serve --state-db "$W/ps-$w.sqlite" --object-dir "$W/ps-$w.obj" \
    --authority "workload-$w" --principal-map "$W/pki/ps-principals.tsv" \
    --trust-system-clock --db-mib "$PHYS_DB_MIB" --wal-mib "$PHYS_WAL_MIB" \
    "${AUTH_FAULT[@]}" "${KILL_C[@]}" "${DELAY_C[@]}" \
    --bind "127.0.0.1:$port" --cert "$W/pki/ps-server-$w.pem" \
    --key "$W/pki/ps-server-$w.key" --client-ca "$W/pki/ps-ca.pem" \
    --result-authority "localhost:$port" --ready-file "$W/ps-$w.ready" \
    > "$W/ps-$w.log" 2>&1 &
  PIDS="$PIDS $!"
done
# C16f: the sampler starts before the ready-wait so short-lived workers
# are caught by the t=0 burst; the coordinator (spawned later) joins via
# the pidfile, re-read every pass. Its exit status is wait-propagated and
# still fails the run. check_samples requires every worker; the
# coordinator rows are required only when it lived >= 1 s (>= 5 ticks):
# a ~24 ms empty-corpus coordinator cannot be sampled, and its columns
# stay honestly absent for that run.
: > "$W/sampler-pids.txt"
SAMPLE_PIDFILE="$W/sampler-pids.txt" "$HERE/sample.sh" "$ART/ps-sample.tsv" $PIDS &
SAMPLER=$!
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  for _ in $(seq 1 100); do [ -f "$W/ps-$w.ready" ] && break; sleep 0.1; done
  [ -f "$W/ps-$w.ready" ] || { echo "worker $w did not start (see $W/ps-$w.log)"; exit 1; }
done
ms_now() { date +%s%N | cut -c1-13; }
START_MS=$(ms_now)
"$PS_COORD" run --ca "$W/pki/ps-ca.pem" --cert "$W/pki/ps-client.pem" \
  --key "$W/pki/ps-client.key" --owner workload \
  --journal-a "$W/ps-j0.sqlite" --journal-b "$W/ps-j1.sqlite" --journal-c "$W/ps-j2.sqlite" \
  --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444 --connect-c 127.0.0.1:17445 \
  --seed "$SEED" --size "$SIZE" --staging "$W/ps-staging" \
  --output "$ART/ps-final.bin" --events "$ART/ps-events.tsv" \
  "${COORD_SWAP[@]}" "${COORD_EXEC[@]}" "${COORD_DROP[@]}" "${COORD_NOFETCH[@]}" "${COORD_KILL[@]}" "${COORD_STOP[@]}" &
COORD_PID=$!
echo "$COORD_PID" > "$W/sampler-pids.txt"
COORD_RC=0
wait "$COORD_PID" || COORD_RC=$?
END_MS=$(ms_now)
[ "$COORD_RC" -eq 0 ] || { echo "PS coordinator exit $COORD_RC"; exit "$COORD_RC"; }
# Contract §6 negative controls: a dead metric collector or missing
# per-worker samples fails the run instead of passing silently.
check_samples() {
  local f="$1" wall="$2" coord="$3"; shift 3
  kill -0 "$SAMPLER" 2>/dev/null || { echo "metric sampler died mid-run"; return 1; }
  [ -s "$f" ] || { echo "metric sample file empty: $f"; return 1; }
  local pid
  for pid in "$@"; do
    grep -q -m1 "[[:space:]]$pid[[:space:]]" "$f" || { echo "missing metric samples for worker $pid"; return 1; }
  done
  if [ "$wall" -ge 1000 ]; then
    grep -q -m1 "[[:space:]]$coord[[:space:]]" "$f" \
      || { echo "missing metric samples for coordinator $coord"; return 1; }
  else
    echo "coordinator short-lived (${wall} ms): coordinator sample rows optional"
  fi
}
check_samples "$ART/ps-sample.tsv" "$((END_MS - START_MS))" "$COORD_PID" $PIDS || exit 1
kill "$SAMPLER" 2>/dev/null || true
kill $PIDS 2>/dev/null || true
wait 2>/dev/null || true
lo_after=$(awk -F: '/lo:/{split($2,f," "); print f[1]":"f[9]}' /proc/net/dev)
echo -e "wall_ms=$((END_MS - START_MS))\nlo_rx_tx_before=$lo_before\nlo_rx_tx_after=$lo_after" > "$ART/ps-net.txt"
echo -e "restart-safety: Pure (deterministic re-execution, no external effects)\nfixture-schedule-schema: kimi interface-v1 1452f60 (c566751a...)\nphys-funding-mib: db=$PHYS_DB_MIB wal=$PHYS_WAL_MIB" > "$ART/run-record.txt"
# ALLOW_VACUOUS=1 (empty corpus): zero chunks admit nothing, so the
# first-usable milestone cannot fire; final-verified + digest govern.
if [ "${ALLOW_VACUOUS:-0}" != 1 ]; then
  grep -q "first-usable-output" "$ART/ps-events.tsv" || { echo "PS: no first-usable"; exit 1; }
fi
grep -q "final-verified" "$ART/ps-events.tsv" || { echo "PS: no final-verified"; exit 1; }
echo "PS arm done in $((END_MS - START_MS)) ms"
