#!/usr/bin/env bash
# One gRPC arm run into REP. Env: GRPC_WORKER, GRPC_COORD. Args: REP SEED SIZE.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REP="$1"; SEED="$2"; SIZE="$3"
mkdir -p "$REP"
ART="$REP/artifacts"; mkdir -p "$ART"
W="$REP/work"; mkdir -p "$W"

[ -x "${GRPC_WORKER:?}" ] || { echo "MISSING $GRPC_WORKER"; exit 1; }
[ -x "${GRPC_COORD:?}" ] || { echo "MISSING $GRPC_COORD"; exit 1; }
sha256sum "$GRPC_WORKER" "$GRPC_COORD" > "$ART/bin.sha256"

[ -d "$W/pki" ] || "$HERE/mk-test-pki.sh" "$W/pki"
lo_before=$(awk -F: '/lo:/{split($2,f," "); print f[1]":"f[9]}' /proc/net/dev)

PIDS=""
SAMPLER=""
cleanup() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true
  [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true
}
trap cleanup EXIT
# TEST-ONLY negative-control plumbing. Defaults preserve measured behavior.
WORKER_FAULT=()
[ "${GRPC_TEST_WRONG_TRANSFORM:-0}" = 1 ] && WORKER_FAULT+=(--test-wrong-transform)
[ -n "${GRPC_WORK_DELAY_MS:-}" ] && WORKER_FAULT+=(--test-work-delay-ms "$GRPC_WORK_DELAY_MS")
WORKER_RETENTION=()
[ -n "${GRPC_RETENTION_MS:-}" ] && WORKER_RETENTION+=(--output-retention-ms "$GRPC_RETENTION_MS")
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1))); port=$((18443 + i))
  # TEST-ONLY slow worker: per-chunk work delay lands on worker-c only.
  DELAY_C=()
  [ "$w" = c ] && [ -n "${GRPC_WORK_DELAY_C_MS:-}" ] \
    && DELAY_C=(--test-work-delay-ms "$GRPC_WORK_DELAY_C_MS")
  "$GRPC_WORKER" --bind "127.0.0.1:$port" --cert "$W/pki/grpc-server-$w.pem" \
    --key "$W/pki/grpc-server-$w.key" --client-ca "$W/pki/grpc-ca.pem" \
    --principal-map "$W/pki/grpc-principals.tsv" --authority "workload-$w" \
    --db "$W/grpc-$w.sqlite" --object-dir "$W/grpc-$w.obj" \
    "${WORKER_FAULT[@]}" "${WORKER_RETENTION[@]}" "${DELAY_C[@]}" \
    --ready-file "$W/grpc-$w.ready" > "$W/grpc-$w.log" 2>&1 &
  PIDS="$PIDS $!"
done
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  for _ in $(seq 1 100); do [ -f "$W/grpc-$w.ready" ] && break; sleep 0.1; done
  [ -f "$W/grpc-$w.ready" ] || { echo "worker $w did not start (see $W/grpc-$w.log)"; exit 1; }
done
"$HERE/sample.sh" "$ART/grpc-sample.tsv" $PIDS &
SAMPLER=$!
ms_now() { date +%s%N | cut -c1-13; }
START_MS=$(ms_now)
COORD_SWAP=()
[ -n "${GRPC_SWAP_INPUTS:-}" ] && COORD_SWAP+=(--test-swap-inputs "$GRPC_SWAP_INPUTS")
COORD_DROP=()
[ -n "${GRPC_DROP_INPUT:-}" ] && COORD_DROP+=(--test-drop-input "$GRPC_DROP_INPUT")
COORD_NOFETCH=()
[ "${GRPC_NO_FETCH:-0}" = 1 ] && COORD_NOFETCH+=(--test-no-fetch)
COORD_STOP=()
[ -n "${GRPC_FETCH_DELAY_MS:-}" ] && COORD_STOP+=(--test-fetch-delay-ms "$GRPC_FETCH_DELAY_MS")
[ -n "${GRPC_STALL_READ_MS:-}" ] && COORD_STOP+=(--test-stall-read-ms "$GRPC_STALL_READ_MS")
"$GRPC_COORD" run --ca "$W/pki/grpc-ca.pem" --cert "$W/pki/grpc-client.pem" \
  --key "$W/pki/grpc-client.key" --owner workload --db "$W/grpc-coord.sqlite" \
  --endpoint-a https://127.0.0.1:18443 --endpoint-b https://127.0.0.1:18444 \
  --endpoint-c https://127.0.0.1:18445 \
  --seed "$SEED" --size "$SIZE" --staging "$W/grpc-staging" \
  --output "$ART/grpc-final.bin" --events "$ART/grpc-events.tsv" \
  "${COORD_SWAP[@]}" "${COORD_DROP[@]}" "${COORD_NOFETCH[@]}" "${COORD_STOP[@]}"
END_MS=$(ms_now)
# Contract §6 negative controls: a dead metric collector or missing
# per-worker samples fails the run instead of passing silently.
check_samples() {
  local f="$1"; shift
  kill -0 "$SAMPLER" 2>/dev/null || { echo "metric sampler died mid-run"; return 1; }
  [ -s "$f" ] || { echo "metric sample file empty: $f"; return 1; }
  local pid
  for pid in "$@"; do
    grep -q -m1 "[[:space:]]$pid[[:space:]]" "$f" || { echo "missing metric samples for worker $pid"; return 1; }
  done
}
check_samples "$ART/grpc-sample.tsv" $PIDS || exit 1
kill "$SAMPLER" 2>/dev/null || true
kill $PIDS 2>/dev/null || true
wait 2>/dev/null || true
lo_after=$(awk -F: '/lo:/{split($2,f," "); print f[1]":"f[9]}' /proc/net/dev)
echo -e "wall_ms=$((END_MS - START_MS))\nlo_rx_tx_before=$lo_before\nlo_rx_tx_after=$lo_after" > "$ART/grpc-net.txt"
echo -e "restart-safety: Pure (deterministic re-execution, no external effects)\nfixture-schedule-schema: kimi interface-v1 1452f60 (c566751a...)" > "$ART/run-record.txt"
grep -q "first-usable-output" "$ART/grpc-events.tsv" || { echo "gRPC: no first-usable"; exit 1; }
grep -q "final-verified" "$ART/grpc-events.tsv" || { echo "gRPC: no final-verified"; exit 1; }
echo "gRPC arm done in $((END_MS - START_MS)) ms"
