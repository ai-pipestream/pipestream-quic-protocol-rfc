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
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1))); port=$((18443 + i))
  "$GRPC_WORKER" --bind "127.0.0.1:$port" --cert "$W/pki/grpc-server-$w.pem" \
    --key "$W/pki/grpc-server-$w.key" --client-ca "$W/pki/grpc-ca.pem" \
    --principal-map "$W/pki/grpc-principals.tsv" --authority "workload-$w" \
    --db "$W/grpc-$w.sqlite" --object-dir "$W/grpc-$w.obj" \
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
"$GRPC_COORD" run --ca "$W/pki/grpc-ca.pem" --cert "$W/pki/grpc-client.pem" \
  --key "$W/pki/grpc-client.key" --owner workload --db "$W/grpc-coord.sqlite" \
  --endpoint-a https://127.0.0.1:18443 --endpoint-b https://127.0.0.1:18444 \
  --endpoint-c https://127.0.0.1:18445 \
  --seed "$SEED" --size "$SIZE" --staging "$W/grpc-staging" \
  --output "$ART/grpc-final.bin" --events "$ART/grpc-events.tsv"
END_MS=$(ms_now)
kill "$SAMPLER" 2>/dev/null || true
kill $PIDS 2>/dev/null || true
wait 2>/dev/null || true
lo_after=$(awk -F: '/lo:/{split($2,f," "); print f[1]":"f[9]}' /proc/net/dev)
echo -e "wall_ms=$((END_MS - START_MS))\nlo_rx_tx_before=$lo_before\nlo_rx_tx_after=$lo_after" > "$ART/grpc-net.txt"
echo -e "restart-safety: Pure (deterministic re-execution, no external effects)\nfixture-schedule-schema: kimi interface-v1 1452f60 (c566751a...)" > "$ART/run-record.txt"
grep -q "first-usable-output" "$ART/grpc-events.tsv" || { echo "gRPC: no first-usable"; exit 1; }
grep -q "final-verified" "$ART/grpc-events.tsv" || { echo "gRPC: no final-verified"; exit 1; }
echo "gRPC arm done in $((END_MS - START_MS)) ms"
