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
lo_before=$(awk -F'[: ]+' '/^lo:/{print $3":"$11}' /proc/net/dev)

PIDS=""
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1))); port=$((17443 + i))
  "$PS_AUTH" init-authority --state-db "$W/ps-$w.sqlite" --object-dir "$W/ps-$w.obj" \
    --authority "workload-$w" --principal-map "$W/pki/ps-principals.tsv" \
    --trust-system-clock
  "$PS_AUTH" serve --state-db "$W/ps-$w.sqlite" --object-dir "$W/ps-$w.obj" \
    --authority "workload-$w" --principal-map "$W/pki/ps-principals.tsv" \
    --trust-system-clock --bind "127.0.0.1:$port" --cert "$W/pki/ps-server-$w.pem" \
    --key "$W/pki/ps-server-$w.key" --client-ca "$W/pki/ps-ca.pem" \
    --result-authority "localhost:$port" --ready-file "$W/ps-$w.ready" \
    > "$W/ps-$w.log" 2>&1 &
  PIDS="$PIDS $!"
done
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  for _ in $(seq 1 100); do [ -f "$W/ps-$w.ready" ] && break; sleep 0.1; done
done
"$HERE/sample.sh" "$ART/ps-sample.tsv" $PIDS &
SAMPLER=$!
START_MS=$(date +%s%3N)
"$PS_COORD" run --ca "$W/pki/ps-ca.pem" --cert "$W/pki/ps-client.pem" \
  --key "$W/pki/ps-client.key" --owner workload \
  --journal-a "$W/ps-j0.sqlite" --journal-b "$W/ps-j1.sqlite" --journal-c "$W/ps-j2.sqlite" \
  --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444 --connect-c 127.0.0.1:17445 \
  --seed "$SEED" --size "$SIZE" --staging "$W/ps-staging" \
  --output "$ART/ps-final.bin" --events "$ART/ps-events.tsv"
END_MS=$(date +%s%3N)
kill "$SAMPLER" 2>/dev/null || true
kill $PIDS 2>/dev/null || true
wait 2>/dev/null || true
lo_after=$(awk -F'[: ]+' '/^lo:/{print $3":"$11}' /proc/net/dev)
echo -e "wall_ms=$((END_MS - START_MS))\nlo_rx_tx_before=$lo_before\nlo_rx_tx_after=$lo_after" > "$ART/ps-net.txt"
grep -q "first-usable-output" "$ART/ps-events.tsv" || { echo "PS: no first-usable"; exit 1; }
grep -q "final-verified" "$ART/ps-events.tsv" || { echo "PS: no final-verified"; exit 1; }
echo "PS arm done in $((END_MS - START_MS)) ms"
