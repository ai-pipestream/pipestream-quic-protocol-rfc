#!/usr/bin/env bash
# Quick correctness gate: tiny corpus end-to-end on BOTH arms.
# Fails on any error, digest mismatch, or missing artifact. No performance claim.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"  # scratch root for this run (state, logs, artifacts)

PS_AUTH="$REPO/examples/durable-transform-workload/workload-authority/target/release/workload-authority"
PS_COORD="$REPO/examples/durable-transform-workload/workload-coordinator/target/release/workload-coordinator"
GRPC_WORKER="$REPO/examples/durable-transform-grpc/target/release/grpc-worker"
GRPC_COORD="$REPO/examples/durable-transform-grpc/target/release/grpc-coordinator"

for b in "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD"; do
  [ -x "$b" ] || { echo "MISSING binary: $b (build first, record hashes)"; exit 1; }
done

mkdir -p "$WORK"
ART="$WORK/artifacts"
mkdir -p "$ART"
{
  echo "binaries:"
  sha256sum "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD"
  echo "toolchains:"
  rustc --version
  openssl version
  uname -a
} > "$ART/provenance.txt"

"$HERE/mk-test-pki.sh" "$WORK/pki"

SEED=7
SIZE=200000  # binary dataset: all byte values, uneven chunks

# ---------------- PipeStream arm ----------------
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  "$PS_AUTH" init-authority --state-db "$WORK/ps-$w.sqlite" \
    --object-dir "$WORK/ps-$w.obj" --authority "workload-$w" \
    --principal-map "$WORK/pki/ps-principals.tsv" --trust-system-clock
done
PIDS=""
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  port=$((17443 + i))
  "$PS_AUTH" serve --state-db "$WORK/ps-$w.sqlite" \
    --object-dir "$WORK/ps-$w.obj" --authority "workload-$w" \
    --principal-map "$WORK/pki/ps-principals.tsv" --trust-system-clock \
    --bind "127.0.0.1:$port" --cert "$WORK/pki/ps-server-$w.pem" \
    --key "$WORK/pki/ps-server-$w.key" --client-ca "$WORK/pki/ps-ca.pem" \
    --result-authority "localhost:$port" --ready-file "$WORK/ps-$w.ready" \
    > "$WORK/ps-$w.log" 2>&1 &
  PIDS="$PIDS $!"
done
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  for _ in $(seq 1 100); do [ -f "$WORK/ps-$w.ready" ] && break; sleep 0.1; done
  [ -f "$WORK/ps-$w.ready" ] || { echo "ps worker $w did not start"; kill $PIDS; exit 1; }
done
"$PS_COORD" run --ca "$WORK/pki/ps-ca.pem" --cert "$WORK/pki/ps-client.pem" \
  --key "$WORK/pki/ps-client.key" --owner workload \
  --journal-a "$WORK/ps-j0.sqlite" --journal-b "$WORK/ps-j1.sqlite" --journal-c "$WORK/ps-j2.sqlite" \
  --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444 --connect-c 127.0.0.1:17445 \
  --seed "$SEED" --size "$SIZE" --staging "$WORK/ps-staging" \
  --output "$ART/ps-final.bin" --events "$ART/ps-events.tsv"
kill $PIDS
wait 2>/dev/null || true

# ---------------- gRPC arm ----------------
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  port=$((18443 + i))
  "$GRPC_WORKER" --bind "127.0.0.1:$port" --cert "$WORK/pki/grpc-server-$w.pem" \
    --key "$WORK/pki/grpc-server-$w.key" --client-ca "$WORK/pki/grpc-ca.pem" \
    --principal-map "$WORK/pki/grpc-principals.tsv" --authority "workload-$w" \
    --db "$WORK/grpc-$w.sqlite" --object-dir "$WORK/grpc-$w.obj" \
    --ready-file "$WORK/grpc-$w.ready" > "$WORK/grpc-$w.log" 2>&1 &
  PIDS="$PIDS $!"
done
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  for _ in $(seq 1 100); do [ -f "$WORK/grpc-$w.ready" ] && break; sleep 0.1; done
  [ -f "$WORK/grpc-$w.ready" ] || { echo "grpc worker $w did not start"; kill $PIDS; exit 1; }
done
"$GRPC_COORD" run --ca "$WORK/pki/grpc-ca.pem" --cert "$WORK/pki/grpc-client.pem" \
  --key "$WORK/pki/grpc-client.key" --owner workload --db "$WORK/grpc-coord.sqlite" \
  --endpoint-a https://127.0.0.1:18443 --endpoint-b https://127.0.0.1:18444 \
  --endpoint-c https://127.0.0.1:18445 \
  --seed "$SEED" --size "$SIZE" --staging "$WORK/grpc-staging" \
  --output "$ART/grpc-final.bin" --events "$ART/grpc-events.tsv"
kill $PIDS
wait 2>/dev/null || true

# ---------------- gates ----------------
cmp "$ART/ps-final.bin" "$ART/grpc-final.bin" || { echo "ARM OUTPUT DIVERGENCE"; exit 1; }
for arm in ps grpc; do
  grep -q "first-usable-output" "$ART/$arm-events.tsv" || { echo "no first-usable milestone ($arm)"; exit 1; }
  grep -q "final-verified" "$ART/$arm-events.tsv" || { echo "no final-verified milestone ($arm)"; exit 1; }
done
sha256sum "$ART/ps-final.bin" "$ART/grpc-final.bin" > "$ART/final.sha256"
echo "QUICK PASS: both arms byte-identical, milestones present"
