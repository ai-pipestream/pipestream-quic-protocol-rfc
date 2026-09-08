#!/usr/bin/env bash
# Crash-recovery demonstrations. Each must end byte-exact via resume.
# Args: WORK SEED SIZE. Env: PS_AUTH, PS_COORD, GRPC_WORKER, GRPC_COORD.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$1"; SEED="$2"; SIZE="$3"
mkdir -p "$WORK"

# Fault 1 (PipeStream): SIGKILL one worker mid-run, restart it, resume.
F1="$WORK/fault1-worker-kill"; mkdir -p "$F1"
[ -d "$F1/pki" ] || "$HERE/mk-test-pki.sh" "$F1/pki"
PIDS=""
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1))); port=$((17443 + i))
  "$PS_AUTH" init-authority --state-db "$F1/ps-$w.sqlite" --object-dir "$F1/ps-$w.obj" \
    --authority "workload-$w" --principal-map "$F1/pki/ps-principals.tsv" --trust-system-clock
  "$PS_AUTH" serve --state-db "$F1/ps-$w.sqlite" --object-dir "$F1/ps-$w.obj" \
    --authority "workload-$w" --principal-map "$F1/pki/ps-principals.tsv" \
    --trust-system-clock --bind "127.0.0.1:$port" --cert "$F1/pki/ps-server-$w.pem" \
    --key "$F1/pki/ps-server-$w.key" --client-ca "$F1/pki/ps-ca.pem" \
    --result-authority "localhost:$port" --ready-file "$F1/ps-$w.ready" \
    > "$F1/ps-$w.log" 2>&1 &
  PIDS="$PIDS $!"
done
sleep 2
COARGS=(run --ca "$F1/pki/ps-ca.pem" --cert "$F1/pki/ps-client.pem" --key "$F1/pki/ps-client.key"
  --owner workload --journal-a "$F1/ps-j0.sqlite" --journal-b "$F1/ps-j1.sqlite"
  --journal-c "$F1/ps-j2.sqlite" --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444
  --connect-c 127.0.0.1:17445 --seed "$SEED" --size "$SIZE" --staging "$F1/staging"
  --output "$F1/artifacts-final.bin" --events "$F1/events.tsv")
"$PS_COORD" "${COARGS[@]}" > "$F1/run1.log" 2>&1 &
COORD=$!
sleep 3
# Kill worker b mid-run (SIGKILL: no graceful drain), then restart it.
BPID=$(pgrep -f "ps-b.sqlite" | head -n 1 || true)
[ -n "$BPID" ] || { echo "fault1: worker b not found"; kill $COORD $PIDS 2>/dev/null; exit 1; }
kill -9 "$BPID"
echo "fault1: killed worker b pid $BPID"
sleep 1
"$PS_AUTH" serve --state-db "$F1/ps-b.sqlite" --object-dir "$F1/ps-b.obj" \
  --authority "workload-b" --principal-map "$F1/pki/ps-principals.tsv" \
  --trust-system-clock --bind "127.0.0.1:17444" --cert "$F1/pki/ps-server-b.pem" \
  --key "$F1/pki/ps-server-b.key" --client-ca "$F1/pki/ps-ca.pem" \
  --result-authority "localhost:17444" --ready-file "$F1/ps-b.ready2" \
  > "$F1/ps-b-restart.log" 2>&1 &
PIDS="$PIDS $!"
wait "$COORD" || { echo "fault1: coordinator died, resuming"; "$PS_COORD" "${COARGS[@]}" --resume >> "$F1/run1.log" 2>&1; }
kill $PIDS 2>/dev/null || true
wait 2>/dev/null || true
[ -f "$F1/artifacts-final.bin" ] || { echo "fault1: no final output"; exit 1; }
echo "fault1 done"

# Fault 2 (gRPC): SIGKILL coordinator mid-run, rerun with --resume.
F2="$WORK/fault2-coord-kill"; mkdir -p "$F2"
[ -d "$F2/pki" ] || "$HERE/mk-test-pki.sh" "$F2/pki"
PIDS=""
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1))); port=$((18443 + i))
  "$GRPC_WORKER" --bind "127.0.0.1:$port" --cert "$F2/pki/grpc-server-$w.pem" \
    --key "$F2/pki/grpc-server-$w.key" --client-ca "$F2/pki/grpc-ca.pem" \
    --principal-map "$F2/pki/grpc-principals.tsv" --authority "workload-$w" \
    --db "$F2/grpc-$w.sqlite" --object-dir "$F2/grpc-$w.obj" \
    --ready-file "$F2/grpc-$w.ready" > "$F2/grpc-$w.log" 2>&1 &
  PIDS="$PIDS $!"
done
sleep 2
GARGS=(run --ca "$F2/pki/grpc-ca.pem" --cert "$F2/pki/grpc-client.pem" --key "$F2/pki/grpc-client.key"
  --owner workload --db "$F2/grpc-coord.sqlite"
  --endpoint-a https://127.0.0.1:18443 --endpoint-b https://127.0.0.1:18444
  --endpoint-c https://127.0.0.1:18445 --seed "$SEED" --size "$SIZE"
  --staging "$F2/staging" --output "$F2/final.bin" --events "$F2/events.tsv")
"$GRPC_COORD" "${GARGS[@]}" > "$F2/run1.log" 2>&1 &
COORD=$!
sleep 3
kill -9 "$COORD" 2>/dev/null || true
echo "fault2: killed coordinator"
sleep 1
"$GRPC_COORD" "${GARGS[@]}" --resume >> "$F2/run1.log" 2>&1
kill $PIDS 2>/dev/null || true
wait 2>/dev/null || true
[ -f "$F2/final.bin" ] || { echo "fault2: no final output"; exit 1; }
echo "fault2 done"
cmp "$F1/artifacts-final.bin" "$F2/final.bin" || { echo "FAULT OUTPUT DIVERGENCE"; exit 1; }
echo "FAULTS PASS: both recoveries byte-exact and cross-arm identical"
