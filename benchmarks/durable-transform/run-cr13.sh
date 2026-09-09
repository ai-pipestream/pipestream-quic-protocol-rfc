#!/usr/bin/env bash
# CR13 arms: client-side object-stream deadline semantics, evidenced
# independently of any driver. `idle`: payload stalls while varied
# unrelated wire requests continue — the idle deadline must still fire.
# `lifetime`: slow continuous progress past the absolute stream lifetime
# — progress must not extend it. `complete`: in-cap transfer commits
# (non-vacuity control). `cancel-neg`: an injected session shutdown must
# NOT earn PASS (negative control on the predicate). Each arm gets a
# FRESH worker (scope membership seals once per state dir).
# Env: PS_AUTH, PS_COORD, JAVA_JAR (for --java). Args: WORK [SEED] [--java].
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$1"; SEED="${2:-6}"; MODE="${3:-rust}"
ART="$WORK/artifacts"
mkdir -p "$ART"
[ -x "${PS_AUTH:?}" ] || { echo "MISSING $PS_AUTH"; exit 1; }
[ -x "${PS_COORD:?}" ] || { echo "MISSING $PS_COORD"; exit 1; }

[ -d "$WORK/pki" ] || "$HERE/mk-test-pki.sh" "$WORK/pki"

PIDS=""
cleanup() { [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true; }
trap cleanup EXIT

start_worker() { # <dir>
  local W="$1" port=17443
  mkdir -p "$W"
  if [ "$MODE" = "--java" ]; then
    local JAR="${JAVA_JAR:?}" root="$W/java-a.root"
    mkdir -p "$root"
    java --enable-native-access=ALL-UNNAMED -cp "$JAR" ai.pipestream.quic.v2.V2Main init-authority \
      --root "$root" --authority "workload-a" --result-authority "localhost:$port" \
      --trust-system-clock
    java --enable-native-access=ALL-UNNAMED -cp "$JAR" ai.pipestream.quic.v2.V2Main serve \
      --root "$root" --authority "workload-a" --result-authority "localhost:$port" \
      --bind "127.0.0.1:$port" --cert "$WORK/pki/ps-server-a.pem" \
      --key "$WORK/pki/ps-server-a.key" --client-ca "$WORK/pki/ps-ca.pem" \
      --principal-map "$WORK/pki/ps-principals.tsv" --trust-system-clock \
      --ready-file "$W/ps-a.ready" > "$W/ps-a.log" 2>&1 &
  else
    "$PS_AUTH" init-authority --state-db "$W/ps-a.sqlite" --object-dir "$W/ps-a.obj" \
      --authority "workload-a" --principal-map "$WORK/pki/ps-principals.tsv" \
      --trust-system-clock
    "$PS_AUTH" serve --state-db "$W/ps-a.sqlite" --object-dir "$W/ps-a.obj" \
      --authority "workload-a" --principal-map "$WORK/pki/ps-principals.tsv" \
      --trust-system-clock --bind "127.0.0.1:$port" --cert "$WORK/pki/ps-server-a.pem" \
      --key "$WORK/pki/ps-server-a.key" --client-ca "$WORK/pki/ps-ca.pem" \
      --result-authority "localhost:$port" --ready-file "$W/ps-a.ready" \
      > "$W/ps-a.log" 2>&1 &
  fi
  PIDS="$PIDS $!"
  for _ in $(seq 1 300); do [ -f "$W/ps-a.ready" ] && break; sleep 0.1; done
  [ -f "$W/ps-a.ready" ] || { echo "probe worker did not start (see $W/ps-a.log)"; exit 1; }
}

stop_worker() {
  [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true
  wait 2>/dev/null || true
  PIDS=""
}

TLS=(--ca "$WORK/pki/ps-ca.pem" --cert "$WORK/pki/ps-client.pem" --key "$WORK/pki/ps-client.key")
for arm in idle lifetime complete cancel-neg; do
  AW="$WORK/$arm"
  start_worker "$AW"
  timeout 300 "$PS_COORD" probe "${TLS[@]}" --owner workload --authority workload-a \
    --connect "127.0.0.1:17443" --journal "$AW/probe.sqlite" \
    --creation-sequence 1 --seed "$SEED" --arm "$arm" --events "$ART/probe-events.tsv" \
    || { echo "CR13 $arm arm FAILED ($MODE worker)"; exit 1; }
  stop_worker
done
grep -q "probe-complete" "$ART/probe-events.tsv" || { echo "CR13: complete arm left no commit row"; exit 1; }
grep -q "probe-cancel-rejected" "$ART/probe-events.tsv" || { echo "CR13: cancel-neg arm left no rejection row"; exit 1; }
echo "CR13 PASS ($MODE worker): idle/lifetime fingerprinted, complete committed, injected cancellation rejected"
