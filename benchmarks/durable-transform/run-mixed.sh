#!/usr/bin/env bash
# Mixed-language arm: two Rust worker-authorities + one Java worker-authority
# (V2Main, ReferenceApplications incl. transform/v2), driven by the Rust
# PipeStream coordinator. Same gates as libexec-run-ps.sh; the run record
# labels which worker ran Java, and EXPECTED_SHA (optional) asserts the
# final is byte-identical to the pinned all-Rust digest.
# Env: PS_AUTH, PS_COORD, JAVA_JAR. Args: WORK SEED SIZE [JAVA_WORKER=a|b|c].
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$1"; SEED="$2"; SIZE="$3"; JWORK="${4:-c}"
JAR="${JAVA_JAR:?}"
[ -f "$JAR" ] || { echo "MISSING Java jar: $JAR"; exit 1; }
command -v java >/dev/null || { echo "MISSING java on PATH"; exit 1; }

REP="$WORK"; ART="$REP/artifacts"; W="$REP/work"
mkdir -p "$ART" "$W"
[ -x "${PS_AUTH:?}" ] || { echo "MISSING $PS_AUTH"; exit 1; }
[ -x "${PS_COORD:?}" ] || { echo "MISSING $PS_COORD"; exit 1; }
sha256sum "$PS_AUTH" "$PS_COORD" "$JAR" > "$ART/bin.sha256"

[ -d "$W/pki" ] || "$HERE/mk-test-pki.sh" "$W/pki"
lo_before=$(awk -F: '/lo:/{split($2,f," "); print f[1]":"f[9]}' /proc/net/dev)

PIDS=""
SAMPLER=""
COORD_PID=""
JAVA_WORKER_PID=""
cleanup() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true
  [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true
  [ -n "$COORD_PID" ] && kill "$COORD_PID" 2>/dev/null || true
}
trap cleanup EXIT

JAVA_MAIN="ai.pipestream.quic.v2.V2Main"
# Physical storage funding for the Rust authority DBs (MiB). Defaults
# preserve historical funding; larger corpora raise these via env.
PHYS_DB_MIB="${PS_PHYS_DB_MIB:-256}"
PHYS_WAL_MIB="${PS_PHYS_WAL_MIB:-64}"
# Same for the Java authority (V2Main init-authority/serve accept
# --db-mib/--wal-mib since Claude ab59dafb; defaults match too).
JAVA_DB_MIB="${JAVA_PHYS_DB_MIB:-256}"
JAVA_WAL_MIB="${JAVA_PHYS_WAL_MIB:-64}"
# TEST-ONLY negative-control plumbing for the Rust workers.
AUTH_FAULT=()
[ "${PS_TEST_WRONG_TRANSFORM:-0}" = 1 ] && AUTH_FAULT+=(--test-wrong-transform)
[ -n "${PS_WORK_DELAY_MS:-}" ] && AUTH_FAULT+=(--test-work-delay-ms "$PS_WORK_DELAY_MS")
MIXED_NO_FETCH=()
[ "${PS_NO_FETCH:-0}" = 1 ] && MIXED_NO_FETCH+=(--test-no-fetch)
MIXED_KILL=()
[ "${PS_KILL_AFTER_FIRST_VERIFIED:-0}" = 1 ] && MIXED_KILL+=(--test-kill-after-first-verified)
MIXED_PIPE=()
[ "${PS_SERIAL:-0}" = 1 ] && MIXED_PIPE+=(--serial)
[ -n "${PS_PENDING_LIMIT:-}" ] && MIXED_PIPE+=(--pending-limit "$PS_PENDING_LIMIT")
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1))); port=$((17443 + i))
  if [ "$w" = "$JWORK" ]; then
    root="$W/java-$w.root"; mkdir -p "$root"
    java --enable-native-access=ALL-UNNAMED -cp "$JAR" "$JAVA_MAIN" init-authority \
      --root "$root" --authority "workload-$w" --result-authority "localhost:$port" \
      --trust-system-clock --db-mib "$JAVA_DB_MIB" --wal-mib "$JAVA_WAL_MIB"
    java --enable-native-access=ALL-UNNAMED -cp "$JAR" "$JAVA_MAIN" serve \
      --root "$root" --authority "workload-$w" --result-authority "localhost:$port" \
      --bind "127.0.0.1:$port" --cert "$W/pki/ps-server-$w.pem" \
      --key "$W/pki/ps-server-$w.key" --client-ca "$W/pki/ps-ca.pem" \
      --principal-map "$W/pki/ps-principals.tsv" --trust-system-clock \
      --db-mib "$JAVA_DB_MIB" --wal-mib "$JAVA_WAL_MIB" \
      --ready-file "$W/ps-$w.ready" \
      > "$W/ps-$w.log" 2>&1 &
    JAVA_WORKER_PID=$!
  else
    "$PS_AUTH" init-authority --state-db "$W/ps-$w.sqlite" --object-dir "$W/ps-$w.obj" \
      --authority "workload-$w" --principal-map "$W/pki/ps-principals.tsv" \
      --trust-system-clock --db-mib "$PHYS_DB_MIB" --wal-mib "$PHYS_WAL_MIB"
    "$PS_AUTH" serve --state-db "$W/ps-$w.sqlite" --object-dir "$W/ps-$w.obj" \
      --authority "workload-$w" --principal-map "$W/pki/ps-principals.tsv" \
      --trust-system-clock --db-mib "$PHYS_DB_MIB" --wal-mib "$PHYS_WAL_MIB" \
      "${AUTH_FAULT[@]}" \
      --bind "127.0.0.1:$port" --cert "$W/pki/ps-server-$w.pem" \
      --key "$W/pki/ps-server-$w.key" --client-ca "$W/pki/ps-ca.pem" \
      --result-authority "localhost:$port" --ready-file "$W/ps-$w.ready" \
      > "$W/ps-$w.log" 2>&1 &
  fi
  PIDS="$PIDS $!"
done
# C16f: the sampler starts before the ready-wait so short-lived workers
# are caught by the t=0 burst (the Java worker needs seconds to boot, so
# its PID is already known); the coordinator joins via the pidfile.
# Coordinator rows required only when it lived >= 1 s (see
# libexec-run-ps.sh).
[ -n "$JAVA_WORKER_PID" ] || { echo "MIXED: java worker PID not captured"; exit 1; }
: > "$W/sampler-pids.txt"
SAMPLE_JAVA_PID="$JAVA_WORKER_PID" SAMPLE_GC_OUT="$ART/mixed-gc.tsv" \
  SAMPLE_PIDFILE="$W/sampler-pids.txt" \
  "$HERE/sample.sh" "$ART/mixed-sample.tsv" $PIDS &
SAMPLER=$!
for i in 0 1 2; do
  w=$(printf '%s' abc | cut -c $((i + 1)))
  for _ in $(seq 1 300); do [ -f "$W/ps-$w.ready" ] && break; sleep 0.1; done
  [ -f "$W/ps-$w.ready" ] || { echo "worker $w did not start (see $W/ps-$w.log)"; exit 1; }
done
ms_now() { date +%s%N | cut -c1-13; }
START_MS=$(ms_now)
"$PS_COORD" run --ca "$W/pki/ps-ca.pem" --cert "$W/pki/ps-client.pem" \
  --key "$W/pki/ps-client.key" --owner workload \
  --journal-a "$W/ps-j0.sqlite" --journal-b "$W/ps-j1.sqlite" --journal-c "$W/ps-j2.sqlite" \
  --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444 --connect-c 127.0.0.1:17445 \
  --seed "$SEED" --size "$SIZE" --staging "$W/ps-staging" \
  --output "$ART/mixed-final.bin" --events "$ART/mixed-events.tsv" \
  ${PS_SWAP_INPUTS:+--test-swap-inputs "$PS_SWAP_INPUTS"} \
  ${PS_EXECUTION_MS:+--execution-ms "$PS_EXECUTION_MS"} \
  ${PS_DROP_INPUT:+--test-drop-input "$PS_DROP_INPUT"} \
  "${MIXED_NO_FETCH[@]}" "${MIXED_KILL[@]}" "${MIXED_PIPE[@]}" &
COORD_PID=$!
echo "$COORD_PID" > "$W/sampler-pids.txt"
COORD_RC=0
wait "$COORD_PID" || COORD_RC=$?
END_MS=$(ms_now)
[ "$COORD_RC" -eq 0 ] || { echo "MIXED coordinator exit $COORD_RC"; exit "$COORD_RC"; }
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
check_samples "$ART/mixed-sample.tsv" "$((END_MS - START_MS))" "$COORD_PID" $PIDS || exit 1
# C16f: a jstat gap fails the sample (never zero-filled).
[ -s "$ART/mixed-gc.tsv" ] || { echo "metric gc sample file empty"; exit 1; }
grep -q -m1 "JSTAT_GAP" "$ART/mixed-gc.tsv" && { echo "jstat gap in metric sample"; exit 1; }
grep -q -m1 "[[:space:]]$JAVA_WORKER_PID[[:space:]]" "$ART/mixed-gc.tsv" \
  || { echo "missing jstat samples for java worker $JAVA_WORKER_PID"; exit 1; }
kill "$SAMPLER" 2>/dev/null || true
kill $PIDS 2>/dev/null || true
wait 2>/dev/null || true
lo_after=$(awk -F: '/lo:/{split($2,f," "); print f[1]":"f[9]}' /proc/net/dev)
echo -e "wall_ms=$((END_MS - START_MS))\nlo_rx_tx_before=$lo_before\nlo_rx_tx_after=$lo_after" > "$ART/mixed-net.txt"
echo -e "java-worker: $JWORK\njava-jar: $JAR\nrestart-safety: Pure (Rust) / IDEMPOTENT (Java transform/v2)\nfixture-schedule-schema: kimi interface-v1 1452f60 (c566751a...)\nrust-phys-funding-mib: db=$PHYS_DB_MIB wal=$PHYS_WAL_MIB\njava-phys-funding-mib: db=$JAVA_DB_MIB wal=$JAVA_WAL_MIB" > "$ART/run-record.txt"
# ALLOW_VACUOUS=1 (empty corpus): see libexec-run-ps.sh.
if [ "${ALLOW_VACUOUS:-0}" != 1 ]; then
  grep -q "first-usable-output" "$ART/mixed-events.tsv" || { echo "MIXED: no first-usable"; exit 1; }
fi
grep -q "final-verified" "$ART/mixed-events.tsv" || { echo "MIXED: no final-verified"; exit 1; }
if [ -n "${EXPECTED_SHA:-}" ]; then
  echo "$EXPECTED_SHA  $ART/mixed-final.bin" | sha256sum -c - || { echo "MIXED DIGEST MISMATCH"; exit 1; }
fi
echo "MIXED arm done in $((END_MS - START_MS)) ms (java worker: $JWORK)"
