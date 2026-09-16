#!/usr/bin/env bash
# durable-index-build gate: both directions x3 + cancel + kill/resume demos.
# Usage: run-index.sh WORK
#
# Self-contained: builds Rust binaries and the Java example jar, mints test
# PKI, launches one authority pair per direction, and gates every run.
# Whole-run lock serializes concurrent invocations; each coordinator run has
# its own timeout inside that lock.
#
# Gate corpus: 2 files x 128 words, seed 6 -> index digest pinned below.
# Cancel demo: 2 files x 8192 words (TF slow enough that the subtree cancel
# lands mid-flight), cancel the last file -> 4x CANCELLED, scope sums gated.
# The DESIGN default (8 x 2048) exceeds the example authority's default
# aggregate admission capacity, so it is NOT in the gate (see README).
set -euo pipefail

WORK="${1:?Usage: run-index.sh WORK}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms256m -Xmx2g}"

IA="$HERE/target/debug/index-authority"
IC="$HERE/target/debug/index-coordinator"
JAR="$HERE/java/target/durable-index-build-0.1.0-SNAPSHOT-all.jar"
MKPKI="$REPO/benchmarks/durable-transform/mk-test-pki.sh"

PIN_SMALL="693796e4d81888a85a05f92bd5ffa5b95e524213a56e5080718b7d64013d96c5"
PIN_CANCEL="5e0c8c588204085de69ee99bec45aebae145411aef2b7f0540ab5fd4e9aca87c"

mkdir -p "$WORK"
exec {LOCKFD}>"$WORK/run.lock"
flock -w 3600 "$LOCKFD"
note() { echo "$1" | tee -a "$WORK/RUN-LOG.txt"; }
: > "$WORK/RUN-LOG.txt"

note "building rust binaries"
cargo build --offline --manifest-path "$HERE/Cargo.toml" -p index-authority -p index-coordinator 2>&1 | tail -1
[ -x "$IA" ] && [ -x "$IC" ] || { note "MISSING rust binaries"; exit 1; }

note "building java example jar"
(cd "$HERE/java" && mvn -q package -DskipTests 2>&1 | tail -2)
[ -f "$JAR" ] || { note "MISSING java jar"; exit 1; }
sha256sum "$IA" "$IC" "$JAR" > "$WORK/gate.bin.sha256"

PORTS=""
teardown() { for port in $PORTS; do pkill -9 -f -- "--bind 127.0.0.1:$port" 2>/dev/null || true; done; PORTS=""; }
trap teardown EXIT

wait_ready() { # <ready-file> <name>
  for _ in $(seq 1 30); do [ -f "$1" ] && return 0; sleep 2; done
  note "server $2 never ready"; return 1
}

# launch_rust <dir> <authority> <port> <cert> <key> [reader-endpoint]
launch_rust() {
  local dir="$1" auth="$2" port="$3" cert="$4" key="$5" reader="${6:-}"
  rm -f "$dir/$auth.ready"
  local extra=()
  [ -n "$reader" ] && extra=(--reader-endpoint "$reader" --reader-ca "$dir/pki/ps-ca.pem" --reader-cert "$dir/pki/ps-client.pem" --reader-key "$dir/pki/ps-client.key" --reader-owner workload)
  setsid -f "$IA" serve --state-db "$dir/$auth.sqlite" --object-dir "$dir/$auth.obj" \
    --authority "$auth" --principal-map "$dir/pki/ps-principals.tsv" --trust-system-clock \
    --bind "127.0.0.1:$port" --cert "$dir/pki/$cert" --key "$dir/pki/$key" \
    --client-ca "$dir/pki/ps-ca.pem" --result-authority "127.0.0.1:$port" \
    --ready-file "$dir/$auth.ready" "${extra[@]}" </dev/null >>"$WORK/$auth-srv.log" 2>&1
  PORTS="$PORTS $port"
  wait_ready "$dir/$auth.ready" "$auth"
}

# launch_java <dir> <authority> <port> <server-cert> [reader-endpoint]
launch_java() {
  local dir="$1" auth="$2" port="$3" scert="$4" reader="${5:-}"
  rm -rf "$dir/$auth.jwork"; rm -f "$dir/$auth.ready"
  java --enable-native-access=ALL-UNNAMED -cp "$JAR" ai.pipestream.examples.index.IndexMain \
    init-authority --root "$dir/$auth.jwork" --authority "$auth" --result-authority "localhost:$port" \
    >>"$WORK/$auth-srv.log" 2>&1
  local extra=()
  [ -n "$reader" ] && extra=(--reader-endpoint "$reader" --reader-ca "$dir/pki/ps-ca.pem" --reader-cert "$dir/pki/ps-client.pem" --reader-key "$dir/pki/ps-client.key" --reader-owner workload)
  setsid -f java --enable-native-access=ALL-UNNAMED -cp "$JAR" ai.pipestream.examples.index.IndexMain serve \
    --root "$dir/$auth.jwork" --authority "$auth" --result-authority "localhost:$port" \
    --principal-map "$dir/pki/ps-principals.tsv" --client-ca "$dir/pki/ps-ca.pem" \
    --cert "$dir/pki/$scert" --key "$dir/pki/${scert%.pem}.key" --bind "127.0.0.1:$port" \
    --ready-file "$dir/$auth.ready" --trust-system-clock "${extra[@]}" \
    </dev/null >>"$WORK/$auth-srv.log" 2>&1
  PORTS="$PORTS $port"
  wait_ready "$dir/$auth.ready" "$auth"
}

# run_coordinator <name> <dir> <connect-a> <connect-b> [extra...]: gates VERIFIED + pin
DIGESTS=""
run_coordinator() {
  local name="$1" dir="$2" ca="$3" cb="$4"; shift 4
  local out="$WORK/$name.out"
  timeout -s KILL 300000 "$IC" --owner workload \
    --authority-a "$AAUTH" --authority-b "$BAUTH" \
    --connect-a "$ca" --connect-b "$cb" \
    --ca "$dir/pki/ps-ca.pem" --cert "$dir/pki/ps-client.pem" --key "$dir/pki/ps-client.key" \
    "$@" >"$out" 2>&1 || { note "FAIL $name (exit $?)"; tail -3 "$out"; return 1; }
  local line digest
  line="$(grep -E '^VERIFIED index byte-exact' "$out" | tail -1)" || { note "FAIL $name (no VERIFIED)"; tail -3 "$out"; return 1; }
  digest="${line##*digest }"
  [ "$digest" = "$PIN_SMALL" ] || { note "FAIL $name digest $digest != pin"; return 1; }
  DIGESTS="$DIGESTS $digest"
  note "PASS $name digest $digest"
}

setup_dir() { # <dir>
  rm -rf "$1"; mkdir -p "$1"
  bash "$MKPKI" "$1/pki" >/dev/null 2>&1
}

# fresh_rust_pair <dir>: stop the rust-pair servers, wipe authority state,
# re-init and relaunch. Each demo group gets a clean pair: accumulated
# sessions plus abrupt client exits (the kill demos) leave residue under
# which new reader connections are intermittently dropped with a bare
# "connection lost" (see README). Fresh state keeps every group inside the
# proven-stable envelope (<=3 creations, no prior kills).
fresh_rust_pair() { # <dir>
  pkill -9 -f -- "--bind 127.0.0.1:17601" 2>/dev/null || true
  pkill -9 -f -- "--bind 127.0.0.1:17602" 2>/dev/null || true
  sleep 2
  rm -f "$1/index-a.sqlite"* "$1/index-b.sqlite"*; rm -rf "$1/index-a.obj" "$1/index-b.obj"
  "$IA" init-authority --state-db "$1/index-a.sqlite" --object-dir "$1/index-a.obj" \
    --authority index-a --principal-map "$1/pki/ps-principals.tsv" --trust-system-clock >/dev/null
  "$IA" init-authority --state-db "$1/index-b.sqlite" --object-dir "$1/index-b.obj" \
    --authority index-b --principal-map "$1/pki/ps-principals.tsv" --trust-system-clock >/dev/null
  AAUTH=index-a; BAUTH=index-b
  launch_rust "$1" index-a 17601 ps-server-a.pem ps-server-a.key
  launch_rust "$1" index-b 17602 ps-server-b.pem ps-server-b.key 127.0.0.1:17601
}

# --- direction: rust stages + rust merge ---
setup_dir "$WORK/rust"
fresh_rust_pair "$WORK/rust"
for rep in 1 2 3; do
  rm -f "$WORK/rust/ra$rep.sqlite"* "$WORK/rust/rb$rep.sqlite"*; rm -rf "$WORK/rust/stage-r$rep"
  run_coordinator "rust-r$rep" "$WORK/rust" 127.0.0.1:17601 127.0.0.1:17602 \
    --journal-a "$WORK/rust/ra$rep.sqlite" --journal-b "$WORK/rust/rb$rep.sqlite" \
    --creation-a "$rep" --creation-b "$rep" \
    --staging "$WORK/rust/stage-r$rep" --output "$WORK/rust/index-r$rep.bin" \
    --events "$WORK/rust/events-r$rep.tsv" --seed 6 --files 2 --words-per-file 128
done

# --- direction: java stages + rust merge ---
setup_dir "$WORK/mixed"
AAUTH=workload-j; BAUTH=workload-m
launch_java "$WORK/mixed" workload-j 17611 ps-server-a.pem
"$IA" init-authority --state-db "$WORK/mixed/workload-m.sqlite" --object-dir "$WORK/mixed/workload-m.obj" \
  --authority workload-m --principal-map "$WORK/mixed/pki/ps-principals.tsv" --trust-system-clock >/dev/null
launch_rust "$WORK/mixed" workload-m 17612 ps-server-b.pem ps-server-b.key 127.0.0.1:17611
for rep in 1 2 3; do
  rm -f "$WORK/mixed/ja$rep.sqlite"* "$WORK/mixed/jb$rep.sqlite"*; rm -rf "$WORK/mixed/stage-m$rep"
  # fresh java stage state per rep would need a server restart; creations isolate sessions instead
  run_coordinator "mixed-r$rep" "$WORK/mixed" 127.0.0.1:17611 127.0.0.1:17612 \
    --journal-a "$WORK/mixed/ja$rep.sqlite" --journal-b "$WORK/mixed/jb$rep.sqlite" \
    --creation-a "$rep" --creation-b "$rep" \
    --staging "$WORK/mixed/stage-m$rep" --output "$WORK/mixed/index-m$rep.bin" \
    --events "$WORK/mixed/events-m$rep.tsv" --seed 6 --files 2 --words-per-file 128
done

# --- direction: rust stages + java merge ---
setup_dir "$WORK/rev"
"$IA" init-authority --state-db "$WORK/rev/workload-a.sqlite" --object-dir "$WORK/rev/workload-a.obj" \
  --authority workload-a --principal-map "$WORK/rev/pki/ps-principals.tsv" --trust-system-clock >/dev/null
AAUTH=workload-a; BAUTH=workload-j
launch_rust "$WORK/rev" workload-a 17621 ps-server-a.pem ps-server-a.key
launch_java "$WORK/rev" workload-j 17622 ps-server-b.pem 127.0.0.1:17621
for rep in 1 2 3; do
  rm -f "$WORK/rev/va$rep.sqlite"* "$WORK/rev/vb$rep.sqlite"*; rm -rf "$WORK/rev/stage-v$rep"
  run_coordinator "rev-r$rep" "$WORK/rev" 127.0.0.1:17621 127.0.0.1:17622 \
    --journal-a "$WORK/rev/va$rep.sqlite" --journal-b "$WORK/rev/vb$rep.sqlite" \
    --creation-a "$rep" --creation-b "$rep" \
    --staging "$WORK/rev/stage-v$rep" --output "$WORK/rev/index-v$rep.bin" \
    --events "$WORK/rev/events-v$rep.tsv" --seed 6 --files 2 --words-per-file 128
done

# digest equality across all nine reps (pins already checked per rep)
[ "$(echo "$DIGESTS" | tr ' ' '\n' | sort -u | grep -c .)" = 1 ] || { note "FAIL digest mismatch: $DIGESTS"; exit 1; }
note "direction symmetry PASS (9 reps, one digest)"

# --- cancel demo on a fresh rust pair ---
fresh_rust_pair "$WORK/rust"
rm -f "$WORK/rust/ca.sqlite"* "$WORK/rust/cb.sqlite"*; rm -rf "$WORK/rust/stage-c" "$WORK/rust/events-c.tsv"
timeout -s KILL 300000 "$IC" --owner workload --authority-a index-a --authority-b index-b \
  --connect-a 127.0.0.1:17601 --connect-b 127.0.0.1:17602 \
  --journal-a "$WORK/rust/ca.sqlite" --journal-b "$WORK/rust/cb.sqlite" \
  --creation-a 1 --creation-b 1 \
  --ca "$WORK/rust/pki/ps-ca.pem" --cert "$WORK/rust/pki/ps-client.pem" --key "$WORK/rust/pki/ps-client.key" \
  --staging "$WORK/rust/stage-c" --output "$WORK/rust/index-c.bin" --events "$WORK/rust/events-c.tsv" \
  --seed 6 --files 2 --words-per-file 8192 --cancel-file 1 >"$WORK/cancel.out" 2>&1 \
  || { note "FAIL cancel (exit $?)"; tail -3 "$WORK/cancel.out"; exit 1; }
grep -q "^VERIFIED index byte-exact, digest $PIN_CANCEL\$" "$WORK/cancel.out" || { note "FAIL cancel digest"; exit 1; }
grep -q $'scope-cancelled\tfile 1 child scope' "$WORK/rust/events-c.tsv" || { note "FAIL cancel event"; exit 1; }
[ "$(grep "terminal state 7" "$WORK/rust/events-c.tsv" | cut -f3 | sort -u | wc -l)" = 4 ] || { note "FAIL cancel 4xCANCELLED"; exit 1; }
grep -q $'scope-counts\tcancelled child scope .*: declared=4 success=0 failure=0 cancelled=4 skipped=0' "$WORK/rust/events-c.tsv" || { note "FAIL cancel scope sums"; exit 1; }
grep -q $'scope-counts\tscope 0: declared=2 success=1 failure=1 cancelled=0 skipped=0' "$WORK/rust/events-c.tsv" || { note "FAIL cancel scope-0 sums"; exit 1; }
note "PASS cancel (4xCANCELLED, exclusion VERIFIED, sums match)"

# --- kill/resume demos, each on a fresh rust pair ---
# A 40s settle between kill and resume: the killed coordinator's
# connections need server-side reaping (past the 30s idle backstop),
# otherwise the resume plus the merge reader trips the 4-connections-
# per-principal ceiling and the reader connect is refused. Production
# crash supervision restarts slower than that anyway; see README (F3).
for stage in stage2 stage3; do
  fresh_rust_pair "$WORK/rust"
  rm -f "$WORK/rust/k.sqlite"* "$WORK/rust/kb.sqlite"*; rm -rf "$WORK/rust/stage-k$stage" "$WORK/rust/events-k$stage.tsv"
  timeout -s KILL 300000 "$IC" --owner workload --authority-a index-a --authority-b index-b \
    --connect-a 127.0.0.1:17601 --connect-b 127.0.0.1:17602 \
    --journal-a "$WORK/rust/k.sqlite" --journal-b "$WORK/rust/kb.sqlite" \
    --creation-a 1 --creation-b 1 \
    --ca "$WORK/rust/pki/ps-ca.pem" --cert "$WORK/rust/pki/ps-client.pem" --key "$WORK/rust/pki/ps-client.key" \
    --staging "$WORK/rust/stage-k$stage" --output "$WORK/rust/index-k$stage.bin" --events "$WORK/rust/events-k$stage.tsv" \
    --seed 6 --files 2 --words-per-file 128 --kill-at "$stage" >"$WORK/kill-$stage.out" 2>&1 \
    || { note "FAIL kill $stage (exit $?)"; exit 1; }
  grep -q "KILLED $stage" "$WORK/kill-$stage.out" || { note "FAIL kill $stage marker"; exit 1; }
  sleep 40
  timeout -s KILL 300000 "$IC" --owner workload --authority-a index-a --authority-b index-b \
    --connect-a 127.0.0.1:17601 --connect-b 127.0.0.1:17602 \
    --journal-a "$WORK/rust/k.sqlite" --journal-b "$WORK/rust/kb.sqlite" \
    --creation-a 1 --creation-b 1 \
    --ca "$WORK/rust/pki/ps-ca.pem" --cert "$WORK/rust/pki/ps-client.pem" --key "$WORK/rust/pki/ps-client.key" \
    --staging "$WORK/rust/stage-k$stage" --output "$WORK/rust/index-k$stage.bin" --events "$WORK/rust/events-k$stage.tsv" \
    --seed 6 --files 2 --words-per-file 128 --resume >"$WORK/resume-$stage.out" 2>&1 \
    || { note "FAIL resume $stage (exit $?)"; tail -3 "$WORK/resume-$stage.out"; exit 1; }
  grep -q "^VERIFIED index byte-exact, digest $PIN_SMALL\$" "$WORK/resume-$stage.out" || { note "FAIL resume $stage digest"; exit 1; }
  note "PASS kill/resume $stage"
done

teardown
trap - EXIT
note "GATE PASS: 9 direction reps + cancel + 2 kill/resume"
