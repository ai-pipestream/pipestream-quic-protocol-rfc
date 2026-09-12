#!/usr/bin/env bash
# C16g full matrix on the pinned (anchored-authority) binaries.
# Usage: run-full.sh WORK
#
# Sequential suites; suite scripts that take BENCHMARK.lock themselves are
# called WITHOUT holding it (a driver-held lock would deadlock them).
# Direct libexec rep calls, run-quick and run-cr13 (no self-lock) go through
# `flock` per invocation.
#
# Matrix: smoke (run-quick) -> ladder cells (empty/tiny/uneven/quick/
# standard x5 reps, large48 x3 reps; warmup + mirror-rotated arm order,
# per-rep DONE resume, digest gates) -> negative -> boundary -> stopped ->
# slow -> cr13 (rust + java) -> pipeline (own internal resume).
# C16f metrics are NOT re-run: c16f-standard-seed6 already ran on these
# binaries (hash-verified below); xlarge64 stays UNAVAILABLE (Java storage,
# 2nd Claude request open). Any failure aborts loudly with evidence kept;
# relaunch resumes (suite DONE-skips + rep DONE-skips).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms256m -Xmx2g}"

export PS_AUTH="$REPO/examples/durable-transform-workload/workload-authority/target/release/workload-authority"
export PS_COORD="$REPO/examples/durable-transform-workload/workload-coordinator/target/release/workload-coordinator"
export GRPC_WORKER="$REPO/examples/durable-transform-grpc/target/release/grpc-worker"
export GRPC_COORD="$REPO/examples/durable-transform-grpc/target/release/grpc-coordinator"
STD_JAR=/tmp/c4-standard/pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar
BIG_JAR=/tmp/c16a-large48/pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar
for b in "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD"; do
  [ -x "$b" ] || { echo "MISSING binary: $b"; exit 1; }
done
[ "$(sha256sum "$STD_JAR" | cut -c1-8)" = 61ab64a3 ] || { echo "std jar pin mismatch"; exit 1; }
[ "$(sha256sum "$BIG_JAR" | cut -c1-8)" = e1763b4a ] || { echo "large48 jar pin mismatch"; exit 1; }

mkdir -p "$WORK"
note() { echo "$1" | tee -a "$WORK/FULL-LOG.txt"; }
: > "$WORK/FULL-LOG.txt"
sha256sum "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD" > "$WORK/full.bin.sha256"
note "binaries pinned: $(sha256sum "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD" | cut -c1-8 | tr '\n' ' ')"

# Pin check: authority must be the anchored C16f build; the other three must
# be unchanged since the C16e measurement (pins from committed archives).
C16E_PIN="$REPO/benchmarks/durable-transform/results/pipeline-seed6.bin.sha256"
C16F_PIN="$REPO/benchmarks/durable-transform/results/c16f-standard-seed6/c16f.bin.sha256"
check_pin() { # <binary> <pinfile>
  local want got
  want=$(grep -F "  $1" "$2" | cut -d' ' -f1)
  got=$(sha256sum "$1" | cut -d' ' -f1)
  [ "$want" = "$got" ] || { note "PIN MISMATCH for $1"; exit 1; }
}
check_pin "$PS_AUTH" "$C16F_PIN"
for b in "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD"; do check_pin "$b" "$C16E_PIN"; done
note "pin check PASS (anchored authority + C16e trio)"

# suite <name> <script> [args...]: skip when DONE, else run (self-locking),
# mark DONE on success. Failure aborts with evidence in place.
suite() {
  local name="$1" script="$2"; shift 2
  if [ -f "$WORK/$name/DONE" ]; then
    note "FULL $name: SKIP (already done)"
    return 0
  fi
  note "FULL $name: start ($script $*)"
  # Via bash: suite scripts are committed either mode (run-negative.sh is
  # 644); direct exec would fail on those.
  bash "$script" "$WORK/$name" "$@" || { note "FULL $name: FAILED (see $WORK/$name)"; exit 1; }
  date -u +%FT%TZ > "$WORK/$name/DONE"
  note "FULL $name: DONE"
}
# suite_locked: same for scripts WITHOUT a self-lock (run-quick, run-cr13);
# the driver holds BENCHMARK.lock around them (never around self-locking
# children: that would deadlock).
suite_locked() {
  local name="$1" script="$2"; shift 2
  if [ -f "$WORK/$name/DONE" ]; then
    note "FULL $name: SKIP (already done)"
    return 0
  fi
  note "FULL $name: start ($script $*)"
  flock -w 3600 "$COORD_LOCK" bash "$script" "$WORK/$name" "$@" \
    || { note "FULL $name: FAILED (see $WORK/$name)"; exit 1; }
  date -u +%FT%TZ > "$WORK/$name/DONE"
  note "FULL $name: DONE"
}

# ---- smoke ----
suite_locked smoke "$HERE/run-quick.sh"

# ---- ladder cells ----
# spec: cell size reps pin jar
ladder_rep() { # cell size pin jar fund mode arm rep
  local cell="$1" size="$2" pin="$3" jar="$4" fund="$5" arm="$6" rep="$7"
  if [ -f "$rep/DONE" ]; then
    note "FULL $cell/$arm $(basename "$rep"): SKIP (already verified)"
    return 0
  fi
  rm -rf "$rep"
  local envs=()
  [ "$fund" = large ] && envs+=(PS_PHYS_DB_MIB=1024 PS_PHYS_WAL_MIB=256 JAVA_PHYS_DB_MIB=1024 JAVA_PHYS_WAL_MIB=256)
  [ "$size" = 0 ] && envs+=(ALLOW_VACUOUS=1)
  local rc=0 fin ev log
  log="$rep.stdout.log"
  if [ "$arm" = mixed ]; then
    env "${envs[@]}" JAVA_JAR="$jar" EXPECTED_SHA="$pin" \
      flock -w 3600 "$COORD_LOCK" "$HERE/run-mixed.sh" "$rep" "$SEED" "$size" c >"$log" 2>&1 || rc=$?
    fin="$rep/artifacts/mixed-final.bin"; ev="$rep/artifacts/mixed-events.tsv"
  elif [ "$arm" = ps ]; then
    env "${envs[@]}" \
      flock -w 3600 "$COORD_LOCK" "$HERE/libexec-run-ps.sh" "$rep" "$SEED" "$size" >"$log" 2>&1 || rc=$?
    fin="$rep/artifacts/ps-final.bin"; ev="$rep/artifacts/ps-events.tsv"
  else
    env "${envs[@]}" \
      flock -w 3600 "$COORD_LOCK" "$HERE/libexec-run-grpc.sh" "$rep" "$SEED" "$size" >"$log" 2>&1 || rc=$?
    fin="$rep/artifacts/grpc-final.bin"; ev="$rep/artifacts/grpc-events.tsv"
  fi
  [ "$rc" -eq 0 ] || { note "FULL $cell/$arm $(basename "$rep"): exit $rc"; return 1; }
  # SIZE=0 admits zero chunks, so first-usable-output is vacuous (C4 empty
  # was PARTIAL on this exact gate); final-verified + empty digest govern.
  if [ "$size" != 0 ]; then
    grep -q "first-usable-output" "$ev" || { note "FULL $cell/$arm: no first-usable"; return 1; }
  fi
  grep -q "final-verified" "$ev" || { note "FULL $cell/$arm: no final-verified"; return 1; }
  [ "$(sha256sum "$fin" | cut -d' ' -f1)" = "$pin" ] || { note "FULL $cell/$arm: digest mismatch"; return 1; }
  date -u +%FT%TZ > "$rep/DONE"
  note "FULL $cell/$arm $(basename "$rep"): PASS $(grep -o -m1 'done in [0-9]* ms' "$log")"
}
EMPTY_PIN=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
TINY_PIN=55d2e6e9cea9d2d08cbaf40c7c0dc5df3c1ed0fb64206ee1b3657cdb4cd3603f
UNEVEN_PIN=652837331fe96df99a99cb09ebe6898e87f1c0968047dd28aa49c619fdb8eb9d
QUICK_PIN=3cde15aae8059eb0d8cfbb62fa897bbf4df1e1dfd5e3de30a4784faa314ad192
STD_PIN=b3dd5e34376dba8b6042c99fea2ab6e821045305e7ed00ff9a0cecfe92b0160b
BIG_PIN=acc15911648496f7ef12c7291e31fcb07aff45a268e8fd8b293c616d6b58af9a
for spec in "empty 0 5 $EMPTY_PIN $STD_JAR std" "tiny 1000 5 $TINY_PIN $STD_JAR std" \
            "uneven 100000 5 $UNEVEN_PIN $STD_JAR std" "quick 200000 5 $QUICK_PIN $STD_JAR std" \
            "standard 8388608 5 $STD_PIN $STD_JAR std" "large48 50331648 3 $BIG_PIN $BIG_JAR large"; do
  # shellcheck disable=SC2086
  set -- $spec; cell="$1"; size="$2"; reps="$3"; pin="$4"; jar="$5"; fund="$6"
  cdir="$WORK/ladder-$cell"
  mkdir -p "$cdir"
  if [ -f "$cdir/DONE" ]; then
    note "FULL ladder-$cell: SKIP (already done)"
    continue
  fi
  cp "$jar" "$cdir/cell-jar-$(sha256sum "$jar" | cut -c1-8).jar"
  : > "$cdir/order.log"
  for arm in ps mixed grpc; do
    ladder_rep "$cell" "$size" "$pin" "$jar" "$fund" "$arm" "$cdir/warmup-$arm" || exit 1
    echo "warmup $cell/$arm" >> "$cdir/order.log"
  done
  for i in $(seq 1 "$reps"); do
    if ((i % 2 == 1)); then order="ps mixed grpc"; else order="grpc mixed ps"; fi
    for arm in $order; do
      ladder_rep "$cell" "$size" "$pin" "$jar" "$fund" "$arm" "$cdir/rep$i-$arm" || exit 1
      echo "rep$i $cell/$arm" >> "$cdir/order.log"
    done
  done
  date -u +%FT%TZ > "$cdir/DONE"
  note "FULL ladder-$cell: DONE ($reps reps x 3 arms + warmups)"
done

# ---- correctness suites (self-locking scripts) ----
suite negative "$HERE/run-negative.sh"
suite boundary "$HERE/run-faults-boundary.sh"
suite stopped "$HERE/run-stopped.sh"
suite slow "$HERE/run-slow.sh"
suite_locked cr13-rust "$HERE/run-cr13.sh" "$SEED"
export JAVA_JAR="$STD_JAR"
suite_locked cr13-java "$HERE/run-cr13.sh" "$SEED" --java

# ---- pipeline before/after (own internal resume) ----
suite pipeline "$HERE/run-pipeline.sh"

note "FULL MATRIX PASS (c16f metrics cited from c16f-standard-seed6 on these binaries; xlarge64 UNAVAILABLE)"
