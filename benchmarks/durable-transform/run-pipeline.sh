#!/usr/bin/env bash
# C16e before/after: serial vs pipelined coordinators on standard (8 MiB)
# and large48 (48 MiB), all three arms, five repeats, alternating order,
# warmup separate. Usage: run-pipeline.sh WORK   (takes BENCHMARK.lock)
# Serial (before): PS_SERIAL=1 / GRPC_SERIAL=1. Pipelined (after): default
# flags (pending-limit 16). Same pinned binaries both modes; per-run digest
# pin; per-chunk phase timings summarized to PIPELINE-SUMMARY.txt.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6; REPS=5
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock

exec 9>"$COORD_LOCK"
flock -w 3600 9 || { echo "could not take BENCHMARK.lock"; exit 1; }

export PS_AUTH="$REPO/examples/durable-transform-workload/workload-authority/target/release/workload-authority"
export PS_COORD="$REPO/examples/durable-transform-workload/workload-coordinator/target/release/workload-coordinator"
export GRPC_WORKER="$REPO/examples/durable-transform-grpc/target/release/grpc-worker"
export GRPC_COORD="$REPO/examples/durable-transform-grpc/target/release/grpc-coordinator"
for b in "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD"; do
  [ -x "$b" ] || { echo "MISSING binary: $b"; exit 1; }
done
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms256m -Xmx2g}"
mkdir -p "$WORK"
note() { echo "$1" | tee -a "$WORK/PIPELINE-LOG.txt"; }
sha256sum "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD" > "$WORK.bin.sha256" 2>/dev/null || true

# cell_run <cell> <size> <pin> <jar> <funding> <mode> <arm> <repdir>
# Resumable: a verified rep leaves DONE; relaunch skips it (failed runs
# stay visible and are retried).
cell_run() {
  local cell="$1" size="$2" pin="$3" jar="$4" fund="$5" mode="$6" arm="$7" rep="$8"
  if [ -f "$rep/DONE" ]; then
    note "PIPELINE $cell/$mode/$arm $(basename "$rep"): SKIP (already verified)"
    return 0
  fi
  # Fresh state for every attempt; a failed rep is retried clean.
  rm -rf "$rep"
  local envs=()
  [ "$mode" = serial ] && envs+=(PS_SERIAL=1 GRPC_SERIAL=1)
  if [ "$fund" = large ]; then
    envs+=(PS_PHYS_DB_MIB=1024 PS_PHYS_WAL_MIB=256 JAVA_PHYS_DB_MIB=1024 JAVA_PHYS_WAL_MIB=256)
  fi
  local rc=0 fin ev
  mkdir -p "$(dirname "$rep")"
  if [ "$arm" = mixed ]; then
    env "${envs[@]}" JAVA_JAR="$jar" EXPECTED_SHA="$pin" \
      bash "$HERE/run-mixed.sh" "$rep" "$SEED" "$size" >"$rep.stdout.log" 2>&1 || rc=$?
    fin="$rep/artifacts/mixed-final.bin"; ev="$rep/artifacts/mixed-events.tsv"
  elif [ "$arm" = ps ]; then
    env "${envs[@]}" bash "$HERE/libexec-run-ps.sh" "$rep" "$SEED" "$size" >"$rep.stdout.log" 2>&1 || rc=$?
    fin="$rep/artifacts/ps-final.bin"; ev="$rep/artifacts/ps-events.tsv"
  else
    env "${envs[@]}" bash "$HERE/libexec-run-grpc.sh" "$rep" "$SEED" "$size" >"$rep.stdout.log" 2>&1 || rc=$?
    fin="$rep/artifacts/grpc-final.bin"; ev="$rep/artifacts/grpc-events.tsv"
  fi
  [ "$rc" -eq 0 ] || { note "PIPELINE $cell/$mode/$arm $(basename "$rep"): exit $rc"; return 1; }
  grep -q "first-usable-output" "$ev" || { note "PIPELINE $cell/$mode/$arm: no first-usable"; return 1; }
  grep -q "final-verified" "$ev" || { note "PIPELINE $cell/$mode/$arm: no final-verified"; return 1; }
  [ "$(sha256sum "$fin" | cut -d' ' -f1)" = "$pin" ] || { note "PIPELINE $cell/$mode/$arm: digest mismatch"; return 1; }
  note "PIPELINE $cell/$mode/$arm $(basename "$rep"): PASS $(grep -o -m1 'done in [0-9]* ms' "$rep.stdout.log")"
  date -u +%FT%TZ > "$rep/DONE"
}

# Resolve per-cell jars (copied per cell dir, hash-verified, never rebuilt).
STD_JAR=/tmp/c4-standard/pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar
BIG_JAR=/tmp/c16a-large48/pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar
[ "$(sha256sum "$STD_JAR" | cut -c1-8)" = 61ab64a3 ] || { echo "std jar pin mismatch"; exit 1; }
[ "$(sha256sum "$BIG_JAR" | cut -c1-8)" = e1763b4a ] || { echo "large48 jar pin mismatch"; exit 1; }
STD_PIN=b3dd5e34376dba8b6042c99fea2ab6e821045305e7ed00ff9a0cecfe92b0160b
BIG_PIN=acc15911648496f7ef12c7291e31fcb07aff45a268e8fd8b293c616d6b58af9a
cp "$STD_JAR" "$WORK/standard-jar-61ab64a3.jar"
cp "$BIG_JAR" "$WORK/large48-jar-e1763b4a.jar"

: > "$WORK/order.log"
# mode order: before (serial) then after (pipelined), per cell.
for spec in "standard 8388608 $STD_PIN $WORK/standard-jar-61ab64a3.jar std" \
            "large48 50331648 $BIG_PIN $WORK/large48-jar-e1763b4a.jar large"; do
  # shellcheck disable=SC2086
  set -- $spec; cell="$1"; size="$2"; pin="$3"; jar="$4"; fund="$5"
  for mode in serial pipe; do
    # Warmups, one per arm, excluded from the summary.
    for arm in ps grpc mixed; do
      WU="$WORK/$cell-$mode/warmup-$arm"
      cell_run "$cell" "$size" "$pin" "$jar" "$fund" "$mode" "$arm" "$WU" \
        || { note "PIPELINE warmup failed: $cell/$mode/$arm"; exit 1; }
      echo "warmup $cell/$mode/$arm" >> "$WORK/order.log"
    done
    # Measured reps, mirror-rotated arm order per rep.
    for i in $(seq 1 $REPS); do
      if ((i % 2 == 1)); then order="ps grpc mixed"; else order="mixed grpc ps"; fi
      for arm in $order; do
        REP="$WORK/$cell-$mode/rep$i-$arm"
        cell_run "$cell" "$size" "$pin" "$jar" "$fund" "$mode" "$arm" "$REP" || exit 1
        echo "rep$i $cell/$mode/$arm" >> "$WORK/order.log"
      done
    done
  done
done

# Per-chunk phase p50/p95/p99 + wall medians from the measured reps.
python3 - "$WORK" <<'EOF' | tee "$WORK/PIPELINE-SUMMARY.txt"
import glob, os, re, statistics, sys
work = sys.argv[1]
def pct(v, p):
    if not v: return None
    s = sorted(v); k = (len(s) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] if f == c else s[f] + (s[c] - s[f]) * (k - f)
def row(s):
    for pat in (r'admit=(\d+)ms', r'watch=(\d+)ms', r'fetch=(\d+)ms'):
        m = re.search(pat, s)
        if m: return pat[0], int(m.group(1))
    return None, None
print("cell/mode/arm phase n p50_ms p95_ms p99_ms")
for cell in ("standard", "large48"):
    for mode in ("serial", "pipe"):
        for arm in ("ps", "grpc", "mixed"):
            ev = "ps-events.tsv" if arm == "ps" else ("grpc-events.tsv" if arm == "grpc" else "mixed-events.tsv")
            adm, wat, fet, span, walls = [], [], [], [], []
            for rep in sorted(glob.glob(os.path.join(work, f"{cell}-{mode}", "rep*-" + arm))):
                adm_t, ver_t = {}, {}
                for line in open(os.path.join(rep, "artifacts", ev), errors="replace"):
                    c = line.rstrip("\n").split("\t")
                    if len(c) < 6: continue
                    rel, lab, det, o = int(c[0]), c[4], c[5], c[3]
                    k, v = row(det)
                    if lab == "admitted":
                        adm_t[o] = rel
                        if k == "a": adm.append(v)
                    elif lab == "succeeded" and k == "w": wat.append(v)
                    elif lab == "chunk-verified":
                        if k == "f": fet.append(v)
                        if o in adm_t: span.append(rel - adm_t[o])
                m = re.search(r"done in (\d+) ms", open(rep + ".stdout.log", errors="replace").read())
                if m: walls.append(int(m.group(1)))
            for name, v in (("admit", adm), ("watch", wat), ("fetch", fet), ("span", span)):
                if not v: continue
                print(f"{cell}/{mode}/{arm} {name} n={len(v)} p50={pct(v,50):.0f} p95={pct(v,95):.0f} p99={pct(v,99):.0f}")
            if walls:
                print(f"{cell}/{mode}/{arm} wall n={len(walls)} median={statistics.median(walls):.0f} min={min(walls)} max={max(walls)}")
EOF
note "PIPELINE SUITE PASS (see PIPELINE-SUMMARY.txt)"
