#!/usr/bin/env bash
# Slow-worker arm, both arms, five repeats, same inputs/placement.
# Usage: run-slow.sh WORK   (takes BENCHMARK.lock itself)
# Worker-c runs at a tenth of the others' pace: a positive twin measures
# the median per-chunk admitted->verified latency, the slow delay is set to
# 10x that (floored at 1000 ms for visibility, capped far below the 60 s
# execution deadline), applied to worker-c only via FLAG. Per rep: show
# first-usable-output and total completion, that worker-c's chunk is
# processed exactly once (no attempt-2 / no duplicate verified row), and
# how much of the injected stall each arm hides (exposed = slow wall -
# positive wall). Quick config (seed 6, 200000, 4 chunks; worker-c owns
# ordinal 2).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6; SIZE=200000; REPS=5; SLOW_ORD=2
QUICK_PIN=3cde15aae8059eb0d8cfbb62fa897bbf4df1e1dfd5e3de30a4784faa314ad192
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock

exec 9>"$COORD_LOCK"
flock -w 600 9 || { echo "could not take BENCHMARK.lock"; exit 1; }

export PS_AUTH="$REPO/examples/durable-transform-workload/workload-authority/target/release/workload-authority"
export PS_COORD="$REPO/examples/durable-transform-workload/workload-coordinator/target/release/workload-coordinator"
export GRPC_WORKER="$REPO/examples/durable-transform-grpc/target/release/grpc-worker"
export GRPC_COORD="$REPO/examples/durable-transform-grpc/target/release/grpc-coordinator"
for b in "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD"; do
  [ -x "$b" ] || { echo "MISSING binary: $b"; exit 1; }
done
sha256sum "$PS_AUTH" "$PS_COORD" "$GRPC_WORKER" "$GRPC_COORD" > "$WORK.bin.sha256" 2>/dev/null || true
mkdir -p "$WORK"
note() { echo "$1" | tee -a "$WORK/SLOW-LOG.txt"; }
wall_of() { grep -o -m1 "done in [0-9]* ms" "$1" | grep -o "[0-9]*"; }

# median_latency <events>: median admitted->chunk-verified ms over chunks.
median_latency() {
  awk -F'\t' '$5=="admitted"{a[$4]=$1} $5=="chunk-verified"&&($4 in a){print $1-a[$4]}' "$1" \
    | sort -n | awk '{v[NR]=$1} END{if(NR==0)exit 1; print v[int((NR+1)/2)]}'
}

for arm in ps grpc; do
  if [ "$arm" = ps ]; then
    RUNNER="$HERE/libexec-run-ps.sh"; EVN=ps-events.tsv; FIN=ps-final.bin; DENV=PS_WORK_DELAY_C_MS
  else
    RUNNER="$HERE/libexec-run-grpc.sh"; EVN=grpc-events.tsv; FIN=grpc-final.bin; DENV=GRPC_WORK_DELAY_C_MS
  fi
  # Positive twin: pin + pace calibration.
  bash "$RUNNER" "$WORK/$arm-pos" "$SEED" "$SIZE" >"$WORK/$arm-pos.stdout.log" 2>&1
  [ "$(sha256sum "$WORK/$arm-pos/artifacts/$FIN" | cut -d' ' -f1)" = "$QUICK_PIN" ] \
    || { note "SLOW $arm: positive digest mismatch"; exit 1; }
  POSWALL=$(wall_of "$WORK/$arm-pos.stdout.log")
  MED=$(median_latency "$WORK/$arm-pos/artifacts/$EVN") || { note "SLOW $arm: no latencies"; exit 1; }
  DELAY=$((MED * 10)); [ "$DELAY" -lt 1000 ] && DELAY=1000
  # Stall discipline: delay stays >=12x below the 60 s execution deadline.
  [ "$DELAY" -le 5000 ] || { note "SLOW $arm: calibrated delay $DELAY ms too big (median $MED)"; exit 1; }
  note "SLOW $arm positive: PASS pin holds, wall ${POSWALL}ms, median chunk ${MED}ms -> slow delay ${DELAY}ms on worker-c"
  echo "positive_wall_ms=$POSWALL median_chunk_ms=$MED slow_delay_ms=$DELAY" > "$WORK/$arm-calibration.txt"
  : > "$WORK/$arm-slow-walls.txt"
  for i in $(seq 1 $REPS); do
    D="$WORK/$arm-slow-rep$i"
    rc=0
    env "$DENV=$DELAY" bash "$RUNNER" "$D" "$SEED" "$SIZE" >"$D.stdout.log" 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || { note "SLOW $arm rep$i: run failed (exit $rc)"; exit 1; }
    EV="$D/artifacts/$EVN"
    grep -q "first-usable-output" "$EV" || { note "SLOW $arm rep$i: no first-usable"; exit 1; }
    grep -q "final-verified" "$EV" || { note "SLOW $arm rep$i: no final-verified"; exit 1; }
    grep -q -i -m1 "timeout" "$EV" "$D.stdout.log" \
      && { note "SLOW $arm rep$i: unexpected timeout"; exit 1; } || true
    # Slow chunk processed exactly once (no double-processing, no retry).
    NVER=$(awk -F'\t' -v o="$SLOW_ORD" '$4==o && $5=="chunk-verified"' "$EV" | wc -l)
    [ "$NVER" = 1 ] || { note "SLOW $arm rep$i: slow chunk verified $NVER times"; exit 1; }
    if [ "$arm" = ps ]; then
      grep -q "attempt 2" "$EV" && { note "SLOW $arm rep$i: retry observed"; exit 1; } || true
    fi
    # Stall window differs by arm (honest asymmetry): ps admits async so the
    # authority-side stall lands between admitted and verified; grpc submits
    # synchronously so the worker-side stall lands before admitted.
    if [ "$arm" = ps ]; then
      SLAT=$(awk -F'\t' -v o="$SLOW_ORD" '$5=="admitted"&&$4==o{a=$1} $5=="chunk-verified"&&$4==o{print $1-a}' "$EV")
    else
      SLAT=$(awk -F'\t' -v o="$SLOW_ORD" '$5=="admitted"&&$4!=o{if(m==""){m=$1} if($1<m)m=$1} $5=="admitted"&&$4==o{print $1-m}' "$EV")
    fi
    # Tolerance 50 ms (stated): rel-ms timestamps truncate and the three
    # worker tasks phase against each other; both effects are << delay.
    [ "${SLAT:-0}" -ge "$((DELAY - 50))" ] || { note "SLOW $arm rep$i: slow chunk ${SLAT}ms < delay ${DELAY}ms"; exit 1; }
    [ "$(sha256sum "$D/artifacts/$FIN" | cut -d' ' -f1)" = "$QUICK_PIN" ] \
      || { note "SLOW $arm rep$i: digest mismatch"; exit 1; }
    W=$(wall_of "$D.stdout.log")
    echo "$W" >> "$WORK/$arm-slow-walls.txt"
    EXPOSED=$((W - POSWALL)); HIDDEN=$((DELAY - EXPOSED))
    echo "rep$i wall_ms=$W exposed_ms=$EXPOSED hidden_ms=$HIDDEN slow_chunk_ms=$SLAT" >> "$WORK/$arm-calibration.txt"
    note "SLOW $arm rep$i: complete pin-exact, slow chunk ${SLAT}ms (delay ${DELAY}ms), exposed ${EXPOSED}ms hidden ${HIDDEN}ms"
  done
done
note "SLOW SUITE PASS"
