#!/usr/bin/env bash
# Full comparative run: standard corpus, alternating arms, repeats, plus
# crash-recovery demonstrations. Records raw artifacts; analysis is separate.
# Usage: run-full.sh WORK [REPEATS]
# Heavy builds/measurement take BENCHMARK.lock; native rebuilds NATIVE-BUILD.lock.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$1"
REPEATS="${2:-5}"
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock

exec 9>"$COORD_LOCK"
flock -w 600 9 || { echo "could not take BENCHMARK.lock"; exit 1; }

SEED=6
SIZE=8388608  # standard dataset

run_arm() { # run_arm <ps|grpc> <repdir>
  local arm="$1" rep="$2"
  if [ "$arm" = ps ]; then
    PS_AUTH="$REPO_BIN/ps-auth" PS_COORD="$REPO_BIN/ps-coord" \
      "$HERE/libexec-run-ps.sh" "$rep" "$SEED" "$SIZE"
  else
    GRPC_WORKER="$REPO_BIN/grpc-worker" GRPC_COORD="$REPO_BIN/grpc-coord" \
      "$HERE/libexec-run-grpc.sh" "$rep" "$SEED" "$SIZE"
  fi
}

export REPO_BIN="$REPO_BIN"
# Alternating order A/B/B/A per repeat pair to cancel drift.
i=0
while [ "$i" -lt "$REPEATS" ]; do
  run_arm ps "$WORK/rep-$i-ps"
  run_arm grpc "$WORK/rep-$i-grpc"
  i=$((i + 1))
done

# Crash demonstrations (each must still end byte-exact via resume):
# 1. worker kill after admission, 2. coordinator kill mid-run then --resume.
"$HERE/libexec-faults.sh" "$WORK/faults" "$SEED" "$SIZE"

echo "FULL PASS: all repeats + fault demonstrations byte-exact (see $WORK)"
