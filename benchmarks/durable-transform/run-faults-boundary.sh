#!/usr/bin/env bash
# Boundary-armed faults F3/F4 in Kimi interface-v1 vocabulary.
# Usage: run-faults-boundary.sh WORK   (takes BENCHMARK.lock itself)
#
# F3 (worker-c, authority side, ps only): worker-c carries the FLAG-armed
#   --test-kill-after-output-installed 1 and aborts right after its first
#   OUTPUT_INSTALLED commit -- the nearest app-visible prior commit to the
#   scheduled PUBLICATION_COMMITTED, which lives in framework code past
#   execute()'s return and has no app-visible hook point (framed honestly
#   in CELL.txt). The coordinator fail-fasts on the dead worker; recovery
#   is restart worker-c on the same state dir + coordinator --resume on the
#   same journals (frozen operation identities, byte-verified staging
#   short-circuit). No false completion: oracle + pin check stay on.
# F4 (coordinator, RESULT_VERIFIED, ps + mixed): coordinator FLAG-armed
#   --test-kill-after-first-verified aborts at the first verified chunk
#   (one-shot via staging sentinel); recovery restarts all workers on
#   their state dirs + coordinator --resume.
# F3 on mixed (Java authority side) needs the neutral fixture adapter
#   (FixtureMain, interface-v1), not reachable from the production
#   launcher: recorded UNAVAILABLE, no run faked.
# Five repeats per reached boundary; recovery measured kill-wall (F3) or
# boundary-epoch (F4) to final-verified epoch. Quick config (seed 6,
# 200000 bytes); finals must equal the quick pin 3cde15aa....
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6; SIZE=200000; REPS=5
QUICK_PIN=3cde15aae8059eb0d8cfbb62fa897bbf4df1e1dfd5e3de30a4784faa314ad192
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock
JAR=/tmp/c4-quick/pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar
JAR_PIN=61ab64a31290

exec 9>"$COORD_LOCK"
flock -w 600 9 || { echo "could not take BENCHMARK.lock"; exit 1; }

export PS_AUTH="$REPO/examples/durable-transform-workload/workload-authority/target/release/workload-authority"
export PS_COORD="$REPO/examples/durable-transform-workload/workload-coordinator/target/release/workload-coordinator"
export JAVA_JAR="$JAR"
for b in "$PS_AUTH" "$PS_COORD"; do [ -x "$b" ] || { echo "MISSING binary: $b"; exit 1; }; done
[ "$(sha256sum "$JAR" | cut -c1-12)" = "$JAR_PIN" ] || { echo "jar pin mismatch"; exit 1; }
[ -f "$JAR" ] || { echo "MISSING Java jar"; exit 1; }
command -v java >/dev/null || { echo "MISSING java"; exit 1; }
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms256m -Xmx2g}"
sha256sum "$PS_AUTH" "$PS_COORD" "$JAR" > "$WORK.bin.sha256" 2>/dev/null || true
mkdir -p "$WORK"

note() { echo "$1" | tee -a "$WORK/BOUNDARY-LOG.txt"; }
ms_now() { date +%s%N | cut -c1-13; }
RESTARTED=""; SAMPLERS=""

# restart_rs_worker <rundir> <letter> <port> <logfile>: serve an existing
# Rust state dir again. Sets LAST_PID. Never call in command substitution.
restart_rs_worker() {
  local d="$WORK/$1" letter="$2" port="$3" log="$4"
  rm -f "$d/work/ps-$letter.ready"
  "$PS_AUTH" serve --state-db "$d/work/ps-$letter.sqlite" --object-dir "$d/work/ps-$letter.obj" \
    --authority "workload-$letter" --principal-map "$d/work/pki/ps-principals.tsv" \
    --trust-system-clock --bind "127.0.0.1:$port" --cert "$d/work/pki/ps-server-$letter.pem" \
    --key "$d/work/pki/ps-server-$letter.key" --client-ca "$d/work/pki/ps-ca.pem" \
    --result-authority "localhost:$port" --ready-file "$d/work/ps-$letter.ready" \
    > "$log" 2>&1 &
  LAST_PID=$!
  RESTARTED="$RESTARTED $LAST_PID"
  for _ in $(seq 1 200); do [ -f "$d/work/ps-$letter.ready" ] && break; sleep 0.1; done
  [ -f "$d/work/ps-$letter.ready" ] || { note "worker-$letter never restarted in $1"; return 1; }
}

# restart_java_worker <rundir> <letter> <port> <logfile>: serve the existing
# Java root again (no re-init: retained file policy cannot change on reopen).
restart_java_worker() {
  local d="$WORK/$1" letter="$2" port="$3" log="$4"
  rm -f "$d/work/ps-$letter.ready"
  java --enable-native-access=ALL-UNNAMED -cp "$JAR" ai.pipestream.quic.v2.V2Main serve \
    --root "$d/work/java-$letter.root" --authority "workload-$letter" --result-authority "localhost:$port" \
    --bind "127.0.0.1:$port" --cert "$d/work/pki/ps-server-$letter.pem" \
    --key "$d/work/pki/ps-server-$letter.key" --client-ca "$d/work/pki/ps-ca.pem" \
    --principal-map "$d/work/pki/ps-principals.tsv" --trust-system-clock \
    --ready-file "$d/work/ps-$letter.ready" \
    > "$log" 2>&1 &
  LAST_PID=$!
  RESTARTED="$RESTARTED $LAST_PID"
  for _ in $(seq 1 300); do [ -f "$d/work/ps-$letter.ready" ] && break; sleep 0.1; done
  [ -f "$d/work/ps-$letter.ready" ] || { note "java worker-$letter never restarted in $1"; return 1; }
}

cleanup_all() {
  [ -n "$RESTARTED" ] && kill $RESTARTED 2>/dev/null || true
  [ -n "$SAMPLERS" ] && kill $SAMPLERS 2>/dev/null || true
}
trap cleanup_all EXIT

# wait_worker_death <rep>: libexec TERMs workers on exit but does not wait
# for their graceful drain; a restart races the old process (payload root
# already owned / bind conflict). Poll until no serve holds this rep dir.
wait_worker_death() {
  local r="$1"
  for _ in $(seq 1 600); do
    pgrep -f "$r/work/ps-.\.sqlite" >/dev/null || return 0
    sleep 0.1
  done
  return 1
}

# resume_coord <rundir> <events> <output> <stdoutlog>: coordinator --resume
# on the same journals/staging, appending to the same events file.
resume_coord() {
  local d="$WORK/$1" ev="$2" out="$3" log="$4"
  "$PS_COORD" run --ca "$d/work/pki/ps-ca.pem" --cert "$d/work/pki/ps-client.pem" \
    --key "$d/work/pki/ps-client.key" --owner workload \
    --journal-a "$d/work/ps-j0.sqlite" --journal-b "$d/work/ps-j1.sqlite" --journal-c "$d/work/ps-j2.sqlite" \
    --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444 --connect-c 127.0.0.1:17445 \
    --seed "$SEED" --size "$SIZE" --staging "$d/work/ps-staging" \
    --output "$d/$out" --events "$d/$ev" \
    --resume >>"$log" 2>&1
}

check_final() { # check_final <rundir> <events> <finalbin>
  local d="$WORK/$1" ev="$2" fin="$3"
  grep -q "final-verified" "$d/$ev" || { note "$1: no final-verified after resume"; return 1; }
  local digest
  digest=$(sha256sum "$d/$fin" | cut -d' ' -f1)
  [ "$digest" = "$QUICK_PIN" ] || { note "$1: final digest $digest != pin"; return 1; }
  echo "$digest"
}

# The schedule executed (interface-v1 / fault-schedule schema v1 columns).
{
  echo -e "version\trun_id\tscenario_id\ttarget\tboundary\taction\tseed\tdeadline_ms"
  for i in $(seq 1 $REPS); do
    echo -e "1\tc16-f3-r$i\tF3\tworker-c\tOUTPUT_INSTALLED*\tkill\t6\t30000"
    echo -e "1\tc16-f3-r$i\tF3\tworker-c\tOUTPUT_INSTALLED*\trestart\t6\t60000"
    echo -e "1\tc16-f4ps-r$i\tF4\tcoordinator\tRESULT_VERIFIED\tkill\t6\t30000"
    echo -e "1\tc16-f4ps-r$i\tF4\tcoordinator\tRESULT_VERIFIED\trestart\t6\t60000"
    echo -e "1\tc16-f4mixed-r$i\tF4\tcoordinator\tRESULT_VERIFIED\tkill\t6\t30000"
    echo -e "1\tc16-f4mixed-r$i\tF4\tcoordinator\tRESULT_VERIFIED\trestart\t6\t60000"
  done
} > "$WORK/fault-schedule-c16.tsv"
note "schedule: $WORK/fault-schedule-c16.tsv (*F3 hook at OUTPUT_INSTALLED, one commit before scheduled PUBLICATION_COMMITTED; see header note)"
echo "F3 boundary note: scheduled PUBLICATION_COMMITTED lives in framework code past execute() return; hook fires at nearest app-visible prior commit OUTPUT_INSTALLED" > "$WORK/F3-BOUNDARY-NOTE.txt"

# ---- F3 x5 on ps ----
: > "$WORK/f3-RECOVERY.txt"
for i in $(seq 1 $REPS); do
  R="f3-ps-rep$i"; D="$WORK/$R"
  rc=0
  PS_KILL_C_AFTER_OUTPUT=1 bash "$HERE/libexec-run-ps.sh" "$D" "$SEED" "$SIZE" \
    >"$D.stdout.log" 2>&1 || rc=$?
  [ "$rc" -ne 0 ] || { note "F3 $R: armed run passed (hook did not fire?)"; exit 1; }
  # Boundary instant: poll for the authority abort line (post-commit).
  KILL_WALL=""
  for _ in $(seq 1 600); do
    if grep -q "test-kill-after-output-installed: aborting" "$D/work/ps-c.log" 2>/dev/null; then
      KILL_WALL=$(ms_now); break
    fi
    sleep 0.1
  done
  [ -n "$KILL_WALL" ] || { note "F3 $R: kill hook never fired"; exit 1; }
  wait_worker_death "$R" || { note "F3 $R: old workers never drained"; exit 1; }
  awk -F'\t' '$3==2 && $5=="succeeded" {found=1} END {exit !found}' "$D/artifacts/ps-events.tsv" \
    && { note "F3 $R: chunk 2 completed despite kill (false completion)"; exit 1; } || true
  note "F3 $R: worker-c aborted at OUTPUT_INSTALLED (wall $KILL_WALL), run failed as designed (rc=$rc)"
  echo "$R kill_wall_ms=$KILL_WALL" >> "$WORK/f3-RECOVERY.txt"
  # Restart all three workers on their existing state dirs (libexec
  # cleanup killed them when the coordinator exited), no kill flag.
  restart_rs_worker "$R" a 17443 "$D/ps-a-resume.log" || exit 1
  PA=$LAST_PID
  restart_rs_worker "$R" b 17444 "$D/ps-b-resume.log" || exit 1
  PB=$LAST_PID
  restart_rs_worker "$R" c 17445 "$D/ps-c-resume.log" || exit 1
  PC=$LAST_PID
  "$HERE/sample.sh" "$D/artifacts/ps-sample-resume.tsv" "$PA" "$PB" "$PC" &
  RSAMP=$!
  SAMPLERS="$SAMPLERS $RSAMP"
  # Resume the coordinator on the same journals/staging/events.
  resume_coord "$R" "artifacts/ps-events.tsv" "artifacts/ps-final.bin" "$D.stdout.log" \
    || { note "F3 $R: resume failed"; exit 1; }
  DIGEST=$(check_final "$R" "artifacts/ps-events.tsv" "artifacts/ps-final.bin") || exit 1
  FINAL_EPOCH=$(grep "final-verified" "$D/artifacts/ps-events.tsv" | tail -1 | cut -f2)
  echo "$R final_verified_epoch_ms=$FINAL_EPOCH final_digest=$DIGEST recovery_ms=$((FINAL_EPOCH - KILL_WALL))" >> "$WORK/f3-RECOVERY.txt"
  note "F3 $R PASS: resume byte-exact, recovery $((FINAL_EPOCH - KILL_WALL)) ms from boundary"
  kill "$PA" "$PB" "$PC" "$RSAMP" 2>/dev/null || true
  wait_worker_death "$R" || { note "$R: resumed workers never drained"; exit 1; }
done

# ---- F4 x5 on ps ----
: > "$WORK/f4ps-RECOVERY.txt"
for i in $(seq 1 $REPS); do
  R="f4-ps-rep$i"; D="$WORK/$R"
  rc=0
  PS_KILL_AFTER_FIRST_VERIFIED=1 bash "$HERE/libexec-run-ps.sh" "$D" "$SEED" "$SIZE" \
    >"$D.stdout.log" 2>&1 || rc=$?
  [ "$rc" -ne 0 ] || { note "F4 $R: coordinator survived the boundary"; exit 1; }
  [ -f "$D/work/ps-staging/kill-f4-fired" ] || { note "F4 $R: kill sentinel missing"; exit 1; }
  grep -q "test-kill-after-first-verified: aborting" "$D.stdout.log" \
    || { note "F4 $R: abort line missing"; exit 1; }
  KILL_EPOCH=$(grep -m1 "chunk-verified" "$D/artifacts/ps-events.tsv" | cut -f2)
  [ -n "$KILL_EPOCH" ] || { note "F4 $R: no verified row precedes death"; exit 1; }
  wait_worker_death "$R" || { note "F4 $R: old workers never drained"; exit 1; }
  note "F4 $R: coordinator dead (exit $rc) at RESULT_VERIFIED epoch $KILL_EPOCH"
  echo "$R kill_verified_epoch_ms=$KILL_EPOCH coord_exit=$rc" >> "$WORK/f4ps-RECOVERY.txt"
  restart_rs_worker "$R" a 17443 "$D/ps-a-resume.log" || exit 1
  PA=$LAST_PID
  restart_rs_worker "$R" b 17444 "$D/ps-b-resume.log" || exit 1
  PB=$LAST_PID
  restart_rs_worker "$R" c 17445 "$D/ps-c-resume.log" || exit 1
  PC=$LAST_PID
  "$HERE/sample.sh" "$D/artifacts/ps-sample-resume.tsv" "$PA" "$PB" "$PC" &
  RSAMP=$!
  SAMPLERS="$SAMPLERS $RSAMP"
  resume_coord "$R" "artifacts/ps-events.tsv" "artifacts/ps-final.bin" "$D.stdout.log" \
    || { note "F4 $R: resume failed"; exit 1; }
  DIGEST=$(check_final "$R" "artifacts/ps-events.tsv" "artifacts/ps-final.bin") || exit 1
  FINAL_EPOCH=$(grep "final-verified" "$D/artifacts/ps-events.tsv" | tail -1 | cut -f2)
  echo "$R final_verified_epoch_ms=$FINAL_EPOCH final_digest=$DIGEST recovery_ms=$((FINAL_EPOCH - KILL_EPOCH))" >> "$WORK/f4ps-RECOVERY.txt"
  note "F4 $R PASS: resume byte-exact, recovery $((FINAL_EPOCH - KILL_EPOCH)) ms from boundary"
  kill "$PA" "$PB" "$PC" "$RSAMP" 2>/dev/null || true
  wait_worker_death "$R" || { note "$R: resumed workers never drained"; exit 1; }
done

# ---- F4 x5 on mixed (coordinator side; Java worker c) ----
: > "$WORK/f4mixed-RECOVERY.txt"
for i in $(seq 1 $REPS); do
  R="f4-mixed-rep$i"; D="$WORK/$R"
  rc=0
  PS_KILL_AFTER_FIRST_VERIFIED=1 bash "$HERE/run-mixed.sh" "$D" "$SEED" "$SIZE" \
    >"$D.stdout.log" 2>&1 || rc=$?
  [ "$rc" -ne 0 ] || { note "F4 $R: coordinator survived the boundary"; exit 1; }
  [ -f "$D/work/ps-staging/kill-f4-fired" ] || { note "F4 $R: kill sentinel missing"; exit 1; }
  grep -q "test-kill-after-first-verified: aborting" "$D.stdout.log" \
    || { note "F4 $R: abort line missing"; exit 1; }
  KILL_EPOCH=$(grep -m1 "chunk-verified" "$D/artifacts/mixed-events.tsv" | cut -f2)
  [ -n "$KILL_EPOCH" ] || { note "F4 $R: no verified row precedes death"; exit 1; }
  wait_worker_death "$R" || { note "F4 $R: old workers never drained"; exit 1; }
  note "F4 $R: coordinator dead (exit $rc) at RESULT_VERIFIED epoch $KILL_EPOCH"
  echo "$R kill_verified_epoch_ms=$KILL_EPOCH coord_exit=$rc" >> "$WORK/f4mixed-RECOVERY.txt"
  restart_rs_worker "$R" a 17443 "$D/ps-a-resume.log" || exit 1
  PA=$LAST_PID
  restart_rs_worker "$R" b 17444 "$D/ps-b-resume.log" || exit 1
  PB=$LAST_PID
  restart_java_worker "$R" c 17445 "$D/ps-c-resume.log" || exit 1
  PC=$LAST_PID
  "$HERE/sample.sh" "$D/artifacts/mixed-sample-resume.tsv" "$PA" "$PB" "$PC" &
  RSAMP=$!
  SAMPLERS="$SAMPLERS $RSAMP"
  resume_coord "$R" "artifacts/mixed-events.tsv" "artifacts/mixed-final.bin" "$D.stdout.log" \
    || { note "F4 $R: resume failed"; exit 1; }
  DIGEST=$(check_final "$R" "artifacts/mixed-events.tsv" "artifacts/mixed-final.bin") || exit 1
  FINAL_EPOCH=$(grep "final-verified" "$D/artifacts/mixed-events.tsv" | tail -1 | cut -f2)
  echo "$R final_verified_epoch_ms=$FINAL_EPOCH final_digest=$DIGEST recovery_ms=$((FINAL_EPOCH - KILL_EPOCH))" >> "$WORK/f4mixed-RECOVERY.txt"
  note "F4 $R PASS: resume byte-exact, recovery $((FINAL_EPOCH - KILL_EPOCH)) ms from boundary"
  kill "$PA" "$PB" "$PC" "$RSAMP" 2>/dev/null || true
  wait_worker_death "$R" || { note "$R: resumed workers never drained"; exit 1; }
done

# ---- F3 on mixed: UNAVAILABLE (Java authority side) ----
cat > "$WORK/f3-mixed-UNAVAILABLE.txt" <<'EOF'
UNAVAILABLE: F3 targets worker-c at PUBLICATION_COMMITTED on the authority
side. Against the Java authority this needs the neutral fixture adapter
(FixtureMain, Kimi interface-v1), which is not reachable from the
production launcher (V2Main serve has no commit-time kill flag and must
not gain a TEST-ONLY one from this lane). No run faked.
EOF
note "F3 mixed: UNAVAILABLE (FixtureMain not reachable; see f3-mixed-UNAVAILABLE.txt)"

note "BOUNDARY SUITE PASS"