#!/usr/bin/env bash
# A1: client-boundary kill suite (ps + mixed arms, quick config).
# Usage: run-faults-client.sh WORK   (takes BENCHMARK.lock per run, not whole-suite)
#
# For each runnable interface-v1 client boundary, chunk ordinals 0 and 2 of
# the 4-chunk quick corpus (seed 6, 200000 bytes), 5 reps on ps and on mixed
# (Java worker c, 28c3369b jar): arm --test-kill-at BOUNDARY:N, assert the
# run dies at the boundary (boundary row present with the right ordinal, no
# lifecycle row for the killed ordinal after it; rare trailing appends from
# other sessions racing the abort are recorded), restart workers on the
# same state,
# --resume without flags, and assert byte-exact final against the quick pin
# with frozen operation identities (no new declared rows after the kill
# epoch; same journal files; post-kill re-admits are legal same-identity
# resends). Recovery = final-verified epoch - kill epoch.
# REFUSAL_RECEIVED runs also set --test-skip-declare N (leaves chunk N
# undeclared so admission meets NOT_READY "input needs a covering durable
# declaration receipt"); an undeclared chunk is structurally uncompletable
# under frozen identities, so those rows assert no-false-completion on
# resume (bounded: still retrying, never invented; rep1 to the retry budget
# for the named terminal reason) instead of byte-exact resume. Its positive
# twin (same config, no skip) must pass with zero refusal rows.
# RECEIPT_VALIDATED is UNAVAILABLE (no inter-commit seam: validation is
# pure, the commit is RECEIPT_JOURNALED).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$1"; SEED=6; SIZE=200000; REPS=5
QUICK_PIN=3cde15aae8059eb0d8cfbb62fa897bbf4df1e1dfd5e3de30a4784faa314ad192
COORD_LOCK=/work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock
JAR_SRC=/home/krickert/.rfc-tmp/jars/java-28c3369b-all.jar
JAR_PIN=28c3369bd95210ab

export PS_AUTH="$REPO/examples/durable-transform-workload/workload-authority/target/release/workload-authority"
export PS_COORD="$REPO/examples/durable-transform-workload/workload-coordinator/target/release/workload-coordinator"
export JAVA_JAR="$WORK/java-28c3369b-all.jar"
export JAVA_TOOL_OPTIONS="-Xms256m -Xmx2g -Djava.io.tmpdir=/home/krickert/.rfc-tmp"
for b in "$PS_AUTH" "$PS_COORD"; do [ -x "$b" ] || { echo "MISSING binary: $b" >&2; exit 1; }; done
command -v java >/dev/null || { echo "MISSING java" >&2; exit 1; }
mkdir -p "$WORK"
[ -f "$JAVA_JAR" ] || cp "$JAR_SRC" "$JAVA_JAR"
[ "$(sha256sum "$JAVA_JAR" | cut -c1-16)" = "$JAR_PIN" ] || { echo "jar pin mismatch" >&2; exit 1; }
sha256sum "$PS_AUTH" "$PS_COORD" "$JAVA_JAR" > "$WORK.bin.sha256" 2>/dev/null || true

note() { echo "$1" | tee -a "$WORK/CLIENT-LOG.txt"; }
[ -f "$WORK/CLIENT-LOG.txt" ] || : > "$WORK/CLIENT-LOG.txt"
if [ ! -f "$WORK/CELL.txt" ]; then
  { echo "cell: A1 client-boundary kills, quick (SIZE=200000, 4 chunks, seed 6)";
    echo "binaries: workload-authority C16g pin, workload-coordinator WITH --test-kill-at (re-pinned, see commit); jar 28c3369b copied+verified";
    echo "funding: defaults (quick corpus); stores: $WORK on $(df -T "$WORK" | awk 'NR==2{print $2}')";
    echo "occurrence rule: N = chunk ordinal (deterministic across workers), not global firing order; ordinals 0 and 2";
    echo "RECEIPT_VALIDATED UNAVAILABLE (see UNAVAILABLE file); all other 7 client boundaries runnable";
    echo "frozen identities = no new declared rows after kill epoch + same journals + pin (post-kill re-admits are legal same-identity resends; invented identities self-police via NOT_READY/CONFLICT)";
    echo "harness note 2026-09-13: timeout MUST wrap inside flock (flock ... timeout ... cmd); timeout-outside-flock orphans the run holding the lock (one ~20 min orphan found and killed). Leftover rep processes are SIGKILLed after every run.";
    echo "REFUSAL deviation: undeclared chunks are structurally uncompletable under frozen identities (any completion would invent a declaration), so those 12 rows (3 per arm x ordinal: 1 full-budget exemplar + 2 bounded) assert death-at-refusal + no-false-completion on resume instead of byte-exact resume; transient admission refusals are not producible at 4-chunk scale with available knobs (measured: 2 s work-delay probe, 0 refusal rows)"; } > "$WORK/CELL.txt"
fi

# restart_rs_worker <repdir> <letter> <port> <logfile>
restart_rs_worker() {
  local d="$1" letter="$2" port="$3" log="$4"
  rm -f "$d/work/ps-$letter.ready"
  "$PS_AUTH" serve --state-db "$d/work/ps-$letter.sqlite" --object-dir "$d/work/ps-$letter.obj" \
    --authority "workload-$letter" --principal-map "$d/work/pki/ps-principals.tsv" \
    --trust-system-clock --bind "127.0.0.1:$port" --cert "$d/work/pki/ps-server-$letter.pem" \
    --key "$d/work/pki/ps-server-$letter.key" --client-ca "$d/work/pki/ps-ca.pem" \
    --result-authority "localhost:$port" --ready-file "$d/work/ps-$letter.ready" \
    > "$log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 200); do [ -f "$d/work/ps-$letter.ready" ] && break; sleep 0.1; done
  [ -f "$d/work/ps-$letter.ready" ] || { note "worker-$letter never restarted"; kill $pid 2>/dev/null || true; return 1; }
  echo "$pid"
}

# restart_java_worker <repdir> <port> <logfile>  (worker c on mixed)
restart_java_worker() {
  local d="$1" port="$2" log="$3"
  rm -f "$d/work/ps-c.ready"
  java --enable-native-access=ALL-UNNAMED -cp "$JAVA_JAR" ai.pipestream.quic.v2.V2Main serve \
    --root "$d/work/java-c.root" --authority "workload-c" --result-authority "localhost:$port" \
    --bind "127.0.0.1:$port" --cert "$d/work/pki/ps-server-c.pem" \
    --key "$d/work/pki/ps-server-c.key" --client-ca "$d/work/pki/ps-ca.pem" \
    --principal-map "$d/work/pki/ps-principals.tsv" --trust-system-clock \
    --ready-file "$d/work/ps-c.ready" \
    > "$log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 300); do [ -f "$d/work/ps-c.ready" ] && break; sleep 0.1; done
  [ -f "$d/work/ps-c.ready" ] || { note "java worker-c never restarted"; kill $pid 2>/dev/null || true; return 1; }
  echo "$pid"
}

wait_death() { # repdir
  local d="$1"
  for _ in $(seq 1 600); do pgrep -f "$d/work/ps-.\.sqlite" >/dev/null || return 0; sleep 0.1; done
  return 1
}

# kill_leftovers <dir>: SIGKILL anything still referencing a finished run
# dir (hung-run orphans keep inherited lock fds open). First path char in
# brackets so the pattern never matches this shell itself.
kill_leftovers() {
  local d="$1"
  local pat="[${d:0:1}]${d:1}"
  pkill -9 -f "$pat" 2>/dev/null || true
  sleep 1
  if pgrep -f "$pat" >/dev/null; then
    note "$d: leftovers survive SIGKILL"
    return 1
  fi
}

# start_workers <repdir> <arm>: restart workers on same state dirs.
# Sets WPIDS (space-separated) and WEV/WOUT (events/final paths, relative).
start_workers() {
  local d="$1" arm="$2"
  WPIDS=""
  wait_death "$d" || { note "$d: old workers never drained"; return 1; }
  if [ "$arm" = ps ]; then
    WPIDS="$(restart_rs_worker "$d" a 17443 "$d/work/ps-a-resume.log")" || return 1
    WPIDS="$WPIDS $(restart_rs_worker "$d" b 17444 "$d/work/ps-b-resume.log")" || return 1
    WPIDS="$WPIDS $(restart_rs_worker "$d" c 17445 "$d/work/ps-c-resume.log")" || return 1
    WEV="artifacts/ps-events.tsv"; WOUT="artifacts/ps-final.bin"
  else
    WPIDS="$(restart_rs_worker "$d" a 17443 "$d/work/ps-a-resume.log")" || return 1
    WPIDS="$WPIDS $(restart_rs_worker "$d" b 17444 "$d/work/ps-b-resume.log")" || return 1
    WPIDS="$WPIDS $(restart_java_worker "$d" 17445 "$d/work/ps-c-resume.log")" || return 1
    WEV="artifacts/mixed-events.tsv"; WOUT="artifacts/mixed-final.bin"
  fi
}

stop_workers() { # repdir
  kill $WPIDS 2>/dev/null || true
  wait_death "$1" || {
    kill_leftovers "$1/work" || return 1
    wait_death "$1" || { note "$1: workers never drained"; return 1; }
  }
}

# resume_run <repdir> <arm>: resume coordinator WITHOUT kill flags; must
# complete byte-exact (transient-fault rows only).
resume_run() {
  local d="$1" arm="$2"
  start_workers "$d" "$arm" || return 1
  flock -w 3600 "$COORD_LOCK" timeout -s KILL 3600 \
    "$PS_COORD" run --ca "$d/work/pki/ps-ca.pem" --cert "$d/work/pki/ps-client.pem" \
    --key "$d/work/pki/ps-client.key" --owner workload \
    --journal-a "$d/work/ps-j0.sqlite" --journal-b "$d/work/ps-j1.sqlite" --journal-c "$d/work/ps-j2.sqlite" \
    --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444 --connect-c 127.0.0.1:17445 \
    --seed "$SEED" --size "$SIZE" --staging "$d/work/ps-staging" \
    --output "$d/$WOUT" --events "$d/$WEV" \
    --resume >>"$d.stdout.log" 2>&1 || { rc=$?; stop_workers "$d"; note "$d: resume failed rc=$rc"; return 1; }
  stop_workers "$d" || return 1
}

# resume_nofalse <repdir> <arm> <kill_epoch> <bound_secs|full>: resume a
# structural-fault kill (undeclared chunk). Must NEVER complete: no
# final-verified, no final file, retries still meeting NOT_READY (the
# declaration is never invented). Bounded runs assert the steady state at
# timeout; the "full" exemplar runs to the retry budget for the named
# terminal reason.
resume_nofalse() {
  local d="$1" arm="$2" k="$3" bound="$4"
  start_workers "$d" "$arm" || return 1
  local rc=0 tmo=1500
  [ "$bound" = full ] || tmo="$bound"
  flock -w 3600 "$COORD_LOCK" timeout -s KILL "$tmo" \
    "$PS_COORD" run --ca "$d/work/pki/ps-ca.pem" --cert "$d/work/pki/ps-client.pem" \
    --key "$d/work/pki/ps-client.key" --owner workload \
    --journal-a "$d/work/ps-j0.sqlite" --journal-b "$d/work/ps-j1.sqlite" --journal-c "$d/work/ps-j2.sqlite" \
    --connect-a 127.0.0.1:17443 --connect-b 127.0.0.1:17444 --connect-c 127.0.0.1:17445 \
    --seed "$SEED" --size "$SIZE" --staging "$d/work/ps-staging" \
    --output "$d/$WOUT" --events "$d/$WEV" \
    --resume >>"$d.stdout.log" 2>&1 || rc=$?
  stop_workers "$d"
  grep -q "final-verified" "$d/$WEV" && { note "$d: FALSE COMPLETION after structural kill"; return 1; }
  [ -f "$d/$WOUT" ] && { note "$d: final file exists after structural kill"; return 1; }
  awk -F'\t' -v k="$k" '$5=="admit-notready" && $2>k {found=1} END{exit !found}' "$d/$WEV" \
    || { note "$d: no post-kill NOT_READY retries (invented coverage?)"; return 1; }
  if [ "$bound" = full ]; then
    grep -q "admit-budget-out" "$d.stdout.log" "$d/$WEV" \
      || { note "$d: exemplar missing budget-out terminal reason (rc=$rc)"; return 1; }
    note "$d: exemplar terminal reason recorded (rc=$rc)"
  else
    [ "$rc" -eq 124 ] || { note "$d: bounded resume ended rc=$rc, expected timeout-while-retrying"; return 1; }
  fi
}

# check_frozen <repdir> <events> <kill_epoch>: no new declared rows after
# the kill on the same journals, plus pin (checked by caller). Post-kill
# re-admits are legal: the admit path always resends the deterministically
# recomputed frozen identity (no randomness or counters in identity
# construction), and invented identities are self-policing (undeclared
# meets NOT_READY, changed params meet CONFLICT), so a byte-exact resume
# with no new declares is the frozen-identity proof (C16 F4 precedent).
check_frozen() {
  local ev="$1/$2" k="$3"
  awk -F'\t' -v k="$k" '$5=="declared" && $2>k {print "new declare: "$0; bad=1} END{exit bad}' "$ev"
}

# armed_run <arm> <boundary> <ordinal> <repname> [skip]: one kill + resume.
armed_run() {
  local arm="$1" boundary="$2" ord="$3" rep="$4" skip="${5:-}"
  local d="$WORK/$rep"
  if [ -f "$d/DONE" ]; then note "A1 $arm $boundary:$ord $rep: SKIP"; return 0; fi
  rm -rf "$d"
  local evfin
  [ "$arm" = ps ] && evfin="ps-events.tsv" || evfin="mixed-events.tsv"
  local rc=0
  if [ "$arm" = ps ]; then
    env PS_KILL_AT="$boundary:$ord" ${skip:+PS_SKIP_DECLARE="$skip"} \
      flock -w 3600 "$COORD_LOCK" timeout -s KILL 3600 bash "$HERE/libexec-run-ps.sh" "$d" "$SEED" "$SIZE" \
      >"$d.stdout.log" 2>&1 || rc=$?
  else
    env PS_KILL_AT="$boundary:$ord" ${skip:+PS_SKIP_DECLARE="$skip"} \
      flock -w 3600 "$COORD_LOCK" timeout -s KILL 3600 bash "$HERE/run-mixed.sh" "$d" "$SEED" "$SIZE" c \
      >"$d.stdout.log" 2>&1 || rc=$?
  fi
  [ "$rc" -ne 0 ] || { note "A1 $arm $boundary:$ord $rep: armed run survived (hook did not fire)"; return 1; }
  kill_leftovers "$d/work" || return 1
  [ -f "$d/work/ps-staging/kill-armed" ] || { note "A1 $arm $boundary:$ord $rep: kill sentinel missing"; return 1; }
  grep -q "test-kill-at: $boundary firing for ordinal $ord" "$d.stdout.log" \
    || { note "A1 $arm $boundary:$ord $rep: abort line missing"; return 1; }
  # The kill is process-wide abort(); a concurrent session sharing the
  # events file can land one append in the microsecond window between the
  # boundary row and the abort. So: the boundary row must exist with the
  # right ordinal, and no lifecycle row for the killed (worker, ordinal)
  # may follow it; other trailing rows are recorded noise, not progress.
  local bnum kill_epoch w
  bnum=$(grep -n -P "\t$ord\tboundary\t$boundary reached, killing" "$d/artifacts/$evfin" | tail -1 | cut -d: -f1)
  [ -n "$bnum" ] || { note "A1 $arm $boundary:$ord $rep: no boundary row"; return 1; }
  kill_epoch=$(sed -n "${bnum}p" "$d/artifacts/$evfin" | cut -f2)
  w=$((ord % 3))
  tail -n +"$((bnum + 1))" "$d/artifacts/$evfin" | awk -F'\t' -v w="$w" -v o="$ord" \
    '$3==w && $4==o && $5!="boundary" {print "post-kill lifecycle: "$0; bad=1} END{exit bad}' \
    || { note "A1 $arm $boundary:$ord $rep: killed ordinal progressed after boundary"; return 1; }
  trailing=$(tail -n +"$((bnum + 1))" "$d/artifacts/$evfin" | wc -l)
  [ "$trailing" -eq 0 ] || note "A1 $arm $boundary:$ord $rep: $trailing trailing cross-session rows (abort race, recorded)"
  note "A1 $arm $boundary:$ord $rep: died at boundary epoch $kill_epoch (rc=$rc)"
  if [ "$boundary" = REFUSAL_RECEIVED ]; then
    # Structural fault (undeclared chunk): resume must NEVER complete (any
    # completion would invent a declaration). Bounded no-false-completion
    # proof; rep1 runs to the retry budget as the named-reason exemplar.
    if [ "$rep" = "a1-$arm-REFUSAL_RECEIVED-ord$ord-rep1" ]; then
      resume_nofalse "$d" "$arm" "$kill_epoch" full || return 1
    else
      resume_nofalse "$d" "$arm" "$kill_epoch" 150 || return 1
    fi
    echo "$rep kill_epoch=$kill_epoch nofalse_proven digest=none" >> "$WORK/A1-RECOVERY.txt"
    date -u +%FT%TZ > "$d/DONE"
    note "A1 $arm $boundary:$ord $rep: PASS (death at refusal + no false completion)"
    return 0
  fi
  resume_run "$d" "$arm" || return 1
  grep -q "final-verified" "$d/artifacts/$evfin" || { note "A1 $arm $boundary:$ord $rep: no final-verified after resume"; return 1; }
  local digest
  digest=$(sha256sum "$d/artifacts/${evfin%-events.tsv}-final.bin" | cut -d' ' -f1)
  [ "$digest" = "$QUICK_PIN" ] || { note "A1 $arm $boundary:$ord $rep: digest $digest != pin"; return 1; }
  check_frozen "$d" "artifacts/$evfin" "$kill_epoch" || { note "A1 $arm $boundary:$ord $rep: identities not frozen"; return 1; }
  local final_epoch
  final_epoch=$(grep "final-verified" "$d/artifacts/$evfin" | tail -1 | cut -f2)
  echo "$rep kill_epoch=$kill_epoch final_epoch=$final_epoch recovery_ms=$((final_epoch - kill_epoch)) digest=$digest" >> "$WORK/A1-RECOVERY.txt"
  date -u +%FT%TZ > "$d/DONE"
  note "A1 $arm $boundary:$ord $rep: PASS recovery $((final_epoch - kill_epoch)) ms"
}

# twin_run <arm> <repname>: normal quick run (REFUSAL positive twin): must
# pass with zero refusal rows and pin digest.
twin_run() {
  local arm="$1" rep="$2"
  local d="$WORK/$rep"
  if [ -f "$d/DONE" ]; then note "A1 $arm twin $rep: SKIP"; return 0; fi
  rm -rf "$d"
  local evfin
  if [ "$arm" = ps ]; then
    evfin="ps-events.tsv"
    flock -w 3600 "$COORD_LOCK" timeout -s KILL 3600 bash "$HERE/libexec-run-ps.sh" "$d" "$SEED" "$SIZE" \
      >"$d.stdout.log" 2>&1 || { note "A1 $arm twin $rep: run failed"; return 1; }
  else
    evfin="mixed-events.tsv"
    flock -w 3600 "$COORD_LOCK" timeout -s KILL 3600 bash "$HERE/run-mixed.sh" "$d" "$SEED" "$SIZE" c \
      >"$d.stdout.log" 2>&1 || { note "A1 $arm twin $rep: run failed"; return 1; }
  fi
  kill_leftovers "$d/work" || return 1
  if grep -q -P "\tadmit-refused\t|\tadmit-notready\t|\tadmit-ceiling\t" "$d/artifacts/$evfin"; then
    note "A1 $arm twin $rep: refusal rows in positive twin"; return 1
  fi
  local digest
  digest=$(sha256sum "$d/artifacts/${evfin%-events.tsv}-final.bin" | cut -d' ' -f1)
  [ "$digest" = "$QUICK_PIN" ] || { note "A1 $arm twin $rep: digest mismatch"; return 1; }
  date -u +%FT%TZ > "$d/DONE"
  note "A1 $arm twin $rep: PASS (no refusals, pin ok)"
}

[ -f "$WORK/A1-RECOVERY.txt" ] || : > "$WORK/A1-RECOVERY.txt"
for boundary in INTENT_JOURNALED REQUEST_SENT RECEIPT_JOURNALED OBSERVATION_JOURNALED RESULT_VERIFIED RESULT_INSTALLED REFUSAL_RECEIVED; do
  for ord in 0 2; do
    for arm in ps mixed; do
      # Deterministic kills run 3 reps (rep1 = full-budget exemplar for the
      # refusal rows); the kill either fires or it does not.
      nreps=$REPS
      [ "$boundary" = REFUSAL_RECEIVED ] && nreps=3
      for i in $(seq 1 $nreps); do
        if [ "$boundary" = REFUSAL_RECEIVED ]; then
          armed_run "$arm" "$boundary" "$ord" "a1-$arm-$boundary-ord$ord-rep$i" "$ord" || exit 1
        else
          armed_run "$arm" "$boundary" "$ord" "a1-$arm-$boundary-ord$ord-rep$i" || exit 1
        fi
      done
    done
  done
done
for arm in ps mixed; do
  for i in $(seq 1 $REPS); do
    twin_run "$arm" "a1-$arm-twin-rep$i" || exit 1
  done
done
cat > "$WORK/RECEIPT_VALIDATED-UNAVAILABLE.txt" <<'EOF'
UNAVAILABLE: --test-kill-at RECEIPT_VALIDATED cannot fire. In this client's
stack receipt validation is a pure in-memory shape check inside
Context::receipt with no journal commit, so there is no inter-commit seam
to kill at; the durable boundary is RECEIPT_JOURNALED (covered). Reaching
it would require a hook inside the shared pipestream-quinn tree, which the
C17 brief restricts to the three owned directories. No run faked.
EOF
note "A1 CLIENT SUITE PASS (120 kill+resume-complete, 12 kill+nofalse, 10 twins, 1 UNAVAILABLE)"
