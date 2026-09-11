#!/usr/bin/env bash
# Background resource sampler: threads/RSS/HWM/CPU/IO per PID into TSV,
# plus jstat -gc per tick for one Java PID when provided.
# Usage: sample.sh OUTFILE PID...
# Env: SAMPLE_JAVA_PID (JVM pid, must be among PID... when set),
#      SAMPLE_GC_OUT (gc TSV path; required with SAMPLE_JAVA_PID).
# A missed jstat row while the JVM lives is recorded as a JSTAT_GAP row;
# runners fail the run on any JSTAT_GAP (a gap fails the sample, never
# zero-filled). Ticks stretch past 0.2 s rather than skip jstat.
set -euo pipefail
OUT="$1"; shift
echo -e "t_ms\tpid\tcomm\tthreads\trss_kb\thwm_kb\tvsz_kb\tfds\tutime\tstime\trchar\twchar\tread_bytes\twrite_bytes" > "$OUT"
JAVA_PID="${SAMPLE_JAVA_PID:-}"
GC_OUT="${SAMPLE_GC_OUT:-}"
if [ -n "$JAVA_PID" ]; then
  [ -n "$GC_OUT" ] || { echo "SAMPLE_JAVA_PID needs SAMPLE_GC_OUT"; exit 1; }
  command -v jstat >/dev/null || { echo "MISSING jstat on PATH"; exit 1; }
  echo -e "t_ms\tpid\tS0C\tS1C\tS0U\tS1U\tEC\tEU\tOC\tOU\tMC\tMU\tCCSC\tCCSU\tYGC\tYGCT\tFGC\tFGCT\tGCT" > "$GC_OUT"
fi
ms_now() { date +%s%N | cut -c1-13; }
START=$(ms_now)
while true; do
  NOW=$(ms_now)
  for pid in "$@"; do
    if [ -d "/proc/$pid" ]; then
      COMM=$(tr '\0' ' ' < "/proc/$pid/comm" 2>/dev/null || echo "?")
      THREADS=$(awk '/^Threads:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
      RSS=$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
      HWM=$(awk '/^VmHWM:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
      VSZ=$(awk '/^VmSize:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
      FDS=$(ls "/proc/$pid/fd" 2>/dev/null | wc -l || true)
      ST=$(cat "/proc/$pid/stat" 2>/dev/null || echo "")
      ST=${ST##*) }
      # Never `set --` here: it would clobber "$@" (the PID list) and the
      # next tick would sample stat fields as PIDs.
      STAT_FIELDS=()
      read -ra STAT_FIELDS <<< "$ST" || true
      UTIME=${STAT_FIELDS[11]:-0}; STIME=${STAT_FIELDS[12]:-0}
      IO=$(awk -F': ' '/^(rchar|wchar|read_bytes|write_bytes)/{printf "%s%s", sep, $2; sep="\t"}' "/proc/$pid/io" 2>/dev/null || echo "")
      echo -e "$((NOW - START))\t$pid\t$COMM\t$THREADS\t$RSS\t$HWM\t$VSZ\t$FDS\t$UTIME\t$STIME\t$IO" >> "$OUT"
    fi
  done
  if [ -n "$JAVA_PID" ] && [ -d "/proc/$JAVA_PID" ]; then
    if GC_ONE=$(timeout 10 jstat -gc "$JAVA_PID" 2>/dev/null | tail -n +2 | head -n 1) \
      && [ -n "$GC_ONE" ]; then
      GC_ONE=$(echo "$GC_ONE" | tr -s ' ' | sed 's/^ //;s/ /:/g')
      echo -e "$((NOW - START))\t$JAVA_PID\t$GC_ONE" | tr ':' '\t' >> "$GC_OUT"
    else
      echo -e "$((NOW - START))\t$JAVA_PID\tJSTAT_GAP" >> "$GC_OUT"
    fi
  fi
  sleep 0.2
done
