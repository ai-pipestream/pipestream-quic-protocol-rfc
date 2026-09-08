#!/usr/bin/env bash
# Background resource sampler: RSS/HWM/FDs/IO per PID into TSV.
# Usage: sample.sh OUTFILE PID... (runs until killed)
set -euo pipefail
OUT="$1"; shift
echo -e "t_ms\tpid\tcomm\trss_kb\tvsz_kb\tfds\trchar\twchar\tread_bytes\twrite_bytes" > "$OUT"
START=$(date +%s%3N)
while true; do
  NOW=$(date +%s%3N)
  for pid in "$@"; do
    if [ -d "/proc/$pid" ]; then
      COMM=$(tr '\0' ' ' < "/proc/$pid/comm" 2>/dev/null || echo "?")
      RSS=$(awk '/VmRSS/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
      VSZ=$(awk '/VmSize/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
      FDS=$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)
      IO=$(awk -F': ' '/^(rchar|wchar|read_bytes|write_bytes)/{printf "%s%s", sep, $2; sep="\t"}' "/proc/$pid/io" 2>/dev/null || echo "")
      echo -e "$((NOW - START))\t$pid\t$COMM\t$RSS\t$VSZ\t$FDS\t$IO" >> "$OUT"
    fi
  done
  sleep 0.2
done
