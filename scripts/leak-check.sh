#!/usr/bin/env bash
# T102: run the mock leak probe and sample RSS + file descriptors from outside.
#
# The probe itself watches the driver's own counters (live pumps, subscription
# release); this script watches the two things a process cannot honestly sample
# about itself: resident memory and its descriptor table.
#
# Usage: scripts/leak-check.sh [cycles] [batch] [interval-seconds]
#   cycles  default 20000   batch default 50   interval default 2
set -uo pipefail

CYCLES="${1:-20000}"
BATCH="${2:-50}"
INTERVAL="${3:-2}"

cd "$(dirname "$0")/.."
BIN="target/release/examples/leak_probe"

echo "building the probe (release)…"
if ! cargo build --release -p kafka-driver --example leak_probe >/dev/null 2>&1; then
    echo "build failed" >&2
    exit 1
fi

LOG="$(mktemp -t leak-probe)"
"$BIN" "$CYCLES" "$BATCH" > "$LOG" 2>&1 &
PID=$!

first_rss=""; last_rss=""; first_fds=""; last_fds=""; samples=0
while kill -0 "$PID" 2>/dev/null; do
    rss="$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')"
    fds="$(lsof -p "$PID" 2>/dev/null | wc -l | tr -d ' ')"
    if [ -n "$rss" ] && [ "$rss" -ge 4096 ]; then
        [ -z "$first_rss" ] && { first_rss="$rss"; first_fds="$fds"; }
        last_rss="$rss"; last_fds="$fds"; samples=$((samples + 1))
        printf 'sample=%s rss=%sKB fds=%s\n' "$samples" "$rss" "$fds"
    fi
    sleep "$INTERVAL"
done
wait "$PID"
STATUS=$?

echo "--- probe output ---"
cat "$LOG"
echo "--- summary ---"
echo "cycles=$CYCLES batch=$BATCH samples=$samples probe_exit=$STATUS"
echo "rss: first=${first_rss:-?}KB last=${last_rss:-?}KB delta=$(( ${last_rss:-0} - ${first_rss:-0} ))KB"
echo "fds: first=${first_fds:-?} last=${last_fds:-?} delta=$(( ${last_fds:-0} - ${first_fds:-0} ))"
rm -f "$LOG"
echo "(if rss delta stays a few hundred KB and fds delta is 0 over a full run, nothing accumulates)"
