#!/bin/bash
# Runs one command inside the lake container and saves what a terminal would
# (exit status 1 if the log shows a failed statement; run_all.sh then stops)
# show (prompt, the command exactly as run, its output) to evidence/logs/<name>.log,
# followed by the wall-clock time the command took.
#
#   ./capture.sh <name> '<command>'
#   WIDE=1 ./capture.sh <name> '<command>'     run in a 190-column terminal
#
# WIDE=1 gives the command a pseudo-terminal 190 columns wide, so that Beeline
# does not wrap the statements it echoes. The command is the same.
#
# The log keeps everything. The pictures of the report are drawn from these
# logs by render_terminal.py; they leave out the known noise lines listed in
# make_shots.sh, and mark every other omitted part with "[... n lines omitted ...]".
set -u -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAME=$1; CMD=$2
mkdir -p "$HERE/logs" "$HERE/shots"
LOG="$HERE/logs/$NAME.log"
T0=$(date +%s)
{
  echo "bigdata@datalake:~\$ $CMD"
  if [ "${WIDE:-0}" = "1" ]; then
    "$HERE/../env/lake.sh" exec "script -qec 'stty cols 190 rows 60; $CMD' /dev/null" 2>&1 | tr -d '\r'
    echo "${PIPESTATUS[0]}" > "$LOG.status"
  else
    "$HERE/../env/lake.sh" exec "$CMD" 2>&1
    echo "$?" > "$LOG.status"
  fi
  echo "[wall-clock time of the command: $(( $(date +%s) - T0 )) s]"
} > "$LOG"
echo "captured $LOG ($(wc -l < "$LOG") lines, $(( $(date +%s) - T0 )) s)"
# The error path of the nightly load (fig05): spark-shell -i and Beeline keep going after a failed
# statement, so the log itself is checked. A step that failed stops the run.
STATUS=$(cat "$LOG.status" 2>/dev/null || echo 0); rm -f "$LOG.status"
PATTERN='^<console>:[0-9]+: error:|^error:|^Error: |^FAILED: |Exception in thread "main"|^ERROR: |^[a-zA-Z_.]+\.[A-Za-z]+(Exception|Error): '
if [ "$STATUS" != "0" ]; then
  echo "STEP FAILED: $NAME - the command exited with status $STATUS" >&2
  exit 1
fi
if grep -qE "$PATTERN" "$LOG"; then
  echo "STEP FAILED: $NAME - error lines in $LOG:" >&2
  grep -nE "$PATTERN" "$LOG" | head -5 >&2
  exit 1
fi
