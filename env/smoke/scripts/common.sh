# Shared helpers for the smoke tests (sourced by every NN_*.sh; runs inside the container).
S=/home/bigdata/project/env/smoke/scripts
BEELINE="beeline -u jdbc:hive2://localhost:10000 -n bigdata"
FAILS=0
OUT=$(mktemp /tmp/smoke_out.XXXXXX)
trap 'rm -f "$OUT"' EXIT

echo "=============================================================================="
echo "SMOKE TEST: $TEST"
echo "started:    $(date '+%F %T %Z')   host: $(hostname)   user: $(whoami)"
echo "=============================================================================="

# run <command line>  - print the command, run it, show its full output (kept in $OUT too).
run() {
  echo
  echo "\$ $*"
  eval "$*" 2>&1 | tee "$OUT"
  local rc=${PIPESTATUS[0]}
  [ "$rc" -eq 0 ] || echo "[exit code $rc]"
  return "$rc"
}
# must <description>  - the previous run has to have exited 0.
must()   { local rc=$?; if [ $rc -eq 0 ]; then echo "[PASS] $1"; else echo "[FAIL] $1 (exit code $rc)"; FAILS=$((FAILS+1)); fi; }
# expect <description> <egrep pattern>  - the previous run's output has to match.
expect() { if grep -Eq -- "$2" "$OUT"; then echo "[PASS] $1"; else echo "[FAIL] $1 (not found in output: $2)"; FAILS=$((FAILS+1)); fi; }
# reject <description> <egrep pattern>  - the previous run's output must NOT match.
reject() { if grep -Eq -- "$2" "$OUT"; then echo "[FAIL] $1 (found in output: $2)"; FAILS=$((FAILS+1)); else echo "[PASS] $1"; fi; }
finish() {
  echo
  echo "finished:   $(date '+%F %T %Z')"
  if [ "$FAILS" -eq 0 ]; then echo "RESULT: PASS ($TEST)"; exit 0; fi
  echo "RESULT: FAIL ($TEST) - $FAILS check(s) failed"; exit 1
}
