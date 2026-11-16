#!/bin/bash
# Runs the smoke tests inside the running data lake container (services must be started:
# ../lake.sh up && ../lake.sh start-all) and saves each test's commands and full output
# to smoke/<nn>_<name>.log.
#
#   ./run_smoke.sh                 run all ten tests
#   ./run_smoke.sh 05_hive_beeline 07_kafka      run only these
#
# Each test creates only smoke_* tables/topics/paths and removes them again when it is done.
set -u
SMOKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAKE="$SMOKE_DIR/../lake.sh"
IN_CONTAINER=/home/bigdata/project/env/smoke/scripts

if [ $# -eq 0 ]; then
  set -- 01_daemons 02_hdfs 03_mapreduce_pi 04_streaming_wordcount 05_hive_beeline \
         06_hbase 07_kafka 08_spark_hive 09_structured_streaming 10_spark_hbase
fi

overall=0
for t in "$@"; do
  log="$SMOKE_DIR/$t.log"
  printf '%-26s ' "$t"
  {
    echo "# run from the Mac with: ./lake.sh exec 'bash $IN_CONTAINER/$t.sh'"
    "$LAKE" exec "bash $IN_CONTAINER/$t.sh" < /dev/null
    echo "[test script exit code $?]"
  } > "$log" 2>&1
  result=$(grep -E '^RESULT: ' "$log" | tail -1)
  case "$result" in
    "RESULT: PASS"*) ;;
    *) overall=1 ;;
  esac
  echo "${result:-RESULT: FAIL (no result line - see the log)}   [$(grep -c '^\[PASS\]' "$log") pass, $(grep -c '^\[FAIL\]' "$log") fail]   $log"
done
exit $overall
