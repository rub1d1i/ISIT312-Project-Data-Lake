#!/bin/bash
# 11 (extra) - two YARN applications at once: Structured Streaming from Kafka with
#      --master yarn (same Scala script as test 09) while a Hive query runs as a MapReduce job.
#      With the stock capacity-scheduler setting (maximum-am-resource-percent 0.1) the second
#      application would wait in ACCEPTED until the first one ends.
TEST=11_yarn_concurrency; . "$(dirname "$0")/common.sh"
B="--bootstrap-server localhost:9092"
T=smoke_stream
D=/user/bigdata/smoke_stream
C=/user/bigdata/smoke_conc
SPARK_OUT=/tmp/smoke11_spark.out

for t in $T ${T}_out; do kafka-topics.sh $B --delete --topic $t >/dev/null 2>&1; done; sleep 3
hdfs dfs -rm -r -f -skipTrash $D $C >/dev/null 2>&1
run "kafka-topics.sh $B --create --topic $T --partitions 3 --replication-factor 1 && kafka-topics.sh $B --create --topic ${T}_out --partitions 1 --replication-factor 1"
must "topics created"
run "hdfs dfs -mkdir -p $C && printf 'a\nb\nc\nd\n' | hdfs dfs -put -f - $C/rows.txt"
run "$BEELINE -e \"DROP TABLE IF EXISTS smoke_conc; CREATE EXTERNAL TABLE smoke_conc (v STRING) LOCATION '$C';\""
must "Hive table for the concurrent query created"
run "printf '%s\n' '{\"ts\":\"2026-11-16 10:00:05\",\"device\":\"d1\",\"value\":1.0}' '{\"ts\":\"2026-11-16 10:00:20\",\"device\":\"d1\",\"value\":3.0}' '{\"ts\":\"2026-11-16 10:00:40\",\"device\":\"d2\",\"value\":10.0}' | kafka-console-producer.sh $B --topic $T"

echo
echo "\$ spark-shell --master yarn -i $S/09_streaming.scala > $SPARK_OUT 2>&1 &"
spark-shell --master yarn -i $S/09_streaming.scala > $SPARK_OUT 2>&1 < /dev/null &
SPID=$!
for i in $(seq 1 180); do grep -q '### STREAMS STARTED' $SPARK_OUT 2>/dev/null && break; sleep 1; done
if grep -q '### STREAMS STARTED' $SPARK_OUT; then echo "[PASS] streaming queries started on YARN (after ${i}s)"; else echo "[FAIL] streaming queries did not start"; FAILS=$((FAILS+1)); fi

run "yarn application -list -appStates RUNNING 2>/dev/null"
expect "the Spark application is RUNNING on YARN" "SPARK[[:space:]]+bigdata[[:space:]]+default[[:space:]]+RUNNING"

echo; echo "--- a Hive query (MapReduce job) while the Spark application is still running"
run "$BEELINE -e 'SELECT COUNT(*) AS n FROM smoke_conc;'"
must   "beeline exit code 0"
reject "no HiveServer2 error" "^Error:|FAILED:"
expect "Hive MapReduce job returned its result" "\| 4 +\|"
run "yarn application -list -appStates RUNNING 2>/dev/null"
expect "the Spark application was still RUNNING when the Hive job finished" "SPARK[[:space:]]+bigdata[[:space:]]+default[[:space:]]+RUNNING"

run "printf '%s\n' '{\"ts\":\"2026-11-16 10:01:10\",\"device\":\"d1\",\"value\":5.0}' '{\"ts\":\"2026-11-16 10:03:30\",\"device\":\"d2\",\"value\":20.0}' | kafka-console-producer.sh $B --topic $T"
sleep 10
run "printf '%s\n' '{\"ts\":\"2026-11-16 10:00:50\",\"device\":\"d1\",\"value\":99.0}' '{\"ts\":\"2026-11-16 10:03:45\",\"device\":\"d1\",\"value\":7.0}' | kafka-console-producer.sh $B --topic $T"

echo; echo "--- waiting for spark-shell to finish"
wait $SPID; SRC=$?
run "cat $SPARK_OUT"
if [ $SRC -eq 0 ]; then echo "[PASS] spark-shell exit code 0"; else echo "[FAIL] spark-shell exit code $SRC"; FAILS=$((FAILS+1)); fi
reject "no Scala/Spark error in the shell output" "^<console>|error:|Exception|^Caused by"
expect "streaming on YARN printed micro-batches" "^Batch: 0\$"
expect "windowed count correct on YARN" "\|\[2026-11-16 10:00:00, 2026-11-16 10:01:00\]\|d1 +\|2 +\|2\.0 +\|"
expect "file sink wrote the events to HDFS" "### PARQUET_ROWS=[0-9]+"
expect "both queries stopped cleanly" "### active streams after stop: 0"

run "yarn application -list -appStates FINISHED,FAILED,KILLED 2>/dev/null | tail -4"

echo; echo "--- clean up"
run "$BEELINE -e 'DROP TABLE smoke_conc;'"; must "Hive table dropped"
run "kafka-topics.sh $B --delete --topic $T; kafka-topics.sh $B --delete --topic ${T}_out"; must "topics deleted"
run "hdfs dfs -rm -r -skipTrash $D $C"; must "HDFS folders removed"
rm -f $SPARK_OUT
finish
