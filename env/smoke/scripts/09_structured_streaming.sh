#!/bin/bash
# 09 - Structured Streaming from Kafka: JSON parsing, windowed count with a watermark to the
#      console, and Parquet files + checkpoint on HDFS. Data is produced before and while the
#      queries run; the last batch contains one event that is too late and must be dropped.
TEST=09_structured_streaming; . "$(dirname "$0")/common.sh"
B="--bootstrap-server localhost:9092"
T=smoke_stream
D=/user/bigdata/smoke_stream
SPARK_OUT=/tmp/smoke09_spark.out

for t in $T ${T}_out; do kafka-topics.sh $B --delete --topic $t >/dev/null 2>&1; done; sleep 3
run "hdfs dfs -rm -r -f -skipTrash $D"
run "kafka-topics.sh $B --create --topic $T --partitions 3 --replication-factor 1"; must "input topic created"
run "kafka-topics.sh $B --create --topic ${T}_out --partitions 1 --replication-factor 1"; must "output topic created"

echo; echo "--- batch 1: produced BEFORE spark starts (read thanks to startingOffsets=earliest)"
run "printf '%s\n' '{\"ts\":\"2026-11-16 10:00:05\",\"device\":\"d1\",\"value\":1.0}' '{\"ts\":\"2026-11-16 10:00:20\",\"device\":\"d1\",\"value\":3.0}' '{\"ts\":\"2026-11-16 10:00:40\",\"device\":\"d2\",\"value\":10.0}' | kafka-console-producer.sh $B --topic $T"
must "batch 1 produced"

run "cat $S/09_streaming.scala"

echo
echo "\$ spark-shell --master 'local[*]' -i $S/09_streaming.scala > $SPARK_OUT 2>&1 &"
spark-shell --master 'local[*]' -i $S/09_streaming.scala > $SPARK_OUT 2>&1 < /dev/null &
SPID=$!
for i in $(seq 1 180); do grep -q '### STREAMS STARTED' $SPARK_OUT 2>/dev/null && break; sleep 1; done
if grep -q '### STREAMS STARTED' $SPARK_OUT; then echo "[PASS] both streaming queries started (after ${i}s)"; else echo "[FAIL] streaming queries did not start"; FAILS=$((FAILS+1)); fi
sleep 12

echo; echo "--- batch 2: produced while the queries run (moves the watermark to 10:02:30)"
run "printf '%s\n' '{\"ts\":\"2026-11-16 10:01:10\",\"device\":\"d1\",\"value\":5.0}' '{\"ts\":\"2026-11-16 10:03:30\",\"device\":\"d2\",\"value\":20.0}' | kafka-console-producer.sh $B --topic $T"
must "batch 2 produced"
sleep 12

echo; echo "--- batch 3: one event that is too late (10:00:50, behind the watermark) and one on time"
run "printf '%s\n' '{\"ts\":\"2026-11-16 10:00:50\",\"device\":\"d1\",\"value\":99.0}' '{\"ts\":\"2026-11-16 10:03:45\",\"device\":\"d1\",\"value\":7.0}' | kafka-console-producer.sh $B --topic $T"
must "batch 3 produced"

echo; echo "--- waiting for spark-shell to finish (it stops both queries itself after 50 s)"
wait $SPID; SRC=$?
run "cat $SPARK_OUT"
if [ $SRC -eq 0 ]; then echo "[PASS] spark-shell exit code 0"; else echo "[FAIL] spark-shell exit code $SRC"; FAILS=$((FAILS+1)); fi
reject "no Scala/Spark error in the shell output" "^<console>|error:|Exception|^Caused by"
expect "console sink printed micro-batches" "^Batch: 0\$"
expect "window 10:00-10:01, d1: 2 events, avg 2.0 (batch 1)" "\|\[2026-11-16 10:00:00, 2026-11-16 10:01:00\]\|d1 +\|2 +\|2\.0 +\|"
expect "window 10:00-10:01, d2: 1 event (batch 1)"           "\|\[2026-11-16 10:00:00, 2026-11-16 10:01:00\]\|d2 +\|1 +\|10\.0 +\|"
expect "window 10:01-10:02, d1: 1 event (batch 2)"           "\|\[2026-11-16 10:01:00, 2026-11-16 10:02:00\]\|d1 +\|1 +\|5\.0 +\|"
expect "window 10:03-10:04, d2: 1 event (batch 2)"           "\|\[2026-11-16 10:03:00, 2026-11-16 10:04:00\]\|d2 +\|1 +\|20\.0 +\|"
expect "window 10:03-10:04, d1: 1 event (batch 3)"           "\|\[2026-11-16 10:03:00, 2026-11-16 10:04:00\]\|d1 +\|1 +\|7\.0 +\|"
reject "watermark dropped the late event (10:00 window of d1 never reaches 3)" "\|\[2026-11-16 10:00:00, 2026-11-16 10:01:00\]\|d1 +\|3 +\|"
expect "watermark advanced to 10:02:30 after batch 2" "watermark=2026-11-16T10:02:30\.000Z"
expect "both queries stopped cleanly" "### active streams after stop: 0"
expect "file sink wrote all 7 events (it has no watermark)" "### PARQUET_ROWS=7"
expect "Kafka batch read sees 7 messages" "### KAFKA_BATCH_ROWS=7"
expect "Kafka sink write done" "### KAFKA_SINK_WRITTEN"

run "hdfs dfs -ls -R $D | grep -v -E '/chk_[a-z]+/(state|sources)/'"
expect "Parquet part files on HDFS" "$D/out/part-[0-9]+-.*\.parquet"
expect "file sink log (_spark_metadata)" "$D/out/_spark_metadata/0"
expect "checkpoint: offsets" "$D/chk_parquet/offsets/0"
expect "checkpoint: commits" "$D/chk_parquet/commits/0"

run "kafka-console-consumer.sh $B --topic ${T}_out --from-beginning --max-messages 1 --timeout-ms 30000"
expect "message written by the Spark Kafka sink is in the topic" "^hello from the spark kafka sink\$"

echo; echo "--- clean up"
run "kafka-topics.sh $B --delete --topic $T; kafka-topics.sh $B --delete --topic ${T}_out"; must "topics deleted"
run "hdfs dfs -rm -r -skipTrash $D"; must "HDFS folder removed"
rm -f $SPARK_OUT
finish
