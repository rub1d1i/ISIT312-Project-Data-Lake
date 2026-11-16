#!/bin/bash
# 10 - Spark -> HBase 1.2.6 with the HBase client API:
#        (a) batch, foreachPartition, on YARN (proves the executor classpath)
#        (b) streaming, Kafka -> foreachBatch, local[*]
#      then get/scan in the HBase shell and a Hive external table over the HBase table.
TEST=10_spark_hbase; . "$(dirname "$0")/common.sh"
B="--bootstrap-server localhost:9092"
T=smoke_hb_stream
D=/user/bigdata/smoke_hb_stream
SPARK_OUT=/tmp/smoke10_spark.out

kafka-topics.sh $B --delete --topic $T >/dev/null 2>&1; sleep 3
hdfs dfs -rm -r -f -skipTrash $D >/dev/null 2>&1
$BEELINE -e "DROP TABLE IF EXISTS smoke_hive_hbase;" >/dev/null 2>&1
printf "if exists 'smoke_spark_hb'\n  disable 'smoke_spark_hb'\n  drop 'smoke_spark_hb'\nend\n" | hbase shell -n >/dev/null 2>&1

run "grep -E '^spark\.(driver|executor)\.extraClassPath' \$SPARK_HOME/conf/spark-defaults.conf | tr ':' '\n'"
run "echo \"create 'smoke_spark_hb', 'cf'\" | hbase shell -n"; must "HBase table created"

echo; echo "=== (a) batch: foreachPartition on YARN"
run "cat $S/10a_hbase_batch.scala"
run "spark-shell --master yarn -i $S/10a_hbase_batch.scala"
must   "spark-shell exit code 0"
reject "no Scala/Spark error in the shell output" "^<console>|error:|Exception|^Caused by"
expect "ran on YARN" "### master=yarn"
expect "4 rows sent" "### SMOKE10A batch rows sent to HBase: 4"

echo; echo "=== (b) streaming: Kafka -> foreachBatch -> HBase"
run "kafka-topics.sh $B --create --topic $T --partitions 3 --replication-factor 1"; must "topic created"
run "printf 's1,erin,Sydney\ns2,frank,Dubai\n' | kafka-console-producer.sh $B --topic $T"; must "first messages produced"
run "cat $S/10b_hbase_stream.scala"
echo
echo "\$ spark-shell --master 'local[*]' -i $S/10b_hbase_stream.scala > $SPARK_OUT 2>&1 &"
spark-shell --master 'local[*]' -i $S/10b_hbase_stream.scala > $SPARK_OUT 2>&1 < /dev/null &
SPID=$!
for i in $(seq 1 180); do grep -q '### STREAM STARTED' $SPARK_OUT 2>/dev/null && break; sleep 1; done
if grep -q '### STREAM STARTED' $SPARK_OUT; then echo "[PASS] streaming query started (after ${i}s)"; else echo "[FAIL] streaming query did not start"; FAILS=$((FAILS+1)); fi
sleep 12
run "printf 's3,gina,Wollongong\n' | kafka-console-producer.sh $B --topic $T"; must "second message produced while the query runs"
wait $SPID; SRC=$?
run "cat $SPARK_OUT"
if [ $SRC -eq 0 ]; then echo "[PASS] spark-shell exit code 0"; else echo "[FAIL] spark-shell exit code $SRC"; FAILS=$((FAILS+1)); fi
reject "no Scala/Spark error in the shell output" "^<console>|error:|Exception|^Caused by"
expect "first micro-batch wrote 2 rows" "### SMOKE10B batch 0: 2 row\(s\) written to HBase"
expect "a later micro-batch wrote 1 row" "### SMOKE10B batch [1-9][0-9]*: 1 row\(s\) written to HBase"
expect "query stopped cleanly" "### active streams after stop: 0"

echo; echo "=== HBase shell: confirm what Spark wrote"
run "hbase shell -n <<'HB'
get 'smoke_spark_hb', 'b1'
get 'smoke_spark_hb', 's3'
scan 'smoke_spark_hb'
count 'smoke_spark_hb'
HB"
must   "hbase shell exit code 0"
expect "get b1 (batch write)"      "cf:name +timestamp=[0-9]+, value=alice"
expect "get s3 (streaming write)"  "cf:city +timestamp=[0-9]+, value=Wollongong"
expect "scan shows b4"             "b4 +column=cf:name, timestamp=[0-9]+, value=dave"
expect "scan shows s2"             "s2 +column=cf:city, timestamp=[0-9]+, value=Dubai"
expect "7 rows in total (4 batch + 3 streaming)" "^7 row\(s\)"

echo; echo "=== Hive external table over the HBase table (HBaseStorageHandler)"
run "cat $S/10_hive_hbase.sql"
run "$BEELINE -f $S/10_hive_hbase.sql"
must   "beeline exit code 0"
reject "no HiveServer2 error" "^Error:|FAILED:"
expect "Hive reads a batch row from HBase"     "\| b3 +\| carol +\| Wollongong +\|"
expect "Hive reads a streaming row from HBase" "\| s1 +\| erin +\| Sydney +\|"
expect "Hive-on-MapReduce aggregate over HBase: Dubai = 3"      "\| Dubai +\| 3 +\|"
expect "Hive-on-MapReduce aggregate over HBase: Sydney = 2"     "\| Sydney +\| 2 +\|"
expect "Hive-on-MapReduce aggregate over HBase: Wollongong = 2" "\| Wollongong +\| 2 +\|"

echo; echo "--- clean up"
run "$BEELINE -e 'DROP TABLE smoke_hive_hbase;'"; must "Hive table dropped"
# Hive made an (empty) warehouse folder for the external table and DROP leaves it behind.
run "hdfs dfs -rm -r -f -skipTrash /user/hive/warehouse/smoke_hive_hbase"; must "empty warehouse folder of the external table removed"
run "hbase shell -n <<'HB'
disable 'smoke_spark_hb'
drop 'smoke_spark_hb'
list
HB"
must "HBase table dropped"
run "kafka-topics.sh $B --delete --topic $T"; must "topic deleted"
run "hdfs dfs -rm -r -skipTrash $D"; must "checkpoint folder removed"
rm -f $SPARK_OUT
finish
