#!/bin/bash
# ---------------------------------------------------------------------------
# Experiments E2 to E6 - where a tool taught in the subject does not fit.
#
#   E2  MapReduce for fresh data : a MapReduce job over ONE MINUTE of positions.
#   E3  small files on HDFS      : recordings as landed against the packed file.
#   E4  Hive for a key look-up   : "where is vehicle X now?" in Hive and in HBase,
#                                  on the same streamed data.
#   E5  Hive for single writes   : one status change as a Hive INSERT and as an
#                                  HBase put.
#   E6  HBase for analytics      : one aggregate over all positions, on the HBase
#                                  table and on the Parquet table.
#
# Run inside the lake container after the batch, streaming and HBase steps:
#   bash e2_to_e6.sh
# ---------------------------------------------------------------------------
set -uo pipefail
STREAMING=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar
BEELINE="$HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 -n bigdata --silent=true"
now() { date +%s.%N; }
took() { echo "$1" "$2" | awk '{printf "%.1f", $2 - $1}'; }
hb() { hbase shell -n 2>/dev/null | grep -vE "SLF4J|^$|^nil$"; }
REC=/lake/raw/pbx/recordings/branch=A/dt=2026-11-16
PACKED=/lake/curated/media/recordings_packed/branch=A/dt=2026-11-16

echo "=================== E2: MapReduce on one minute of positions ==================="
hadoop fs -mkdir -p /lake/tmp/e2_in
grep -h "2026-11-16 08:30:00" /home/bigdata/project/data/real/gps_branchA_20261116.csv "/home/bigdata/project/data/synthetic/gps/branch=B/gps_branchB_20261116.csv" > /tmp/gps_0830.csv
echo "positions received in the minute 08:30 (both branches): $(wc -l < /tmp/gps_0830.csv)"
hadoop fs -put -f /tmp/gps_0830.csv /lake/tmp/e2_in/
for run in 1 2 3; do
  hadoop fs -rm -r -f -skipTrash /lake/tmp/e2_out > /dev/null 2>&1
  T0=$(now)
  hadoop jar $STREAMING -D mapreduce.job.name="E2 vehicles per state, one minute" -D mapreduce.job.reduces=1 \
    -mapper "cut -d, -f11" -reducer "uniq -c" -input /lake/tmp/e2_in -output /lake/tmp/e2_out > /tmp/e2_run.log 2>&1
  T1=$(now)
  echo "run $run: MapReduce job over one minute of data took $(took $T0 $T1) s (submit to result: every run is a new job)"
done
echo "result (positions per vehicle state code):"; hadoop fs -cat /lake/tmp/e2_out/part-*

echo
echo "=================== E3: small files against one packed file ==================="
echo "-- what the NameNode has to keep"
echo "as landed : $(hdfs fsck $REC 2>/dev/null | grep -E 'Total files|Total blocks' | tr -s ' \t' ' ' | tr '\n' ' ')"
echo "packed    : $(hdfs fsck $PACKED 2>/dev/null | grep -E 'Total files|Total blocks' | tr -s ' \t' ' ' | tr '\n' ' ')"
echo "-- a map-only job that reads every recording once and counts bytes"
hadoop fs -rm -r -f -skipTrash /lake/tmp/e3_small /lake/tmp/e3_packed > /dev/null 2>&1
T0=$(now)
hadoop jar $STREAMING -D mapreduce.job.name="E3 read recordings as landed" -D mapreduce.job.reduces=0 \
  -mapper "wc -c" -input $REC -output /lake/tmp/e3_small > /tmp/e3_small.log 2>&1
T1=$(now)
echo "as landed : $(grep -oE 'number of splits:[0-9]+' /tmp/e3_small.log | sed 's/number of splits:/map tasks /')   $(took $T0 $T1) s"
T0=$(now)
hadoop jar $STREAMING -D mapreduce.job.name="E3 read recordings packed" -D mapreduce.job.reduces=0 \
  -inputformat org.apache.hadoop.mapred.SequenceFileAsTextInputFormat \
  -mapper "wc -c" -input $PACKED -output /lake/tmp/e3_packed > /tmp/e3_packed.log 2>&1
T1=$(now)
echo "packed    : $(grep -oE 'number of splits:[0-9]+' /tmp/e3_packed.log | sed 's/number of splits:/map tasks /')   $(took $T0 $T1) s"

echo
echo "=================== E4: latest position of one vehicle, Hive and HBase ==================="
DRV=$(echo "scan 'fleet:driver_position', {LIMIT => 1, COLUMNS => 'p:state', FILTER => \"SingleColumnValueFilter('p', 'state', =, 'binary:2')\"}" | hb | grep -oE "A\|[0-9]+" | head -1 | cut -d'|' -f2)
echo "vehicle: branch A, driver id $DRV"
echo "(both answers are timed by the tool itself: Hive by HiveServer2, HBase by its shell; client start-up is left out for both)"
echo "-- Hive on the positions the stream landed in the raw zone"
$HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 -n bigdata -e "SELECT ts, lat, lon, state, id FROM lake_raw.gps_stream WHERE branch = 'A' AND cod_drv = $DRV ORDER BY ts DESC, id DESC LIMIT 1;" 2>&1 | grep -E "^[|+]|selected"
echo "-- HBase get by row key (the same stream wrote this row)"
echo "get 'fleet:driver_position', 'A|$DRV'" | hb

echo
echo "=================== E5: one status change, Hive INSERT and HBase put ==================="
$BEELINE -e "CREATE DATABASE IF NOT EXISTS lake_tmp LOCATION '/lake/tmp/_hive'; DROP TABLE IF EXISTS lake_tmp.e5_order_status;
CREATE TABLE lake_tmp.e5_order_status (order_id BIGINT, status INT, changed_at STRING) STORED AS TEXTFILE;" > /dev/null 2>&1
for i in 1 2; do
  echo -n "Hive INSERT of one row, run $i: "
  $HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 -n bigdata -e "INSERT INTO lake_tmp.e5_order_status VALUES (7683340$i, 6, '2026-11-16 08:4$i:00');" 2>&1 | grep -oE "No rows affected \([0-9.]+ seconds\)"
done
echo "files behind the Hive table after two single-row inserts:"
hadoop fs -ls /lake/tmp/_hive/e5_order_status | grep -v "^Found" | awk '{print "  " $5 " bytes  " $8}'
echo "-- HBase put of one cell"
echo "put 'fleet:driver_position', 'E5|1', 'p:state', '6'" | hb
echo "deleteall 'fleet:driver_position', 'E5|1'" | hb > /dev/null
$BEELINE -e "DROP TABLE lake_tmp.e5_order_status;" > /dev/null 2>&1

echo
echo "=================== E6: one aggregate over all positions, HBase and Parquet ==================="
$BEELINE -e "DROP TABLE IF EXISTS lake_tmp.position_history_hbase;
CREATE EXTERNAL TABLE lake_tmp.position_history_hbase (rowkey STRING, lat DOUBLE, lon DOUBLE, speed_kmh DOUBLE, state INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,p:lat,p:lon,p:speed_kmh,p:state')
TBLPROPERTIES ('hbase.table.name' = 'fleet:position_history');" > /dev/null 2>&1
echo "-- HBase shell: count all rows (a full scan)"
echo "count 'fleet:position_history', INTERVAL => 1000000" | hb | tail -2
# each query three times, alternating, so that one slow run cannot decide the comparison
for run in 1 2 3; do
  T0=$(now)
  R1=$($BEELINE --showHeader=false --outputformat=tsv2 -e "SELECT state, COUNT(*) FROM lake_tmp.position_history_hbase GROUP BY state;" 2>/dev/null | sort | tr '\n\t' ' =')
  T1=$(now)
  R2=$($BEELINE --showHeader=false --outputformat=tsv2 -e "SELECT state_id, COUNT(*) FROM lake_curated.gps_positions GROUP BY state_id;" 2>/dev/null | sort | tr '\n\t' ' =')
  T2=$(now)
  echo "run $run: Hive over the HBase table $(took $T0 $T1) s   |   Hive over the Parquet table $(took $T1 $T2) s"
done
echo "positions per state from HBase  : $R1"
echo "positions per state from Parquet: $R2"
$BEELINE -e "DROP TABLE lake_tmp.position_history_hbase;" > /dev/null 2>&1
hadoop fs -rm -r -f -skipTrash /lake/tmp/e2_in /lake/tmp/e2_out /lake/tmp/e3_small /lake/tmp/e3_packed > /dev/null 2>&1
