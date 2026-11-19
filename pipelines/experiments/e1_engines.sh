#!/bin/bash
# ---------------------------------------------------------------------------
# Experiment E1 - one report, four engines, three data sizes.
#
# Report: number of orders and quoted amount per branch, day and status code.
#
# Engines : hand-written MapReduce (Hadoop Streaming, Python), Pig, Hive (on
#           MapReduce), Spark SQL (on YARN). All four are submitted to the
#           same YARN cluster and timed from submit to result, start-up
#           included, because that is what a user waits for.
# Input   : the raw order export as text, one file per branch, at 1x (the
#           archive as it is, about 220,000 rows), 5x and 20x. The files are
#           merged first so that every engine gets the same few large files
#           and the small-files effect (experiment E3) does not blur this one.
# Last    : Hive and Spark on the curated Parquet table (1x), to show what
#           the storage format adds.
#
# Every engine runs three times per size and the median is reported; an input of one
# order row measures the start-up of each engine on its own.
# One node, small data: this measures start-up cost and how each engine
# grows with the data, not behaviour on a real cluster.
#
# Run inside the lake container:  bash e1_engines.sh
# ---------------------------------------------------------------------------
set -uo pipefail
HERE=/home/bigdata/project/pipelines/experiments
STREAMING=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar
BEELINE="$HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 -n bigdata --silent=true --showHeader=false --outputformat=tsv2"
PIG=/home/bigdata/pig-0.17.0/bin/pig
BASE=/lake/tmp/e1
RES=/tmp/e1_results.txt; : > $RES
[ -x "$PIG" ] || { echo "Pig is not installed: run  bash /home/bigdata/project/env/install_pig.sh  first"; exit 1; }
now() { date +%s.%N; }
took() { echo "$1" "$2" | awk '{printf "%.0f", $2 - $1}'; }

echo "== preparing inputs: merged raw order export at 1x, 5x and 20x"
hadoop fs -rm -r -f -skipTrash $BASE > /dev/null 2>&1
for b in A B; do
  hadoop fs -cat "/lake/raw/dispatch/orders/branch=$b/*/*.csv" 2>/dev/null | grep -v '^COD_CL' > /tmp/e1_$b.csv
done
for n in 1 5 20; do
  for b in A B; do
    hadoop fs -mkdir -p $BASE/x$n/branch=$b/dt=all
    for i in $(seq $n); do cat /tmp/e1_$b.csv; done | hadoop fs -put - $BASE/x$n/branch=$b/dt=all/orders.csv
  done
done
rm -f /tmp/e1_A.csv /tmp/e1_B.csv
hadoop fs -du -s -h $BASE/x1 $BASE/x5 $BASE/x20 2>/dev/null
$BEELINE -e "
CREATE DATABASE IF NOT EXISTS lake_tmp LOCATION '/lake/tmp/_hive';
DROP TABLE IF EXISTS lake_tmp.e1_x1;  CREATE EXTERNAL TABLE lake_tmp.e1_x1  LIKE lake_raw.orders LOCATION '$BASE/x1';
DROP TABLE IF EXISTS lake_tmp.e1_x5;  CREATE EXTERNAL TABLE lake_tmp.e1_x5  LIKE lake_raw.orders LOCATION '$BASE/x5';
DROP TABLE IF EXISTS lake_tmp.e1_x20; CREATE EXTERNAL TABLE lake_tmp.e1_x20 LIKE lake_raw.orders LOCATION '$BASE/x20';
ALTER TABLE lake_tmp.e1_x1  SET TBLPROPERTIES ('skip.header.line.count'='0'); MSCK REPAIR TABLE lake_tmp.e1_x1;
ALTER TABLE lake_tmp.e1_x5  SET TBLPROPERTIES ('skip.header.line.count'='0'); MSCK REPAIR TABLE lake_tmp.e1_x5;
ALTER TABLE lake_tmp.e1_x20 SET TBLPROPERTIES ('skip.header.line.count'='0'); MSCK REPAIR TABLE lake_tmp.e1_x20;" > /dev/null 2>&1

# one run of each engine on input $1 (folder) / Hive table $2; prints seconds, sets ROWS and NOTE
run_mr() {
  hadoop fs -rm -r -f -skipTrash /lake/tmp/e1_out_mr > /dev/null 2>&1
  local T0=$(now)
  hadoop jar $STREAMING -D mapreduce.job.name="E1 MapReduce" -D mapreduce.job.reduces=1 -D stream.num.map.output.key.fields=3 \
    -files $HERE/mr_orders_mapper.py,$HERE/mr_orders_reducer.py \
    -mapper "python3 mr_orders_mapper.py" -reducer "python3 mr_orders_reducer.py" \
    -input "$1/*/*" -output /lake/tmp/e1_out_mr > /tmp/e1_mr.log 2>&1
  local T1=$(now)
  ROWS=$(hadoop fs -cat /lake/tmp/e1_out_mr/part-* 2>/dev/null | wc -l | tr -d ' ')
  NOTE="$(grep -oE 'number of splits:[0-9]+' /tmp/e1_mr.log | cut -d: -f2) map tasks"
  hadoop fs -cat /lake/tmp/e1_out_mr/part-* 2>/dev/null | sort > /tmp/e1_mr_rows.txt
  took $T0 $T1
}
run_pig() {
  hadoop fs -rm -r -f -skipTrash /lake/tmp/e1_out_pig > /dev/null 2>&1
  local T0=$(now)
  $PIG -x mapreduce -param INPUT=$1 -param OUTPUT=/lake/tmp/e1_out_pig $HERE/e1_orders.pig > /tmp/e1_pig.log 2>&1
  local T1=$(now)
  ROWS=$(hadoop fs -cat /lake/tmp/e1_out_pig/part-* 2>/dev/null | wc -l | tr -d ' ')
  # same rows as MapReduce? compare branch, day, status, count
  NOTE=$(diff <(hadoop fs -cat /lake/tmp/e1_out_pig/part-* 2>/dev/null | awk -F'\t' '{print $1"\t"$2"\t"$3"\t"$4}' | sort) <(awk -F'\t' '{print $1"\t"$2"\t"$3"\t"$4}' /tmp/e1_mr_rows.txt) > /dev/null && echo "same counts as MapReduce" || echo "DIFFERENT from MapReduce")
  took $T0 $T1
}
run_hive() {
  local T0=$(now)
  ROWS=$($BEELINE -e "SELECT branch, substr(d_pzakaz,1,10), status, COUNT(*), ROUND(SUM(CAST(s_pay AS DOUBLE)),2) FROM $2 GROUP BY branch, substr(d_pzakaz,1,10), status;" 2>/dev/null | grep -c .)
  local T1=$(now)
  NOTE=""
  took $T0 $T1
}
run_spark() {
  local T0=$(now)
  E1_INPUT=$1 E1_FORMAT=csv $SPARK_HOME/bin/spark-shell --master yarn --conf spark.ui.showConsoleProgress=false -i $HERE/e1_spark.scala > /tmp/e1_spark.log 2>&1
  local T1=$(now)
  ROWS=$(grep -oE 'E1_SPARK rows=[0-9]+' /tmp/e1_spark.log | cut -d= -f2)
  NOTE=$(grep -oE 'query alone [0-9.]+ s' /tmp/e1_spark.log)
  took $T0 $T1
}
median3() { printf "%s\n%s\n%s\n" "$1" "$2" "$3" | sort -n | sed -n 2p; }
# three runs of one engine; the result line carries the median and the three runs
three() {   # label engine input table size
  local r1 r2 r3
  r1=$($2 "$3" "$4"); r2=$($2 "$3" "$4"); r3=$($2 "$3" "$4" ; echo "|$ROWS|$NOTE")
  local rest=${r3#*|}; r3=${r3%%|*}
  echo "$1|$5|$(median3 $r1 $r2 $r3)|${rest%%|*}|${rest#*|}; runs $r1/$r2/$r3 s" >> $RES
}

echo "== start-up alone: each engine on an input of one order row, three runs"
ONE=$(hadoop fs -cat "/lake/raw/dispatch/orders/branch=A/*/*.csv" 2>/dev/null | grep -v '^COD_CL' | head -1)
hadoop fs -mkdir -p $BASE/empty/branch=A/dt=all; echo "$ONE" | hadoop fs -put - $BASE/empty/branch=A/dt=all/orders.csv
$BEELINE -e "DROP TABLE IF EXISTS lake_tmp.e1_empty; CREATE EXTERNAL TABLE lake_tmp.e1_empty LIKE lake_raw.orders LOCATION '$BASE/empty';
ALTER TABLE lake_tmp.e1_empty SET TBLPROPERTIES ('skip.header.line.count'='0'); MSCK REPAIR TABLE lake_tmp.e1_empty;" > /dev/null 2>&1
three "MapReduce (Hadoop Streaming, Python)" run_mr  $BASE/empty lake_tmp.e1_empty 1row
three "Pig (on MapReduce)"                   run_pig $BASE/empty lake_tmp.e1_empty 1row
three "Hive (on MapReduce)"                  run_hive $BASE/empty lake_tmp.e1_empty 1row
three "Spark SQL (on YARN)"                  run_spark $BASE/empty lake_tmp.e1_empty 1row

for n in 1 5 20; do
  echo "== size ${n}x, three runs per engine"
  three "MapReduce (Hadoop Streaming, Python)" run_mr  $BASE/x$n lake_tmp.e1_x$n ${n}x
  three "Pig (on MapReduce)"                   run_pig $BASE/x$n lake_tmp.e1_x$n ${n}x
  three "Hive (on MapReduce)"                  run_hive $BASE/x$n lake_tmp.e1_x$n ${n}x
  three "Spark SQL (on YARN)"                  run_spark $BASE/x$n lake_tmp.e1_x$n ${n}x
done

echo "== curated Parquet table (1x): what the storage format adds"
T0=$(now)
ROWS=$($BEELINE -e "SELECT branch_code, order_date, status, COUNT(*), SUM(quoted_pay) FROM lake_curated.orders GROUP BY branch_code, order_date, status;" 2>/dev/null | grep -c .)
T1=$(now)
echo "Hive on curated Parquet|1x|$(took $T0 $T1)|$ROWS|" >> $RES
T0=$(now)
E1_INPUT=/lake/curated/orders E1_FORMAT=parquet $SPARK_HOME/bin/spark-shell --master yarn --conf spark.ui.showConsoleProgress=false -i $HERE/e1_spark.scala > /tmp/e1_spark.log 2>&1
T1=$(now)
echo "Spark SQL on curated Parquet|1x|$(took $T0 $T1)|$(grep -oE 'E1_SPARK rows=[0-9]+' /tmp/e1_spark.log | cut -d= -f2)|$(grep -oE 'query alone [0-9.]+ s' /tmp/e1_spark.log)" >> $RES

echo
echo "== E1 result: seconds from submit to result (start-up included), median of three runs"
printf "%-38s %5s %8s %6s   %s\n" "engine" "size" "seconds" "rows" "note"
awk -F'|' '{printf "%-38s %5s %8s %6s   %s\n", $1, $2, $3, $4, $5}' $RES
hadoop fs -rm -r -f -skipTrash /lake/tmp/e1_out_mr /lake/tmp/e1_out_pig $BASE > /dev/null 2>&1
$BEELINE -e "DROP TABLE IF EXISTS lake_tmp.e1_x1; DROP TABLE IF EXISTS lake_tmp.e1_x5; DROP TABLE IF EXISTS lake_tmp.e1_x20; DROP TABLE IF EXISTS lake_tmp.e1_empty;" > /dev/null 2>&1
