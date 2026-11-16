#!/bin/bash
# 08 - spark-shell (Scala): CSV from HDFS -> DataFrame -> aggregate -> saveAsTable into Hive,
#      then the same table from Beeline; and a table created in Beeline read from Spark.
#      Done twice: --master local[*] and --master yarn.
TEST=08_spark_hive; . "$(dirname "$0")/common.sh"
D=/user/bigdata/smoke_spark

run "hdfs dfs -rm -r -f -skipTrash $D; hdfs dfs -mkdir -p $D/dept"
run "printf 'id,dept,salary\n1,eng,100\n2,eng,150\n3,ops,80\n4,ops,70\n5,hr,60\n' > /tmp/smoke_emp.csv && hdfs dfs -put -f /tmp/smoke_emp.csv $D/emp.csv"
run "printf 'eng,Dubai\nops,Sydney\nhr,Wollongong\n' > /tmp/smoke_dept.csv && hdfs dfs -put -f /tmp/smoke_dept.csv $D/dept/dept.csv"
must "test data uploaded"

echo; echo "--- a Hive table created in Beeline"
run "$BEELINE -e \"DROP TABLE IF EXISTS smoke_beeline_dept; DROP TABLE IF EXISTS smoke_spark_agg_local; DROP TABLE IF EXISTS smoke_spark_agg_yarn; CREATE EXTERNAL TABLE smoke_beeline_dept (dept STRING, location STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '$D/dept'; SELECT * FROM smoke_beeline_dept;\""
must   "beeline exit code 0"
expect "Beeline sees its own table" "\| eng +\| Dubai +\|"

run "cat $S/08_spark_hive.scala"

for mode in local yarn; do
  if [ $mode = local ]; then master="'local[*]'"; else master=yarn; fi
  echo; echo "--- spark-shell --master $master"
  run "SMOKE_SUFFIX=$mode spark-shell --master $master -i $S/08_spark_hive.scala"
  must   "spark-shell ($mode) exit code 0"
  reject "no Scala/Spark error in the shell output ($mode)" "^<console>|error:|Exception|^Caused by"
  expect "running on the requested master ($mode)" "### spark=2\.4\.8 scala=2\.11\.12 master=$mode"
  expect "Hive catalog in use, metastore client 2.3.3 ($mode)" "### catalog=hive hive.metastore.version=2\.3\.3 defaultFS=hdfs://localhost:8020"
  expect "aggregate computed in Spark ($mode)" "\| +eng\| +2\| +250\| +125\.0\|"
  expect "Spark reads the Beeline-created table ($mode)" "\| +ops\| +2\| +150\| +Sydney\|"
  expect "saveAsTable wrote 3 rows ($mode)" "### SMOKE08 rows_written=3 dept_rows=3 suffix=$mode"

  echo; echo "--- the Spark-written table read from Beeline ($mode)"
  run "$BEELINE -e \"SHOW TABLES LIKE 'smoke_*'; SELECT * FROM smoke_spark_agg_$mode ORDER BY dept; SELECT COUNT(*) AS n, SUM(total_salary) AS total FROM smoke_spark_agg_$mode;\""
  must   "beeline exit code 0 ($mode)"
  reject "no HiveServer2 error ($mode)" "^Error:|FAILED:"
  expect "Beeline lists the Spark table ($mode)" "\| smoke_spark_agg_$mode +\|"
  expect "Beeline reads Spark's Parquet rows ($mode)" "\| eng +\| 2 +\| 250 +\| 125\.0 +\|"
  expect "Hive-on-MapReduce aggregates Spark's table ($mode)" "\| 3 +\| 460 +\|"
done

run "$BEELINE -e \"DESCRIBE FORMATTED smoke_spark_agg_local;\""
expect "table is stored as Parquet in the Hive warehouse on HDFS" "hdfs://localhost:8020/user/hive/warehouse/smoke_spark_agg_local"

echo; echo "--- clean up"
run "$BEELINE -e \"DROP TABLE smoke_spark_agg_local; DROP TABLE smoke_spark_agg_yarn; DROP TABLE smoke_beeline_dept; SHOW TABLES;\""; must "tables dropped"
run "hdfs dfs -rm -r -skipTrash $D"; must "HDFS folder removed"
rm -f /tmp/smoke_emp.csv /tmp/smoke_dept.csv
finish
