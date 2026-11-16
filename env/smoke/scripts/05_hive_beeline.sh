#!/bin/bash
# 05 - Hive through Beeline/HiveServer2: external partitioned table, ROLLUP, window function.
TEST=05_hive_beeline; . "$(dirname "$0")/common.sh"
D=/user/bigdata/smoke_hive

run "hdfs dfs -rm -r -f -skipTrash $D"
run "hdfs dfs -mkdir -p $D/staging $D/sales"
run "printf '1,apple,10.0\n2,banana,20.0\n3,apple,5.0\n' > /tmp/smoke_east.csv; printf '4,apple,7.5\n5,cherry,12.5\n' > /tmp/smoke_west.csv"
run "hdfs dfs -put -f /tmp/smoke_east.csv $D/staging/east.csv && hdfs dfs -put -f /tmp/smoke_west.csv $D/staging/west.csv"
must "staging files uploaded"

run "cat $S/05_hive.sql"
run "$BEELINE -f $S/05_hive.sql"
must   "beeline script exit code 0"
reject "no error reported by HiveServer2" "^Error:|FAILED:"
expect "both partitions exist" "region=east"
expect "both partitions exist (west)" "region=west"
expect "ROLLUP subtotal for east = 35.0" "\| east +\| NULL +\| 35\.0 +\|"
expect "ROLLUP subtotal for west = 20.0" "\| west +\| NULL +\| 20\.0 +\|"
expect "ROLLUP grand total = 55.0" "\| NULL +\| NULL +\| 55\.0 +\|"
expect "window function: banana ranks 1st in east, region_total 35.0" "\| 2 +\| east +\| banana +\| 20\.0 +\| 1 +\| 35\.0 +\|"
expect "window function: cherry ranks 1st in west, region_total 20.0" "\| 5 +\| west +\| cherry +\| 12\.5 +\| 1 +\| 20\.0 +\|"

run "hdfs dfs -ls -R $D/sales"
expect "LOAD moved the file into the partition folder" "sales/region=east/east.csv"

echo; echo "--- clean up"
run "$BEELINE -e 'DROP TABLE smoke_sales;'"; must "drop table"
run "hdfs dfs -rm -r -skipTrash $D";          must "remove HDFS folder"
rm -f /tmp/smoke_east.csv /tmp/smoke_west.csv
finish
