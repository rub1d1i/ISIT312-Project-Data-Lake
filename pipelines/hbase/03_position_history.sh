#!/bin/bash
# ---------------------------------------------------------------------------
# HBase table fleet:position_history - the track of every vehicle, keyed so
# that one vehicle's positions are stored together in time order:
#
#   row key : <branch>|<driver id, 7 digits>|<yyyyMMddHHmm>|<position id>
#   family p: lat, lon, speed_kmh, state
#
# Use: "show me where the car was during order N" (complaint or fare dispute)
# is a short range scan between two row keys, not a search through a day of
# positions.
#
# 1. Hive exports the curated positions as tab-separated text.
# 2. ImportTsv loads them.
# 3. The track of one completed order of the real extract is read back.
#
# Run inside the lake container:  bash 03_position_history.sh
# ---------------------------------------------------------------------------
set -euo pipefail
BEELINE="$HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 -n bigdata --silent=true"
TSV=/lake/tmp/position_history_tsv

echo "== 1. table"
if echo "exists 'fleet:position_history'" | hbase shell -n 2>/dev/null | grep -q "does exist"; then
  printf "disable 'fleet:position_history'\ndrop 'fleet:position_history'\n" | hbase shell -n > /dev/null 2>&1
fi
echo "create 'fleet:position_history', {NAME => 'p', VERSIONS => 1}" | hbase shell -n 2>/dev/null | grep -vE "SLF4J|^$|^nil$"

echo "== 2. export from Hive and bulk load"
hadoop fs -rm -r -f -skipTrash $TSV > /dev/null
$BEELINE -e "
INSERT OVERWRITE DIRECTORY '$TSV'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT concat(branch_code, '|', lpad(cast(cod_drv AS STRING), 7, '0'), '|', date_format(received_ts, 'yyyyMMddHHmm'), '|', ping_id),
       lat, lon, speed_kmh, state_id
FROM lake_curated.gps_positions;"
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.columns='HBASE_ROW_KEY,p:lat,p:lon,p:speed_kmh,p:state' \
  fleet:position_history $TSV 2>&1 | grep -E "Job job_.*completed|Bad Lines|Map input records"

echo "== 3. track of one completed order (real extract, branch A)"
ORDER=$($BEELINE --showHeader=false --outputformat=tsv2 -e "
SELECT concat_ws(' ', cast(order_id AS STRING), lpad(cast(cod_drv AS STRING), 7, '0'),
                 date_format(pickup_ts, 'yyyyMMddHHmm'), date_format(completed_ts, 'yyyyMMddHHmm')) AS trip
FROM lake_curated.orders
WHERE branch_code = 'A' AND order_date = '2026-11-16' AND outcome = 'COMPLETED' AND trip_min BETWEEN 6 AND 8 AND order_hour = 9
ORDER BY trip LIMIT 1;" 2>/dev/null | tail -1)
set -- $ORDER
echo "order $1, driver $2, picked up $3, completed $4"
hbase shell -n <<EOF 2>/dev/null | grep -vE "SLF4J|^$|^nil$"
scan 'fleet:position_history', {STARTROW => 'A|$2|$3', STOPROW => 'A|$2|$4~', COLUMNS => ['p:lat', 'p:lon', 'p:speed_kmh']}
EOF
hadoop fs -rm -r -f -skipTrash $TSV > /dev/null
