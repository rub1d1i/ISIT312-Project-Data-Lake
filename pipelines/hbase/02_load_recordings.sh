#!/bin/bash
# ---------------------------------------------------------------------------
# Bulk load of the recording index into HBase (table media:recording).
#
# 1. Hive writes the curated audio index as tab-separated text, with the HBase
#    row key <branch>|<order id>|<recording id> as the first field.
# 2. ImportTsv (a MapReduce job shipped with HBase) loads it.
# 3. A prefix scan shows the look-up the quality-control team needs:
#    "give me the recordings of order N".
#
# Run inside the lake container:  bash 02_load_recordings.sh
# ---------------------------------------------------------------------------
set -euo pipefail
BEELINE="$HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 -n bigdata --silent=true"
TSV=/lake/tmp/recording_index_tsv

echo "== 1. export the index from Hive"
hadoop fs -rm -r -f -skipTrash $TSV > /dev/null
$BEELINE -e "
INSERT OVERWRITE DIRECTORY '$TSV'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT concat(branch_code, '|', order_id, '|', recording_uid), file_name, stored_in, offset_bytes, duration_s, size_bytes, op_id, call_ts
FROM lake_curated.audio_index
WHERE order_id IS NOT NULL;"
hadoop fs -cat $TSV/* | sed -n '1,2p'

echo "== 2. ImportTsv"
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.columns='HBASE_ROW_KEY,r:file,r:stored_in,r:offset,r:duration_s,r:size_bytes,r:op_id,r:call_ts' \
  media:recording $TSV 2>&1 | grep -E "Job job_.*completed|Bad Lines|Map output records|Map input records"

echo "== 3. look-up by order"
ORDER=$(hadoop fs -cat $TSV/* | sed -n '1p' | cut -f1 | cut -d'|' -f2)
hbase shell -n <<EOF 2>/dev/null | grep -vE "SLF4J|^$"
count 'media:recording'
scan 'media:recording', {ROWPREFIXFILTER => 'A|$ORDER|'}
EOF
hadoop fs -rm -r -f -skipTrash $TSV > /dev/null
