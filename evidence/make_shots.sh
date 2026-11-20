#!/bin/bash
# Draws the terminal pictures of the report and the slides from the captured
# logs (evidence/logs, written by run_all.sh). Every picture shows real output.
# Known noise is left out without a marker: SLF4J binding messages, the
# native-library and plan-truncation warnings, Kafka's topic-name warning and
# Hive's map-join housekeeping lines. Every other omitted part is marked
# "[... n lines omitted ...]".
set -euo pipefail
cd "$(dirname "$0")"
N='SLF4J|NativeCodeLoader|WARN util.Utils: Truncated|WARNING: Due to limitations|^$|^nil$'
H="$N|WARNING: Hive-on-MR|Starting to launch local task|Dump the side-table|Uploaded 1 File to|End of local task|Execution completed successfully|MapredLocal task succeeded|Execution log at:"
R="python3 render_terminal.py"
rg() { python3 ranges.py "$@"; }
L=logs
mkdir -p shots/slides

$R $L/01_ingest_batch.log           shots/t01_ingest.png          --drop "$N"
$R $L/03_stream_demo.log            shots/t03_stream_run.png      --drop "$N" --max-cols 150 \
   --lines "2-12,$(rg $L/03_stream_demo.log '^Batch: ' '^\+-' -2 3),$(rg $L/03_stream_demo.log '^=+ streaming progress' '^replayed')"
$R $L/03_stream_demo.log            shots/t03_stream_sinks.png    --drop "$N" --max-cols 100 --lines "$(rg $L/03_stream_demo.log '^== 5\.' 'row\(s\) in' 1 2)"
$R $L/04_hive_raw_tables.log        shots/t04_hive_raw.png        --drop "$H" --max-cols 150 --lines "$(rg $L/04_hive_raw_tables.log '> SHOW TABLES;' 'rows selected' 1 4)"
$R $L/05_spark_curate_dispatch.log  shots/t05_curate_dispatch.png --drop "$N" --max-cols 122
$R $L/06_spark_curate_other.log     shots/t06_curate_other.png    --drop "$N" --max-cols 122
$R $L/07_spark_media_index.log      shots/t07_media.png           --drop "$N" --max-cols 150
$R $L/08_spark_dw_build.log         shots/t08_dw_build.png        --drop "$N" --max-cols 122
# the second night: defects landed, job 20 again (orders line), job 30 again (keys, load report, control totals)
cat $L/11_inject_defects.log \
    <(grep -E 'spark-shell|ORDERS' $L/12_spark_curate_second.log) \
    <(grep -vE "$N|WARN window.WindowExec|Setting default log level|To adjust logging|Spark context|Spark session" $L/13_spark_dw_second.log) > /tmp/second_night.log
$R /tmp/second_night.log            shots/t13_second_night.png    --drop "$N|^\[wall-clock" --max-cols 122
$R $L/14_hive_olap.log              shots/t11_q1_rollup.png       --drop "$H" --max-cols 125 --lines "$(rg $L/14_hive_olap.log '> -- Q1 ' 'rows selected')"
$R $L/14_hive_olap.log              shots/t11_q6_drill.png        --drop "$H" --max-cols 150 --lines "$(rg $L/14_hive_olap.log '> -- Q6 ' 'rows selected')"
$R $L/14_hive_olap.log              shots/t11_q9_pricing.png      --drop "$H" --max-cols 150 --lines "$(rg $L/14_hive_olap.log '> -- Q9 ' 'rows selected')"
$R $L/14_hive_olap.log              shots/t11_q10_ledger.png      --drop "$H" --max-cols 125 --lines "$(rg $L/14_hive_olap.log '> -- Q10 ' 'rows selected')"
$R $L/14_hive_olap.log              shots/t11_q13_zones.png       --drop "$H" --max-cols 150 --lines "$(rg $L/14_hive_olap.log '> -- Q13 ' 'rows selected')"
Q12=$(rg $L/14_hive_olap.log '> -- Q12 ' 'rows selected')
P1=$(rg $L/14_hive_olap.log 'Path -> Alias' 'base file name')
$R $L/14_hive_olap.log              shots/t11_q12_pruning.png     --drop "$H" --max-cols 125 --lines "${Q12%-*}-$(( ${Q12%-*} + 4 )),${P1},${Q12#*-}-${Q12#*-}"
$R $L/15_hive_findings.log          shots/t12_findings_a.png      --drop "$H" --max-cols 150 --lines "$(rg $L/15_hive_findings.log '> -- F1 ' 'rows selected' 1 | cut -d- -f1)-$(rg $L/15_hive_findings.log '> -- F1c ' 'rows selected' | cut -d- -f2)"
$R $L/15_hive_findings.log          shots/t12_findings_a2.png     --drop "$H" --max-cols 150 --lines "$(rg $L/15_hive_findings.log '> -- F2 ' 'rows selected' 1 | cut -d- -f1)-$(rg $L/15_hive_findings.log '> -- F3b ' 'rows selected' | cut -d- -f2)"
$R $L/15_hive_findings.log          shots/t12_findings_b.png      --drop "$H" --max-cols 150 --lines "$(rg $L/15_hive_findings.log '> -- F4 ' 'rows selected' | cut -d- -f1)-$(rg $L/15_hive_findings.log '> -- F4b ' 'rows selected' | cut -d- -f2)"
$R $L/15_hive_findings.log          shots/t12_findings_c.png      --drop "$H" --max-cols 215 --lines "$(rg $L/15_hive_findings.log '> -- F5 ' 'row selected' | cut -d- -f1)-$(rg $L/15_hive_findings.log '> -- F6 ' 'rows selected' | cut -d- -f2)"
$R $L/16_hbase_recordings.log       shots/t13_hbase_recordings.png --drop "$N" --max-cols 150
$R $L/17_hbase_position_history.log shots/t14_hbase_track.png     --drop "$N" --max-cols 100 --lines "2-$(rg $L/17_hbase_position_history.log '^== 3\.' '^order ' | cut -d- -f2),$(( $(grep -c '' $L/17_hbase_position_history.log) - 20 ))-$(grep -c '' $L/17_hbase_position_history.log)"
$R $L/19_experiment_e1.log          shots/t15_e1.png              --drop "$N" --max-cols 150 --lines "$(rg $L/19_experiment_e1.log 'E1 result' 'Spark SQL on curated Parquet, warm' 2>/dev/null || rg $L/19_experiment_e1.log 'E1 result' 'Spark SQL on curated Parquet')"
$R $L/20_experiments_e2_e6.log      shots/t16_e2_e3.png           --drop "$N" --max-cols 110 --lines "$(rg $L/20_experiments_e2_e6.log '=== E2' 'packed    : map tasks')"
$R $L/20_experiments_e2_e6.log      shots/t16_e4_e6.png           --drop "$N" --max-cols 110 --lines "$(rg $L/20_experiments_e2_e6.log '=== E4' 'positions per state from Parquet')"
$R $L/22_stream_restart_test.log    shots/t17_restart.png         --drop "$N|^WARNING|kafka-topics|Created topic" --max-cols 120

# the two tests added after the main run: replication with a lost DataNode, the lake on object storage
[ -f $L/23_replication_test.log ] && $R $L/23_replication_test.log shots/t18_replication.png    --drop "$N|^\\[wall-clock" --max-cols 112
[ -f $L/24_object_storage.log ]   && $R $L/24_object_storage.log   shots/t19_object_storage.png --drop "$N|^\\[wall-clock" --max-cols 135

# compact pictures for the slides
$R $L/03_stream_demo.log            shots/slides/s_stream.png     --drop "$N|^supply board, last|^(raw_landing|latest_position) " --size 24 --max-cols 100 \
   --lines "$(rg $L/03_stream_demo.log '^Batch: ' '^\+-' -2 3),$(rg $L/03_stream_demo.log '^=+ streaming progress' '^replayed')"
$R $L/13_spark_dw_second.log        shots/slides/s_dw_totals.png  --drop "$N" --size 24 --max-cols 112 --lines "$(rg $L/13_spark_dw_second.log '^control totals' '^\+-+.*\+$' 1 3)"
$R $L/07_spark_media_index.log      shots/slides/s_media.png      --drop "$N" --size 24 --max-cols 120 --lines "$(rg $L/07_spark_media_index.log '^recordings as landed' '^direct read')"
echo "pictures drawn"
