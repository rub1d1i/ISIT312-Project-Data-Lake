#!/bin/bash
# ---------------------------------------------------------------------------
# Runs the whole prototype from an empty lake, in order, and captures the
# output of every step in evidence/logs. Every log of the report comes from
# one run of this script.
#
# Prerequisites: data prepared (data_prep/01-03), env/lake.sh up, env/lake.sh start-all.
# Run on the host:  caffeinate -dims evidence/run_all.sh     (about 60 minutes; keep the host awake). A step whose log shows a failed
# statement stops the run (capture.sh), the error path of the BPMN control flow.
# ---------------------------------------------------------------------------
set -u -o pipefail
cd "$(dirname "$0")"
C=./capture.sh
L=../env/lake.sh
SPARK='spark-shell --master yarn --conf spark.ui.showConsoleProgress=false -i'
BEE='beeline --color=false -u jdbc:hive2://localhost:10000 -n bigdata -f'
P=project/pipelines
# before every step: start any service that has stopped (HBase does not survive a long pause of the host)
step() { echo; echo "######## $1  ($(date +%H:%M:%S))"; $L start-all > /dev/null 2>&1 || { echo "services did not come up"; exit 1; }; }

step "0 empty the lake (raw zone is re-landed by step 1)"
$L exec 'hadoop fs -rm -r -f -skipTrash /lake/curated /lake/analytics /lake/checkpoints /lake/raw/stream /lake/tmp > /dev/null 2>&1;
         beeline -u jdbc:hive2://localhost:10000 -n bigdata --silent=true -e "DROP DATABASE IF EXISTS lake_tmp CASCADE; DROP DATABASE IF EXISTS dw CASCADE; DROP DATABASE IF EXISTS lake_curated CASCADE; DROP DATABASE IF EXISTS lake_raw CASCADE;" > /dev/null 2>&1;
         printf "disable_all \x27fleet:.*\x27\ny\ndrop_all \x27fleet:.*\x27\ny\ndisable_all \x27media:.*\x27\ny\ndrop_all \x27media:.*\x27\ny\ndrop_namespace \x27fleet\x27\ndrop_namespace \x27media\x27\n" | hbase shell -n > /dev/null 2>&1; echo emptied' | tail -1

step "1 batch landing";               $C 01_ingest_batch           "bash $P/01_ingest_batch.sh" || exit 1
step "2 HBase tables";                $C 02_hbase_tables           "bash $P/hbase/01_tables.sh" || exit 1
step "3 stream";                      $C 03_stream_demo            "bash $P/stream/run_stream_demo.sh" || exit 1
step "4 Hive over the raw zone"; WIDE=1 $C 04_hive_raw_tables      "$BEE $P/hive/01_raw_tables.hql" || exit 1
step "5 Spark 20";                    $C 05_spark_curate_dispatch  "$SPARK $P/spark/20_curate_dispatch.scala" || exit 1
step "6 Spark 21";                    $C 06_spark_curate_other     "$SPARK $P/spark/21_curate_other_sources.scala" || exit 1
step "7 Spark 22";                    $C 07_spark_media_index      "$SPARK $P/spark/22_media_index.scala" || exit 1
step "8 Spark 30, first build";       $C 08_spark_dw_build         "$SPARK $P/spark/30_dw_build.scala" || exit 1
step "9 Hive curated tables";  WIDE=1 $C 09_hive_curated_tables    "$BEE $P/hive/02_curated_tables.hql" || exit 1
step "10 Hive warehouse tables"; WIDE=1 $C 10_hive_dw_tables       "$BEE $P/hive/03_dw_tables.hql" || exit 1
step "11 defect test: second night";  $C 11_inject_defects         "bash $P/tests/inject_defects.sh" || exit 1
step "12 Spark 20, second night";     $C 12_spark_curate_second    "$SPARK $P/spark/20_curate_dispatch.scala" || exit 1
step "13 Spark 30, second night";     $C 13_spark_dw_second        "$SPARK $P/spark/30_dw_build.scala" || exit 1
step "14 OLAP reports";        WIDE=1 $C 14_hive_olap              "$BEE $P/hive/04_olap_queries.hql" || exit 1
step "15 real-extract findings"; WIDE=1 $C 15_hive_findings        "$BEE $P/hive/05_findings_real_extract.hql" || exit 1
step "16 HBase recording index";      $C 16_hbase_recordings       "bash $P/hbase/02_load_recordings.sh" || exit 1
step "17 HBase vehicle tracks";       $C 17_hbase_position_history "bash $P/hbase/03_position_history.sh" || exit 1
step "18 Pig for E1";                 $C 18_install_pig            "bash project/env/install_pig.sh" || exit 1
step "19 experiment E1";              $C 19_experiment_e1          "bash $P/experiments/e1_engines.sh" || exit 1
step "20 experiments E2-E6";          $C 20_experiments_e2_e6      "bash $P/experiments/e2_to_e6.sh" || exit 1
step "21 sizes of the zones";           $C 21_zone_sizes             "hadoop fs -du -s -h /lake/raw /lake/raw/dispatch/orders /lake/curated /lake/curated/orders /lake/analytics /lake/analytics/dw/fact_orders 2>/dev/null; hadoop fs -count /lake/raw /lake/curated /lake/analytics 2>/dev/null" || exit 1
step "22 stream restart test";        $C 22_stream_restart_test    "bash $P/stream/run_restart_test.sh" || exit 1
echo; echo "######## done ($(date +%H:%M:%S))"
