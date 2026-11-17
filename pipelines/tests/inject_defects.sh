#!/bin/bash
# ---------------------------------------------------------------------------
# Defect test: lands a "second night" of data with known defects in the raw
# zone (see make_defects.py), so that the next run of Spark jobs 20 and 30
# must remove an older order version, give a new client a new key, load two
# orphan orders with the UNKNOWN member, list them in the exception folder and
# show a control-total difference of exactly 1,200.00.
# Run inside the lake container after the first warehouse build:
#   bash inject_defects.sh
# ---------------------------------------------------------------------------
set -euo pipefail
P=/home/bigdata/project
T=/tmp/defects
rm -rf $T
python3 $P/pipelines/tests/make_defects.py $P/data/synthetic $T
hadoop fs -put -f $T/orders_A_20261115_second_night.csv "/lake/raw/dispatch/orders/branch=A/dt=2026-11-15/" 2>/dev/null
hadoop fs -put -f $T/clients_delta_20261116.csv /lake/raw/dispatch/reference/clients/ 2>/dev/null
hadoop fs -ls "/lake/raw/dispatch/orders/branch=A/dt=2026-11-15/" /lake/raw/dispatch/reference/clients/ 2>/dev/null | grep -v "^Found"
