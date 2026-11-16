#!/bin/bash
# ---------------------------------------------------------------------------
# Batch ingestion into the RAW zone of the data lake.
#
# Every source lands unchanged, in its own format, under
#   /lake/raw/<source system>/<data set>/branch=<A|B>/dt=<yyyy-mm-dd>/
# so that a file can always be traced back to the system, branch and day it
# came from. Nothing is cleaned here: cleaning happens when data is promoted
# to the curated zone (pipelines/spark).
#
# In production each block below is a scheduled job per source. Here they run
# together, once, to load the prototype.
#
# Run inside the lake container as user bigdata:
#   bash /home/bigdata/project/pipelines/01_ingest_batch.sh
# ---------------------------------------------------------------------------
set -euo pipefail
DATA=${DATA:-/home/bigdata/project/data}
STAGE=${STAGE:-/tmp/lake_stage}
FS="$HADOOP_HOME/bin/hadoop fs"

iso() { echo "$1" | sed -E 's/.*([0-9]{4})([0-9]{2})([0-9]{2}).*/\1-\2-\3/'; }

echo "== 1. lake zones"
$FS -mkdir -p /lake/raw /lake/curated /lake/analytics /lake/checkpoints
# raw and curated hold personal data (client ids, vehicle positions, recordings): owner and group only.
# the analytics zone holds aggregated facts and is readable by report users.
$FS -chmod 755 /lake /lake/analytics
$FS -chmod 750 /lake/raw /lake/curated

echo "== 2. staging tree (mirrors the raw zone layout)"
rm -rf "$STAGE"; mkdir -p "$STAGE"
R="$STAGE/raw"

# dispatch system (Firebird exports of both branches): orders, vehicle positions, reference tables
for b in A B; do
  for f in "$DATA"/synthetic/orders/branch=$b/orders_${b}_*.csv; do
    d=$(iso "$(basename "$f")"); mkdir -p "$R/dispatch/orders/branch=$b/dt=$d"; cp "$f" "$R/dispatch/orders/branch=$b/dt=$d/"
  done
done
mkdir -p "$R/dispatch/orders/branch=A/dt=2026-11-16" "$R/dispatch/gps/branch=A/dt=2026-11-16" "$R/dispatch/gps/branch=B/dt=2026-11-16"
cp "$DATA/real/orders_branchA_20261116.csv" "$R/dispatch/orders/branch=A/dt=2026-11-16/"
# Vehicle positions: the table export is a one-off back-fill of history. It stops at the cut-over
# time; everything after that reaches the lake through Kafka (pipelines/stream).
CUT="2026-11-16 08:00:00"
awk -F, -v cut="$CUT" 'NR == 1 || $3 < cut' "$DATA/real/gps_branchA_20261116.csv" > "$R/dispatch/gps/branch=A/dt=2026-11-16/gps_branchA_backfill.csv"
awk -F, -v cut="$CUT" 'NR == 1 || $3 < cut' "$DATA/synthetic/gps/branch=B/gps_branchB_20261116.csv" > "$R/dispatch/gps/branch=B/dt=2026-11-16/gps_branchB_backfill.csv"
for f in "$DATA"/synthetic/reference/*.csv; do
  t=$(basename "$f" .csv); mkdir -p "$R/dispatch/reference/$t"; cp "$f" "$R/dispatch/reference/$t/"
done

# PBX: call detail records and call recordings
for b in A B; do
  for f in "$DATA"/synthetic/cdr/branch=$b/cdr_${b}_*.csv; do
    d=$(iso "$(basename "$f")"); mkdir -p "$R/pbx/cdr/branch=$b/dt=$d"; cp "$f" "$R/pbx/cdr/branch=$b/dt=$d/"
  done
done
mkdir -p "$R/pbx/recordings/branch=A/dt=2026-11-16"
cp "$DATA"/media/audio/branch=A/date=2026-11-16/*.wav "$R/pbx/recordings/branch=A/dt=2026-11-16/"

# billing and accounting
for b in A B; do
  for f in "$DATA"/synthetic/payments/branch=$b/payments_${b}_*.csv; do
    d=$(iso "$(basename "$f")"); mkdir -p "$R/billing/payments/branch=$b/dt=$d"; cp "$f" "$R/billing/payments/branch=$b/dt=$d/"
  done
done
mkdir -p "$R/finance/gl"; cp "$DATA"/synthetic/finance/*.csv "$R/finance/gl/"

# messaging: SMS gateway log, in-app chat
for f in "$DATA"/synthetic/sms/sms_*.csv; do
  d=$(iso "$(basename "$f")"); mkdir -p "$R/messaging/sms/dt=$d"; cp "$f" "$R/messaging/sms/dt=$d/"
done
for f in "$DATA"/synthetic/chat/chat_*.jsonl; do
  d=$(iso "$(basename "$f")"); mkdir -p "$R/messaging/chat/dt=$d"; cp "$f" "$R/messaging/chat/dt=$d/"
done

# mobile app / website events (nested JSON), external feeds
for dir in "$DATA"/synthetic/app_events/date=*; do
  d=${dir##*date=}; mkdir -p "$R/app/events/dt=$d"; cp "$dir/events.jsonl" "$R/app/events/dt=$d/"
done
mkdir -p "$R/external/competitor_prices" "$R/external/traffic"
cp "$DATA/synthetic/external/competitor_prices.jsonl" "$R/external/competitor_prices/"
cp "$DATA/synthetic/external/traffic_index.csv" "$R/external/traffic/"

# video: office CCTV segments, dashcam clips pulled on request, and their catalogues
mkdir -p "$R/video/cctv/branch=A/dt=2026-11-16" "$R/video/dashcam/branch=A/dt=2026-11-16" "$R/video/catalog/cctv_cameras" "$R/video/catalog/cctv_segments" "$R/video/catalog/dashcam_requests"
cp "$DATA"/media/video/cctv/branch=A/date=2026-11-16/*.mp4    "$R/video/cctv/branch=A/dt=2026-11-16/"
cp "$DATA"/media/video/dashcam/branch=A/date=2026-11-16/*.mp4 "$R/video/dashcam/branch=A/dt=2026-11-16/"
cp "$DATA/synthetic/media_catalog/cctv_cameras.csv"    "$R/video/catalog/cctv_cameras/"
cp "$DATA/synthetic/media_catalog/cctv_segments.csv"   "$R/video/catalog/cctv_segments/"
cp "$DATA/synthetic/media_catalog/dashcam_requests.jsonl" "$R/video/catalog/dashcam_requests/"

echo "   staged: $(find "$R" -type f | wc -l) files, $(du -sh "$R" | cut -f1)"

echo "== 3. load into HDFS"
START=$(date +%s)
$FS -rm -r -f -skipTrash /lake/raw/dispatch /lake/raw/pbx /lake/raw/billing /lake/raw/finance /lake/raw/messaging /lake/raw/app /lake/raw/external /lake/raw/video > /dev/null
$FS -put "$R"/* /lake/raw/
echo "   put finished in $(( $(date +%s) - START )) s"

echo "== 4. what landed (size on disk, per source system)"
$FS -du -h /lake/raw
echo "== 5. directories / files / bytes in the raw zone"
$FS -count /lake/raw
rm -rf "$STAGE"
