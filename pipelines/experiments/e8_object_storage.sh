#!/bin/bash
# ---------------------------------------------------------------------------
# E8  The lake on object storage (the deployment recommended in section 7.2)
#
# The recommended deployment keeps the zones in a provider's object storage
# and starts a cluster only for the nightly load. The prototype ran on HDFS.
# This test copies part of the lake to an S3-compatible object store that runs
# in a second container (SeaweedFS, reached through Hadoop's S3A connector,
# the same connector a provider's object storage is reached with) and repeats
# the work there: a Spark job on YARN reads raw and curated data from the
# object store and writes Parquet back, and one call recording is read from
# the packed file by its byte offset.
# It shows that the jobs and the offset read work unchanged on object storage.
# The store runs on the same machine, so the times do not include a network.
#
# Run inside the lake container:  bash e8_object_storage.sh
# ---------------------------------------------------------------------------
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
export S3_ENDPOINT=objectstore.local:9000 S3_KEY=lakeaccess S3_SECRET=lakesecret123
LIB=$HADOOP_HOME/share/hadoop/tools/lib
export HADOOP_CLASSPATH="$LIB/*"                   # the S3A connector and the libraries it needs
S="-Dfs.s3a.endpoint=$S3_ENDPOINT -Dfs.s3a.access.key=$S3_KEY -Dfs.s3a.secret.key=$S3_SECRET -Dfs.s3a.connection.ssl.enabled=false"

echo "== 1. raw orders, curated orders, packed recordings and their index copied from HDFS to the object store"
hadoop fs $S -rm -r -f s3a://lake/raw s3a://lake/curated s3a://lake/tmp > /dev/null 2>&1
T0=$(date +%s)
# the folder itself is made first and its content copied into it, so a second run gives the same layout
for p in raw/dispatch/orders curated/orders curated/media/recordings_packed curated/media/audio_index; do
  hadoop fs $S -mkdir -p s3a://lake/$p 2> /dev/null
  hadoop fs $S -cp "/lake/$p/*" s3a://lake/$p/ 2> /dev/null
done
echo "copied in $(( $(date +%s) - T0 )) s"
for p in raw/dispatch/orders curated/orders curated/media/recordings_packed curated/media/audio_index; do
  printf "%-46s HDFS %10s   object store %10s\n" "$p" "$(hadoop fs -du -s /lake/$p 2> /dev/null | cut -d' ' -f1)" "$(hadoop fs $S -du -s s3a://lake/$p 2> /dev/null | cut -d' ' -f1)"
done

echo
echo "== 2. the same Spark job on YARN against both stores"
$SPARK_HOME/bin/spark-shell --master yarn --conf spark.ui.showConsoleProgress=false \
  --jars $LIB/hadoop-aws-2.7.3.jar,$LIB/aws-java-sdk-1.7.4.jar -i $HERE/e8_object_storage.scala > /tmp/e8_spark.out 2> /tmp/e8_spark.err
grep -E '^(store|HDFS|object storage) ' /tmp/e8_spark.out | cut -c1-200
grep -hE 'Exception|^<console>.*error' /tmp/e8_spark.out /tmp/e8_spark.err | head -5

echo
echo "== 3. what the object store holds afterwards"
hadoop fs $S -ls -R s3a://lake/tmp/e8_report 2> /dev/null | grep -c parquet | sed 's/^/Parquet files written by the job: /'
hadoop fs $S -du -s -h s3a://lake/raw s3a://lake/curated s3a://lake/tmp 2> /dev/null
