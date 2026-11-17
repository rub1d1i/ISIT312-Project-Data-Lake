# ISIT312 data lake environment

One container, `isit312-datalake`, with Hadoop, Hive, HBase, Spark and Kafka on a single node.
It runs in its own Colima VM (docker context `colima`), separate from Docker Desktop. Nothing here
changes the machine's current docker context; every command goes through `docker --context colima`.

## Versions

| Component | Version | Where | Notes |
|---|---|---|---|
| OS / Java / Python | Ubuntu 22.04.5 (arm64), OpenJDK 1.8.0_504, Python 3.10.12 | | user `bigdata` (password `bigdata`, passwordless sudo) |
| Hadoop | 2.7.3 | `/usr/share/hadoop` | pseudo-distributed, `fs.defaultFS = hdfs://localhost:8020` |
| Hive | 2.3.9 | `/usr/share/hive` | Derby metastore behind the metastore service `thrift://localhost:9083`; engine = MapReduce |
| HBase | 1.2.6 | `/usr/share/hbase` | its own ZooKeeper (3.4.6) on **2181** |
| Spark | 2.4.8, Scala 2.11.12, built for Hadoop 2.7 | `/usr/share/spark` | `SPARK_HOME` set, `spark-shell` on PATH |
| Kafka | 2.8.2 (Scala 2.12 build) | `/usr/share/kafka` | `KAFKA_HOME` set; its own bundled ZooKeeper (3.5.9) on **2182**; one broker on `localhost:9092` |
| Spark-Kafka connector | `spark-sql-kafka-0-10_2.11-2.4.8.jar` + `kafka-clients-2.0.0.jar` | `$SPARK_HOME/jars` | offline, no `--packages` needed |
| Runtime | Colima 0.10.3 (Lima 2.3.0), Docker 29.5.2 | VM: 8 CPU, 14 GB RAM, 60 GB disk, `vz`, `virtiofs` | |

Image `isit312-datalake:latest` is built `FROM isit312-hadoop:latest` (the lab image) by `Dockerfile`.

## Start and stop

Run from this folder (`project/env`) on the Mac:

```bash
./lake.sh up          # start the Colima VM if needed, check the Mac's ports, create/start the container
./lake.sh start-all   # HDFS+YARN+JobHistory, Hive, HBase, Kafka; returns when each one answers (~1 min)
./lake.sh status      # jps + port checks; exit code 0 only if everything is up
./lake.sh shell       # interactive bash as bigdata
./lake.sh exec 'hdfs dfs -ls /'     # one command, non-interactive (bash -lc)
./lake.sh stop-all    # stop the services
./lake.sh down        # stop the services, then the container (container and its data are kept)
```

Single groups: `./lake.sh start-core | start-hive | start-hbase | start-kafka`. Starting is idempotent:
a daemon that is already running is left alone. Inside the container the same commands are
`lakectl start-all`, `lakectl status`, `lakectl stop-hive`, and so on.

`project/` on the Mac is mounted at `/home/bigdata/project` (read-write).

After a reboot only `./lake.sh up && ./lake.sh start-all` is needed. The VM holds up to 14 GB while
it runs; `colima stop` releases it (`./lake.sh up` starts it again).

Rebuild from nothing: `./lake.sh build`, `./lake.sh up`, `./lake.sh start-all`. `build` needs the lab base
image `isit312-hadoop:latest`: it copies it from Docker Desktop if it is there, and otherwise builds it from
`base/` (Hadoop 2.7.3 and Hive 2.3.9, then HBase 1.2.6; see `base/README.md`).

## Ports (published on the Mac at 127.0.0.1 only)

| Port | What | URL / use |
|---|---|---|
| 50070 | NameNode UI | http://localhost:50070 |
| 50075 | DataNode UI | http://localhost:50075 |
| 8088 | YARN ResourceManager UI | http://localhost:8088 |
| 8042 | NodeManager UI | http://localhost:8042 |
| 19888 | JobHistory UI | http://localhost:19888 |
| 10000 | HiveServer2 JDBC | `jdbc:hive2://localhost:10000` |
| 10002 | HiveServer2 UI | http://localhost:10002 |
| 16010 | HBase Master UI | http://localhost:16010 |
| 4040, 4041 | Spark UI of the first / second running application | http://localhost:4040 |
| 9092 | Kafka broker | `localhost:9092` |

Inside the container only: 8020 NameNode RPC, 9083 Hive metastore, 2181 HBase ZooKeeper,
2182 Kafka ZooKeeper, 16000 HMaster, 16201/16301 HRegionServer.

`./lake.sh up` refuses to start and names the owner if one of the published ports is already taken.

Memory, measured with `docker --context colima stats --no-stream`: 3.85 GiB of 13.62 GiB with all
twelve daemons idle; 5.79 GiB with a `spark-shell --master yarn` session (2 executors) on top.

## Commands that work

```bash
# Beeline
beeline -u jdbc:hive2://localhost:10000 -n bigdata
beeline -u jdbc:hive2://localhost:10000 -n bigdata -f script.sql

# Spark shell (Scala). No extra flags are needed for Hive, Kafka or HBase.
spark-shell --master 'local[*]'
spark-shell --master yarn
spark-shell --master 'local[*]' -i script.scala      # non-interactive; end the script with System.exit(0)

# Kafka
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic t1 --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic t1
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic t1 --from-beginning
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic t1

# Hadoop Streaming
hadoop jar /usr/share/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar \
  -files mapper.py,reducer.py -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' \
  -input <hdfs in> -output <hdfs out>
```

### Spark and Hive

Set in `$SPARK_HOME/conf/spark-defaults.conf` and `$SPARK_HOME/conf/hive-site.xml`:

```
spark.sql.catalogImplementation     hive
spark.sql.hive.metastore.version    2.3.3
spark.sql.hive.metastore.jars       /usr/share/hive/lib/*
spark.sql.warehouse.dir             hdfs://localhost:8020/user/hive/warehouse
hive.metastore.uris                 thrift://localhost:9083        (hive-site.xml)
```

`df.write.saveAsTable("t")` creates a Parquet table in the Hive warehouse that Beeline can query;
`spark.table("t")` reads tables created in Beeline. Relative paths such as
`spark.read.csv("folder/file.csv")` resolve to `hdfs://localhost:8020/user/bigdata/...`.
Hive must be running (`start-hive`) before `spark-shell` touches a table.

### Structured Streaming from Kafka

```scala
val raw = spark.readStream.format("kafka").
  option("kafka.bootstrap.servers", "localhost:9092").
  option("subscribe", "t1").option("startingOffsets", "earliest").load()
val lines = raw.selectExpr("CAST(value AS STRING) AS value")
```

Full working example (JSON parsing, windowed count with watermark to the console, Parquet files plus
checkpoint on HDFS, clean stop): `smoke/scripts/09_streaming.scala`.

### Spark to HBase

The HBase client jars are already on the driver and executor classpath
(`spark.driver.extraClassPath` / `spark.executor.extraClassPath` in `spark-defaults.conf`), so this
works in plain `spark-shell`, in `local[*]` and on YARN:

```scala
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

df.rdd.foreachPartition { rows =>
  val conn  = ConnectionFactory.createConnection(HBaseConfiguration.create())
  val table = conn.getTable(TableName.valueOf("my_table"))        // create it first in hbase shell
  try {
    rows.foreach { r =>
      val put = new Put(Bytes.toBytes(r.getString(0)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(r.getString(1)))
      table.put(put)
    }
  } finally { table.close(); conn.close() }
}
```

In a streaming query, call the same code from
`writeStream.foreachBatch { (batch: DataFrame, batchId: Long) => batch.rdd.foreachPartition(...) }`.
Working examples: `smoke/scripts/10a_hbase_batch.scala` (batch) and
`smoke/scripts/10b_hbase_stream.scala` (Kafka to HBase).

The classpath holds seven jars from `/usr/share/hbase/lib`: `hbase-client`, `hbase-common`,
`hbase-protocol`, `metrics-core-2.2.0` (these four are the minimum for `Put`/`Get`/`Scan`), plus
`hbase-server`, `hbase-hadoop-compat`, `hbase-hadoop2-compat` (needed for `TableInputFormat` /
`TableOutputFormat`). The same four passed with `--jars` also work.

Hive over an HBase table:

```sql
CREATE EXTERNAL TABLE t_hbase (rowkey STRING, name STRING, city STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:name,cf:city')
TBLPROPERTIES ('hbase.table.name' = 'my_table');
```

## What differs from the lab image

- `yarn-site.xml`: NodeManager memory 8192 MB (was 6144), minimum allocation 512 MB, disk health
  threshold 98.5 %.
- `mapred-site.xml`: 1024 MB map/reduce/AM containers with a 768 MB heap (the Hadoop default heap
  is 200 MB, too small for Hive on real data).
- `capacity-scheduler.xml`: `maximum-am-resource-percent` 0.5 (was 0.1), so two YARN applications
  can run at once, for example a Spark streaming job and a Hive query.
- `snappy-java` jar replaced in Hadoop, Hive and HBase (see quirk 1).
- HiveServer2 runs with a 1 GB heap and the metastore with 512 MB (set by `lakectl`).
- Added: Spark, Kafka, `netcat`, `lakectl`. User, paths and everything else are unchanged.

## Known quirks and workarounds

1. **Snappy on Apple Silicon.** The `snappy-java` shipped with Hadoop 2.7.3, HBase 1.2.6 and Hive
   2.3.9 has no native library for linux/arm64. Hive then fails on any Snappy Parquet file, which
   is what Spark writes by default (`SnappyError: [FAILED_TO_LOAD_NATIVE_LIBRARY]`, shown in Beeline
   as `return code 2 from MapRedTask`). The image replaces those jars with Spark's
   `snappy-java-1.1.8.2.jar`. Nothing to do; this explains the jar swap in the Dockerfile.
2. **Hive runs on MapReduce.** Every query that is not a plain `SELECT *` starts a YARN job and
   takes roughly 10 to 25 seconds even on tiny data. The "Hive-on-MR is deprecated" warning is normal.
3. **HBase without `metrics-core-2.2.0.jar` hangs silently.** If you build your own HBase
   classpath for Spark and leave that jar out, the write does not fail, it retries forever.
4. **`URISyntaxException ... HBase Counters.class`** lines appear when `TableInputFormat` is used
   in `spark-shell --master 'local[*]'`. They are harmless; the read returns the right rows.
5. **`spark-shell -i script.scala` exits with code 0 even if a statement failed**, because the REPL
   carries on after an error. Read the output for `error:` or `Exception`.
6. **Spark's progress bar** (`[Stage 0:> ...]`) shares a line with your `println` output when the
   output is captured to a file. Add `--conf spark.ui.showConsoleProgress=false` to switch it off.
7. **`spark.sql.shuffle.partitions` is 8**, not Spark's default 200, so aggregations and stateful
   streaming batches are not split into 200 tiny tasks. Override with `--conf` if needed.
8. **PySpark does not work.** Spark 2.4.8 predates Python 3.10. Use Scala (`spark-shell`).
9. **`beeline` is Hive's.** Spark ships an older `beeline` too; the PATH order keeps Hive's first.
10. **HBase RegionServer listens on 16201/16301**, not 16020/16030, because `start-hbase.sh` starts
    a localhost-only region server that way.
11. **All data lives inside the container** (HDFS, Hive metastore, HBase, Kafka). `./lake.sh down`
    and `up` keep it. `docker --context colima rm isit312-datalake` deletes it for good.
12. **Ports are bound to 127.0.0.1.** To publish on all interfaces, remove the container and run
    `LAKE_BIND=0.0.0.0 ./lake.sh up`.
13. **Clock is UTC** inside the container.
14. **"nodemanager did not stop gracefully after 5 seconds: killing with kill -9"** during
    `stop-all` is Hadoop 2.7's own stop script and is harmless.
15. **Spark cannot read a Hive table that is backed by HBase** (`STORED BY ... HBaseStorageHandler`):
    `spark.table(...)` fails with `ClassNotFoundException: ...HiveHBaseTableInputFormat`. Query such
    tables from Beeline; from Spark read HBase directly (client API or `TableInputFormat`).
16. **`DROP TABLE` on an external Hive table keeps its folder.** That is normal for external tables;
    for an HBase-backed one it leaves an empty folder under `/user/hive/warehouse/<table>`.
17. **Kafka from the Mac.** The broker advertises `localhost:9092` and answers a metadata request
    sent from the Mac. Producing and consuming from the Mac itself was not tested (no Kafka client
    is installed there); inside the container it is tested.
18. **Logs.** Hive: `/home/bigdata/logs/hiveserver2.log`, `/home/bigdata/logs/hive-metastore.log`
    and `/tmp/bigdata/hive.log`. Kafka: `/home/bigdata/logs/kafka/`. Hadoop: `/usr/share/hadoop/logs`
    (YARN container logs under `userlogs/<application>/<container>/`). HBase: `/usr/share/hbase/logs`.

## Smoke tests

`smoke/run_smoke.sh` runs the tests in `smoke/scripts/` inside the container and writes
`smoke/<nn>_<name>.log` (commands plus full output, with `[PASS]`/`[FAIL]` lines and a final
`RESULT:`). The tests create only `smoke_*` tables, topics and paths and remove them afterwards.

`./run_smoke.sh` runs tests 01 to 10. `./run_smoke.sh 11_yarn_concurrency` is an extra test: a
Structured Streaming job on YARN stays running while a Hive query runs as a second YARN job.

## Files

| File | Purpose |
|---|---|
| `Dockerfile` | builds `isit312-datalake:latest` |
| `lake.sh` | launcher on the Mac |
| `lakectl` | service control inside the container (`/usr/local/bin/lakectl`) |
| `conf/hadoop/` | `yarn-site.xml`, `mapred-site.xml`, `capacity-scheduler.xml` |
| `conf/spark/` | `spark-defaults.conf`, `hive-site.xml`, `spark-env.sh` |
| `conf/kafka/` | `server.properties`, `zookeeper.properties` |
| `smoke/` | smoke tests and their logs |
