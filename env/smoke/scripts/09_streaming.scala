// 09 - Structured Streaming: Kafka -> parse JSON -> (a) windowed count with watermark -> console
//                                                   (b) parsed events -> Parquet files on HDFS
// Run with:  spark-shell --master 'local[*]' -i 09_streaming.scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

val base = "hdfs://localhost:8020/user/bigdata/smoke_stream"
val schema = new StructType().add("ts", TimestampType).add("device", StringType).add("value", DoubleType)

val raw = spark.readStream.format("kafka").
  option("kafka.bootstrap.servers", "localhost:9092").
  option("subscribe", "smoke_stream").
  option("startingOffsets", "earliest").
  load()

// Kafka gives key/value as bytes: cast the value to a string, then parse the JSON.
val events = raw.selectExpr("CAST(value AS STRING) AS json", "partition", "offset").
  select(from_json($"json", schema, Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss")).as("e"), $"partition", $"offset").
  select("e.*", "partition", "offset")

// (a) events per device per 1-minute event-time window; data more than 1 minute late is dropped
val counts = events.
  withWatermark("ts", "1 minute").
  groupBy(window($"ts", "1 minute"), $"device").
  agg(count("*").as("events"), avg("value").as("avg_value"))

val q1 = counts.writeStream.queryName("smoke_console").
  outputMode("update").format("console").option("truncate", "false").
  option("checkpointLocation", base + "/chk_console").
  trigger(Trigger.ProcessingTime("5 seconds")).start()

// (b) the parsed events as Parquet files on HDFS, with a checkpoint
val q2 = events.writeStream.queryName("smoke_parquet").
  format("parquet").option("path", base + "/out").
  option("checkpointLocation", base + "/chk_parquet").
  trigger(Trigger.ProcessingTime("5 seconds")).start()

println("### STREAMS STARTED: " + spark.streams.active.map(_.name).sorted.mkString(","))
q1.awaitTermination(50000)          // let both queries run for 50 seconds

println("### console query progress (batchId, inputRows, watermark):")
q1.recentProgress.filter(_.numInputRows > 0).foreach(p => println(s"###   batch=${p.batchId} rows=${p.numInputRows} watermark=${p.eventTime.get("watermark")}"))
q1.stop(); q2.stop()
println("### active streams after stop: " + spark.streams.active.length)

// what the file sink wrote, read back as a normal (batch) DataFrame
val written = spark.read.parquet(base + "/out")
written.orderBy("ts").show(false)
println("### PARQUET_ROWS=" + written.count)

// Kafka as a batch source and as a sink (same connector jar)
println("### KAFKA_BATCH_ROWS=" + spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "smoke_stream").load().count)
Seq("hello from the spark kafka sink").toDF("value").write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "smoke_stream_out").save()
println("### KAFKA_SINK_WRITTEN")
System.exit(0)
