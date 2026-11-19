// ---------------------------------------------------------------------------
// Streaming pipeline: vehicle positions of both branches
//   Kafka topic gps.positions  ->  Spark Structured Streaming  ->  three sinks
//
//   A. supply board    : free and busy vehicles per zone of each city in
//                        5-minute event-time windows: the input for pricing
//                        (few free cars in a zone = raise the price there).
//                        Console here; in production a store read by the
//                        pricing screen and a board for both cities.
//   B. raw landing     : every position appended to the raw zone as Parquet,
//                        by city and day. The nightly batch promotes and
//                        compacts these files (pipelines/spark/20).
//   C. latest position : one row per vehicle in HBase table fleet:driver_position
//
// Live order handling stays in the dispatch system of each branch. This job
// does not replace it; it gives the lake a continuous copy and gives
// management a view across branches that neither dispatch system has.
//
// Windows use the time of the position, not the time it reached Kafka, and a
// 10-minute watermark, so positions of a branch whose link was down for a few
// minutes are still counted in the window they belong to.
//
// Run (HBase client jars are on the Spark class path, see env/README.md):
//   spark-shell --master yarn -i 51_stream_gps.scala
// The job runs for STREAM_SECONDS seconds (default 240), then reports and stops.
// ---------------------------------------------------------------------------
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import spark.implicits._

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")
val runSeconds = sys.env.getOrElse("STREAM_SECONDS", "240").toInt
// topic, landing folder and checkpoint folder can be changed for the restart test (run_restart_test.sh)
val topic = sys.env.getOrElse("STREAM_TOPIC", "gps.positions")
val landingPath = sys.env.getOrElse("STREAM_LANDING", "/lake/raw/stream/gps")
val ckpt = sys.env.getOrElse("STREAM_CKPT", "/lake/checkpoints")

// The HBase write, kept in a serializable object so it can be shipped to executors.
// Every cell is written with the time of the position as its HBase timestamp: if an older
// position arrives after a newer one, HBase still returns the newer one as the latest version.
object HBaseSink extends Serializable {
  def writePartition(rows: Iterator[Row]): Unit = {
    val conn = ConnectionFactory.createConnection(HBaseConfiguration.create())
    val table = conn.getTable(TableName.valueOf("fleet:driver_position"))
    val cf = Bytes.toBytes("p")
    try {
      rows.foreach { r =>
        val at = r.getAs[Long]("event_ms")
        val state = r.getAs[Int]("state")
        val onShift = state != 0                 // an off-shift driver's row keeps the state but not where the car is
        val put = new Put(Bytes.toBytes(r.getAs[String]("branch") + "|" + r.getAs[Int]("cod_drv")))
        put.addColumn(cf, Bytes.toBytes("ts"), at, Bytes.toBytes(r.getAs[String]("ts")))
        put.addColumn(cf, Bytes.toBytes("lat"), at, Bytes.toBytes(if (onShift) r.getAs[Double]("lat").toString else ""))
        put.addColumn(cf, Bytes.toBytes("lon"), at, Bytes.toBytes(if (onShift) r.getAs[Double]("lon").toString else ""))
        put.addColumn(cf, Bytes.toBytes("speed_ms"), at, Bytes.toBytes(if (onShift) Option(r.getAs[Any]("speed")).map(_.toString).getOrElse("") else ""))
        put.addColumn(cf, Bytes.toBytes("state"), at, Bytes.toBytes(state.toString))
        table.put(put)
      }
    } finally { table.close(); conn.close() }
  }
}

val schema = new StructType()
  .add("id", LongType).add("branch", StringType).add("cod_drv", IntegerType).add("ts", StringType)
  .add("lat", DoubleType).add("lon", DoubleType).add("speed", DoubleType).add("track", DoubleType)
  .add("fix", IntegerType).add("quality", IntegerType).add("state", IntegerType).add("terminal", LongType)

// static reference data joined to the stream (landed in the raw zone by the batch ingestion)
val refDir = "/lake/raw/dispatch/reference"
val drivers = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$refDir/drivers")
  .select(when($"branch_id" === 1, "A").otherwise("B").as("branch"), $"cod_drv", $"employment_type")

val received = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", topic)
  .option("startingOffsets", "earliest")
  .option("maxOffsetsPerTrigger", 20000)
  .load()
  .select(from_json($"value".cast("string"), schema).as("p"), $"timestamp".as("kafka_ts"), $"partition", $"offset")
  .select($"p.*", $"kafka_ts", $"partition", $"offset")
  .withColumn("event_ts", to_timestamp($"ts", "yyyy-MM-dd HH:mm:ss"))
  .withWatermark("event_ts", "10 minutes")
val positions = received.where($"fix" === 1)           // a point without a satellite fix is not a position

// ---- A. supply board: free and busy vehicles per zone ----------------------------
// The zones of a city form a 3 x 4 grid over its built-up area (reference table zones):
// the zone of a position is computed from the grid, then named from the table.
val zoneRef = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$refDir/zones")
  .withColumn("branch", when($"branch_id" === 1, "A").otherwise("B"))
val grid = zoneRef.groupBy("branch").agg(min("lat_min").as("s"), max("lat_max").as("n"), min("lon_min").as("w"), max("lon_max").as("e"),
                                         first("branch_id").as("branch_id"))
val zoneNames = zoneRef.select($"zone_id", $"zone_code", $"zone_type")
val board = positions
  .join(broadcast(grid), Seq("branch"))
  .withColumn("row", lit(2) - floor(($"lat" - $"s") / (($"n" - $"s") / 3)))
  .withColumn("col", floor(($"lon" - $"w") / (($"e" - $"w") / 4)))
  .withColumn("zone_id", when($"row".between(0, 2) && $"col".between(0, 3), $"branch_id" * 100 + $"row" * 4 + $"col" + 1))
  .join(broadcast(zoneNames), Seq("zone_id"), "left")
  .join(broadcast(drivers), Seq("branch", "cod_drv"), "left")
  .groupBy(window($"event_ts", "5 minutes"), $"branch", coalesce($"zone_code", lit("OUTSIDE")).as("zone"))
  .agg(approx_count_distinct(when($"state" === 1, $"cod_drv")).as("free"),
       approx_count_distinct(when($"state" === 2, $"cod_drv")).as("on_order"),
       approx_count_distinct(when($"state" === 1 && $"employment_type" === "OWN_FLEET", $"cod_drv")).as("free_own_fleet"))
  .select(date_format($"window.start", "HH:mm").as("from"), date_format($"window.end", "HH:mm").as("to"), $"branch", $"zone", $"free", $"on_order", $"free_own_fleet")

val qBoard = board.writeStream.queryName("supply_board").outputMode("update").format("console")
  .option("truncate", "false").option("numRows", 12)
  .trigger(Trigger.ProcessingTime("10 seconds")).start()

// ---- B. raw landing ---------------------------------------------------------------
val qLanding = received.withColumn("dt", date_format($"event_ts", "yyyy-MM-dd"))   // raw zone: everything, as received
  .writeStream.queryName("raw_landing").outputMode("append").format("parquet")
  .option("path", landingPath)
  .option("checkpointLocation", s"$ckpt/gps_raw_landing")
  .partitionBy("branch", "dt")
  .trigger(Trigger.ProcessingTime("30 seconds")).start()

// ---- C. latest position per vehicle into HBase --------------------------------------
val qHBase = positions.writeStream.queryName("latest_position").outputMode("append")
  .foreachBatch { (batch: DataFrame, batchId: Long) =>
    val latest = batch.groupBy("branch", "cod_drv").agg(max(struct($"event_ts", $"id", $"ts", $"lat", $"lon", $"speed", $"state")).as("p"))
      .select($"branch", $"cod_drv", $"p.ts", $"p.lat", $"p.lon", $"p.speed", $"p.state", (unix_timestamp($"p.event_ts") * 1000).as("event_ms"))
    latest.rdd.foreachPartition(HBaseSink.writePartition _)
  }
  .option("checkpointLocation", s"$ckpt/gps_latest_position")
  .trigger(Trigger.ProcessingTime("10 seconds")).start()

// ---- run for a while, then report what each query did -------------------------------
Thread.sleep(runSeconds * 1000L)
println("\n================ streaming progress report ================")
for (q <- Seq(qBoard, qLanding, qHBase)) {
  val done = q.recentProgress.filter(_.numInputRows > 0)
  if (done.nonEmpty) {
    val rows = done.map(_.numInputRows).sum
    val ms = done.map(_.durationMs.get("triggerExecution").toLong).sorted
    println(f"${q.name}%-16s micro-batches with data: ${done.length}%3d   positions: $rows%6d   micro-batch time ms  min ${ms.head}%5d  median ${ms(ms.length / 2)}%5d  max ${ms.last}%5d")
  } else println(f"${q.name}%-16s no data received")
}
val last = qBoard.recentProgress.filter(_.numInputRows > 0).lastOption
last.foreach(p => println(s"supply board, last micro-batch with data: event time ${p.eventTime}"))
spark.streams.active.foreach(_.stop())

// how late did each branch's positions reach Kafka, compared with the first arrival of the same recorded minute?
// (one replayed minute takes one second of real time, so seconds here read as minutes in production)
val landed = spark.read.parquet(landingPath)
println(s"landing folder holds ${landed.count} rows, ${landed.select("branch", "id").distinct.count} distinct positions")
val firstSeen = landed.groupBy("event_ts").agg(min("kafka_ts").as("first_arrival"))
println("arrival delay per branch, in replayed minutes")
landed.join(firstSeen, "event_ts")
  .withColumn("delay_min", unix_timestamp($"kafka_ts") - unix_timestamp($"first_arrival"))
  .groupBy("branch").agg(count(lit(1)).as("positions"), round(avg("delay_min"), 1).as("avg_delay"), max("delay_min").as("max_delay"))
  .orderBy("branch").show(false)
System.exit(0)
