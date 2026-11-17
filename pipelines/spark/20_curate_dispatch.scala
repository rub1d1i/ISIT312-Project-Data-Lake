// ---------------------------------------------------------------------------
// RAW -> CURATED, dispatch system: reference tables, order archive, vehicle positions.
//
// Curated data is typed, de-duplicated, carries derived business columns and
// data-quality flags, and is stored as Parquet.
//
// Cleaning steps marked [extract] answer a defect seen in the extract the
// company supplied. Steps marked [defensive] guard against a failure that the
// extract does not show; they are tested with defects injected into the
// synthetic history (see data_prep/02_generate_synthetic.py).
//
// Run:  spark-shell --master yarn -i 20_curate_dispatch.scala
// ---------------------------------------------------------------------------
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark.implicits._

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")   // decimals stay readable from Hive 2.x
spark.conf.set("spark.sql.shuffle.partitions", "8")

val RAW = "/lake/raw"
val CUR = "/lake/curated"
val fs = FileSystem.get(sc.hadoopConfiguration)
def tsCol(c: String): Column = to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss")
def mins(a: Column, b: Column): Column = (unix_timestamp(b) - unix_timestamp(a)) / 60.0

// ---- 1. reference tables: CSV -> Parquet, content unchanged ------------------
val refTables = Seq("branches", "zones", "addresses", "services", "order_status", "cancel_reasons", "gps_states",
                    "intake_lines", "operators", "drivers", "cars", "clients", "corp_accounts")
for (t <- refTables) {
  spark.read.option("header", "true").option("inferSchema", "true").csv(s"$RAW/dispatch/reference/$t")
    .write.mode("overwrite").parquet(s"$CUR/reference/$t")
}
println(s"reference tables promoted: ${refTables.size}")

// ---- 2. orders ---------------------------------------------------------------
// branch and dt come from the folder names (branch=A/dt=2026-10-01)
val rawOrders = spark.read.option("header", "true").csv(s"$RAW/dispatch/orders")
val nRaw = rawOrders.count()

// 2a. [defensive] rows re-sent by the branch exchange: drop exact copies, then, if an order still
//     has several versions, keep the one with the latest status change
val distinctOrders = rawOrders.distinct()
val nDistinct = distinctOrders.count()
val latestFirst = Window.partitionBy("branch", "ORDER_ID").orderBy(col("DS_STATUS").desc_nulls_last, col("D_VIPOL").desc_nulls_last)
val dedup = distinctOrders.withColumn("version_rank", row_number().over(latestFirst)).where($"version_rank" === 1).drop("version_rank")

// 2b. [extract] 28 of the 95 exported columns are empty and every column arrives as text:
//     keep the columns that carry information, with proper types and readable names
val typed = dedup.select(
  col("branch").as("branch_code"),
  col("ORDER_ID").cast("long").as("order_id"),
  col("COD_CL").cast("long").as("cod_cl"),
  col("COD_DRV").cast("int").as("cod_drv"),
  col("OP_PRN").cast("int").as("op_prn"),
  col("OP_OTP").cast("int").as("op_otp"),
  col("EADDR_FRM").cast("int").as("addr_from"),
  col("EADDR_TO").cast("int").as("addr_to"),
  col("EADDR_1").cast("int").as("stop_1"),
  col("CART").cast("int").as("cart"),
  col("STATUS").cast("int").as("status"),
  col("REAZON").cast("int").as("reazon"),
  col("CHANNEL").cast("int").as("intake_line"),
  col("CMNT").as("intake_letter"),
  col("PAY").cast("decimal(12,2)").as("pay"),
  col("S_PAY").cast("decimal(12,2)").as("quoted_pay"),
  col("DISTANCE").cast("double").as("distance_km"),
  col("BEZNAL").cast("int").as("beznal"),
  col("CARD_PAYMENT").cast("int").as("card_payment"),
  col("MEJGOROD").cast("int").as("intercity"),
  col("PODACHA").cast("int").as("promised_minutes"),
  tsCol("D_PZAKAZ").as("accepted_ts"),
  tsCol("D_VZAKAZ").as("requested_ts"),
  tsCol("D_BUSY").as("assigned_ts"),
  tsCol("D_VPRIB").as("arrived_ts"),
  tsCol("D_SPARKA").as("pickup_ts"),
  tsCol("D_VIPOL").as("completed_ts"),
  tsCol("D_OPAZD").as("late_flag_ts"),
  col("PODEZD").as("entrance_raw"),
  col("ZAKAZ_INFO").as("comment_category"),
  col("WAVF").as("recording_ref_raw"),
  col("EADDR_LAT").cast("double").as("app_pickup_lat"),
  col("EADDR_LON").cast("double").as("app_pickup_lon"),
  col("dt").cast("string").as("source_dt"))

// 2c. derived columns
val uid = regexp_extract($"recording_ref_raw", "([0-9a-f]{14})$", 1)
val netOfVat = $"pay".cast("double") / 1.22
val orders = typed
  // [extract] the recording reference is stored either as a PBX URL or as a bare id: keep the id
  .withColumn("recording_uid", when(length(uid) > 0, uid))
  // [extract] the entrance field is typed by hand: "3", "п3", "1п", "ч д", "чд", "ч/д" (private house)
  .withColumn("entrance_no", regexp_extract($"entrance_raw", "([0-9]+)", 1).cast("int"))
  .withColumn("private_house", coalesce(lower(regexp_replace($"entrance_raw", "[ /]", "")) === "чд", lit(false)))
  // [extract] the intake channel is not a column: it follows from the intake account.
  // 1583 = mobile app (orders carry phone coordinates and in-app card payments, never a call recording);
  // 218  = a second automated account, taken to be the corporate portal (assumption, not confirmed).
  .withColumn("channel",
    when($"op_prn" === 218, "CORPORATE_PORTAL")
      .when($"op_prn" === 1583 && $"intake_letter" === "Л", "WEBSITE")
      .when($"op_prn" === 1583, "MOBILE_APP")
      .otherwise("CALL_CENTRE"))
  // who pays is a separate question from how the order arrived
  .withColumn("client_segment", when($"beznal" === 1, "CORPORATE").otherwise("PRIVATE"))
  // [extract] most non-cash prices are a round amount plus 22 %: split the tax out where the pattern holds
  .withColumn("vat_amount", when($"beznal" === 1 && abs(netOfVat - round(netOfVat)) < 0.005, round($"pay".cast("double") - netOfVat, 2).cast("decimal(12,2)")))
  // business outcome from status code, reason code and which timestamps exist
  .withColumn("outcome",
    when($"pickup_ts".isNotNull && $"completed_ts".isNotNull, "COMPLETED")
      .when($"reazon" === 26, "CLIENT_NO_SHOW")
      .when($"status" === 0 && $"cod_drv".isNotNull, "CANCELLED_AFTER_ASSIGNMENT")
      .when($"status" === 0, "CANCELLED_BEFORE_ASSIGNMENT")
      .otherwise("NOT_FULFILLED"))
  .withColumn("payment_method",
    when($"outcome" =!= "COMPLETED", "NONE").when($"beznal" === 1, "CORP_INVOICE").when($"card_payment" === 1, "CARD").otherwise("CASH"))
  .withColumn("is_preorder", (unix_timestamp($"requested_ts") - unix_timestamp($"accepted_ts")) > 1800)
  // [extract] a driver "assigned before the order was accepted" is clock skew between two machines: flag it, clamp it
  .withColumn("dq_clock_skew", coalesce(!$"is_preorder" && $"assigned_ts" < $"accepted_ts", lit(false)))
  .withColumn("assign_min", when(!$"is_preorder", greatest(lit(0.0), mins($"accepted_ts", $"assigned_ts"))))
  .withColumn("arrive_min", mins($"assigned_ts", $"arrived_ts"))
  .withColumn("wait_min", mins($"arrived_ts", $"pickup_ts"))
  .withColumn("trip_min", mins($"pickup_ts", $"completed_ts"))
  .withColumn("is_late", $"late_flag_ts".isNotNull)
  .withColumn("dq_zero_distance", coalesce($"distance_km" === 0, lit(false)))
  .withColumn("dq_price_changed", coalesce(abs($"pay" - $"quoted_pay") >= 1, lit(false)))
  .withColumn("order_date", to_date($"accepted_ts"))
  .withColumn("order_hour", hour($"accepted_ts"))
  .withColumn("order_month", date_format($"accepted_ts", "yyyy-MM"))

orders.repartition($"branch_code", $"order_month")
  .write.mode("overwrite").partitionBy("branch_code", "order_month").parquet(s"$CUR/orders")

val cur = spark.read.parquet(s"$CUR/orders")
val nCur = cur.count()
println(s"ORDERS  raw rows: $nRaw   exact copies removed: ${nRaw - nDistinct}   older versions removed: ${nDistinct - nCur}   curated rows: $nCur")
println("orders: data-quality summary per branch")
cur.groupBy("branch_code").agg(
    count("*").as("orders"),
    sum($"dq_clock_skew".cast("int")).as("clock_skew"),
    sum($"dq_zero_distance".cast("int")).as("zero_distance"),
    sum($"dq_price_changed".cast("int")).as("price_changed"),
    sum($"private_house".cast("int")).as("private_house"),
    sum(($"entrance_raw".isNotNull && $"entrance_no".isNull && !$"private_house").cast("int")).as("entrance_unparsed"),
    sum($"recording_uid".isNotNull.cast("int")).as("with_recording"),
    sum($"vat_amount".isNotNull.cast("int")).as("vat_split"))
  .orderBy("branch_code").show(false)

// ---- 3. vehicle positions ------------------------------------------------------
// Two sources with the same content: the one-off back-fill export of the GPS table and the files
// written by the streaming job since the cut-over. The stream writes many small Parquet files;
// promoting them here also compacts them into one file per branch and day.
val backfill = spark.read.option("header", "true").csv(s"$RAW/dispatch/gps").select(
  col("branch"), col("ID").cast("long").as("id"), col("COD_DRV").cast("int").as("cod_drv"), col("D_RECEIVE").as("ts"),
  col("LAT").cast("double").as("lat"), col("LON").cast("double").as("lon"), col("SPEED").cast("double").as("speed"),
  col("TRACK").cast("double").as("track"), col("FIX").cast("int").as("fix"), col("QUALITY").cast("int").as("quality"),
  col("STATE_GPS_ID").cast("int").as("state"), col("ID_TERMINAL").cast("long").as("terminal"))
val streamDir = new Path(s"$RAW/stream/gps")
val hasStream = fs.exists(streamDir)
val nBackfill = backfill.count()
val landed = if (hasStream) spark.read.parquet(streamDir.toString).select("branch", "id", "cod_drv", "ts", "lat", "lon", "speed", "track", "fix", "quality", "state", "terminal") else backfill.limit(0)
val nLanded = landed.count()
val streamFiles = if (hasStream) fs.globStatus(new Path(s"$RAW/stream/gps/*/*/*.parquet")).length else 0

val perMinute = Window.partitionBy("branch_code", "cod_drv", "received_ts")
val zones = spark.read.parquet(s"$CUR/reference/zones")
  .withColumn("zone_branch", when($"branch_id" === 1, "A").otherwise("B"))
  .select("zone_id", "zone_branch", "lat_min", "lat_max", "lon_min", "lon_max")

// keep one row per position: the lowest id wins, so every run keeps the same row
val byId = Window.partitionBy("branch", "id").orderBy("id")
val samePoint = Window.partitionBy("branch", "cod_drv", "ts", "lat", "lon").orderBy("id")
val allPositions = backfill.unionByName(landed)
  .withColumn("r1", row_number().over(byId)).where($"r1" === 1).drop("r1")          // [defensive] arrived by both routes
  .withColumn("r2", row_number().over(samePoint)).where($"r2" === 1).drop("r2")     // [extract] same vehicle, minute and place stored twice
  .persist()
val nMerged = allPositions.count()

val gps = allPositions
  // [privacy] positions of drivers who are off shift (state 0) stay in the restricted raw zone only
  .where($"state" =!= 0)
  .select(
    col("branch").as("branch_code"),
    col("id").as("ping_id"),
    col("cod_drv"),
    to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss").as("received_ts"),
    col("lat"), col("lon"),
    // [extract] -1 means "no value"; the extract also holds speeds up to 58 m/s (208 km/h) in city traffic,
    // so anything above 150 km/h is treated as a GPS glitch: flagged, and the speed is not used
    when(col("speed") >= 0 && col("speed") <= 41.7, col("speed")).as("speed_ms"),
    (col("speed") > 41.7).as("dq_speed_glitch"),
    when(col("track") >= 0, col("track")).as("heading_deg"),
    (col("fix") === 1).as("has_fix"),
    col("quality"),
    col("state").as("state_id"),
    col("terminal").as("terminal_id"))
  .withColumn("dt", date_format($"received_ts", "yyyy-MM-dd"))
  .withColumn("speed_kmh", round($"speed_ms" * 3.6, 1))                      // [extract] the source unit is m/s
  // [extract] up to 13 positions share one receive minute; their order is known only from the id
  .withColumn("seq_in_minute", row_number().over(perMinute.orderBy("ping_id")))
  .withColumn("points_in_minute", count(lit(1)).over(perMinute))

// the zone grid covers the built-up area; "in the city" is the grid widened by half its size on every
// side, so that a car in a suburb is not counted as an intercity trip
val cityBox = zones.groupBy("zone_branch").agg(min("lat_min").as("s"), max("lat_max").as("n"), min("lon_min").as("w"), max("lon_max").as("e"))
  .select($"zone_branch".as("box_branch"), ($"s" - ($"n" - $"s") / 2).as("box_s"), ($"n" + ($"n" - $"s") / 2).as("box_n"),
          ($"w" - ($"e" - $"w") / 2).as("box_w"), ($"e" + ($"e" - $"w") / 2).as("box_e"))
val gpsZoned = gps.join(broadcast(zones),
    $"branch_code" === $"zone_branch" && $"lat" >= $"lat_min" && $"lat" < $"lat_max" && $"lon" >= $"lon_min" && $"lon" < $"lon_max", "left")
  .join(broadcast(cityBox), $"branch_code" === $"box_branch")
  .withColumn("in_city", $"lat".between($"box_s", $"box_n") && $"lon".between($"box_w", $"box_e"))
  .drop("zone_branch", "lat_min", "lat_max", "lon_min", "lon_max", "box_branch", "box_s", "box_n", "box_w", "box_e")

gpsZoned.repartition($"branch_code", $"dt")
  .write.mode("overwrite").partitionBy("branch_code", "dt").parquet(s"$CUR/gps_positions")

val curGps = spark.read.parquet(s"$CUR/gps_positions")
val curFiles = fs.globStatus(new Path(s"$CUR/gps_positions/*/*/*.parquet")).length
println(s"GPS  back-fill rows: $nBackfill   rows landed by the stream: $nLanded (in $streamFiles small files)   after merge and de-duplication: $nMerged")
println("gps: all positions per branch and vehicle state, before the off-shift ones are held back")
allPositions.groupBy("branch").pivot("state").count().orderBy("branch").show(false)
val nCur2 = curGps.count()
println(s"GPS  curated rows (on-shift positions only): $nCur2   off-shift positions kept in raw only: ${nMerged - nCur2}   curated files: $curFiles")
println("gps: data-quality summary per branch")
curGps.groupBy("branch_code").agg(
    count("*").as("positions"),
    countDistinct("cod_drv").as("vehicles"),
    sum((!$"has_fix").cast("int")).as("no_fix"),
    sum($"speed_ms".isNull.cast("int")).as("speed_missing"),
    sum(($"points_in_minute" > 1).cast("int")).as("in_multi_point_minutes"),
    sum((!$"in_city").cast("int")).as("outside_city"),
    sum($"dq_speed_glitch".cast("int")).as("speed_glitch"),
    round(max($"speed_kmh"), 1).as("max_kmh"))
  .orderBy("branch_code").show(false)

System.exit(0)
