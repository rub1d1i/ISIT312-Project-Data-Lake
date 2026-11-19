// ---------------------------------------------------------------------------
// CURATED -> ANALYTICS: dimensional warehouse (fact constellation).
//
// Facts : fact_orders, fact_fleet_activity, fact_calls, fact_market_hourly, fact_ledger
// Shared: dim_date, dim_time_of_day, dim_branch (all facts), dim_driver (orders, fleet)
//
// Surrogate keys: the two branches run independent systems whose own ids
// overlap (driver 10020 in branch A and in branch B are different people), so
// client, driver and operator get warehouse keys. A key, once given, is kept
// on the next load; only new members receive new keys.
//
// Lookups follow the ETL design: an order whose client or driver is not in
// the dimension is loaded with the UNKNOWN member (key -1), so totals still
// reconcile, and is also written to an exception file for correction.
//
// Output: Parquet under /lake/analytics/dw/<table>, described to Hive by
// pipelines/hive/03_dw_tables.hql.
//
// Run:  spark-shell --master yarn -i 30_dw_build.scala
// ---------------------------------------------------------------------------
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark.implicits._

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
spark.conf.set("spark.sql.shuffle.partitions", "8")

val CUR = "/lake/curated"
val DW = "/lake/analytics/dw"
val fs = FileSystem.get(sc.hadoopConfiguration)
def ref(t: String): DataFrame = spark.read.parquet(s"$CUR/reference/$t")
def dateKey(c: Column): Column = date_format(c, "yyyyMMdd").cast("int")

// write to a staging folder first, then swap: a dimension is read (old keys) and replaced in the same run
def save(df: DataFrame, name: String): Unit = {
  val staging = new Path(s"$DW/_staging/$name")
  val target = new Path(s"$DW/$name")
  val previous = new Path(s"$DW/_previous/$name")
  df.coalesce(1).write.mode("overwrite").parquet(staging.toString)
  fs.mkdirs(previous.getParent)
  fs.delete(previous, true)
  if (fs.exists(target)) fs.rename(target, previous)       // keep the old copy until the new one is in place
  fs.rename(staging, target)
  fs.delete(previous, true)
}

// keep the keys already given, number the new members after the highest existing key
def stableKeys(naturals: DataFrame, name: String, keyCol: String, nat: Seq[String]): DataFrame = {
  val path = new Path(s"$DW/$name")
  val byNat = Window.orderBy(nat.map(col): _*)
  if (!fs.exists(path)) naturals.withColumn(keyCol, row_number().over(byNat))
  else {
    val old = spark.read.parquet(path.toString).where(col(keyCol) > 0).select((keyCol +: nat).map(col): _*)
    val maxKey = old.agg(max(keyCol)).first.getInt(0)
    val known = naturals.join(old, nat)
    val fresh = naturals.join(old, nat, "left_anti").withColumn(keyCol, lit(maxKey) + row_number().over(byNat))
    println(s"$name: ${known.count()} members keep their key, ${fresh.count()} new members")
    known.unionByName(fresh)
  }
}

val orders = spark.read.parquet(s"$CUR/orders")
val payments = spark.read.parquet(s"$CUR/payments")
val gl = spark.read.parquet(s"$CUR/gl_entries")

// ============================ dimensions =====================================
val dimBranch = ref("branches").select($"branch_id".cast("int").as("branch_key"), $"branch_code", $"city_label", $"role".as("branch_role"))
save(dimBranch, "dim_branch")

val span = orders.agg(min("order_date"), max("order_date")).first
val dimDate = spark.sql(s"SELECT explode(sequence(to_date('${span.getDate(0)}'), to_date('${span.getDate(1)}'), interval 1 day)) AS full_date")
  .select(
    dateKey($"full_date").as("date_key"), $"full_date",
    dayofmonth($"full_date").as("day_of_month"),
    date_format($"full_date", "EEEE").as("day_name"),
    date_format($"full_date", "u").cast("int").as("day_of_week"),
    (date_format($"full_date", "u").cast("int") >= 6).as("is_weekend"),
    weekofyear($"full_date").as("week_of_year"),
    month($"full_date").as("month_no"),
    date_format($"full_date", "MMMM").as("month_name"),
    date_format($"full_date", "yyyy-MM").as("year_month"),
    quarter($"full_date").as("quarter_no"),
    year($"full_date").as("year_no"))
save(dimDate, "dim_date")

val dimTime = (0 to 23).toDF("hour_key")
  .withColumn("day_part",
    when($"hour_key" < 6, "NIGHT").when($"hour_key" < 10, "MORNING_PEAK").when($"hour_key" < 16, "DAYTIME")
      .when($"hour_key" < 20, "EVENING_PEAK").otherwise("LATE_EVENING"))
save(dimTime, "dim_time_of_day")

val dimZone = ref("zones").join(dimBranch, $"branch_id" === $"branch_key")
  .select($"zone_id".cast("int").as("zone_key"), $"zone_code", $"zone_type", $"branch_key", $"city_label")
  .union(Seq((0, "UNKNOWN", "UNKNOWN", 0, "UNKNOWN")).toDF("zone_key", "zone_code", "zone_type", "branch_key", "city_label"))
save(dimZone, "dim_zone")

save(ref("services").select($"cart".cast("int").as("service_key"), $"service_name", $"service_line", $"vehicle_class"), "dim_service")

save(Seq((1, "CALL_CENTRE", "VOICE"), (2, "MOBILE_APP", "DIGITAL"), (3, "WEBSITE", "DIGITAL"), (4, "CORPORATE_PORTAL", "DIGITAL"))
  .toDF("channel_key", "channel_name", "channel_group"), "dim_channel")

save(Seq((1, "COMPLETED", "FULFILLED"), (2, "CANCELLED_BEFORE_ASSIGNMENT", "CANCELLED"), (3, "CANCELLED_AFTER_ASSIGNMENT", "CANCELLED"),
         (4, "CLIENT_NO_SHOW", "NOT_FULFILLED"), (5, "NOT_FULFILLED", "NOT_FULFILLED"))
  .toDF("outcome_key", "outcome_name", "outcome_group"), "dim_outcome")

save(Seq((1, "CASH"), (2, "CARD"), (3, "CORP_INVOICE"), (4, "NONE")).toDF("payment_method_key", "payment_method"), "dim_payment_method")

val clientCols = Seq("client_key", "branch_key", "cod_cl", "client_type", "corp_account_id", "industry", "app_user")
val clientNat = ref("clients").join(ref("corp_accounts").select($"corp_account_id", $"industry"), Seq("corp_account_id"), "left")
  .select($"branch_id".cast("int").as("branch_key"), $"cod_cl".cast("long").as("cod_cl"), $"client_type",
          $"corp_account_id".cast("int").as("corp_account_id"), coalesce($"industry", lit("NOT_APPLICABLE")).as("industry"), ($"app_user" === 1).as("app_user"))
save(stableKeys(clientNat, "dim_client", "client_key", Seq("branch_key", "cod_cl")).select(clientCols.map(col): _*)
  .union(Seq((-1, 0, 0L, "UNKNOWN", 0, "UNKNOWN", false)).toDF(clientCols: _*)), "dim_client")

val driverCols = Seq("driver_key", "branch_key", "cod_drv", "employment_type", "vehicle_class", "hire_year", "car_id")
val driverNat = ref("drivers").select($"branch_id".cast("int").as("branch_key"), $"cod_drv".cast("int").as("cod_drv"), $"employment_type", $"vehicle_class",
                                      year($"hire_date").as("hire_year"), $"car_id".cast("int").as("car_id"))
save(stableKeys(driverNat, "dim_driver", "driver_key", Seq("branch_key", "cod_drv")).select(driverCols.map(col): _*)
  .union(Seq((0, 0, 0, "NOT_ASSIGNED", "NOT_ASSIGNED", 0, 0), (-1, 0, 0, "UNKNOWN", "UNKNOWN", 0, 0)).toDF(driverCols: _*)), "dim_driver")

val operCols = Seq("operator_key", "branch_key", "op_id", "operator_role", "operator_kind", "shift")
val operNat = ref("operators").select($"branch_id".cast("int").as("branch_key"), $"op_id".cast("int").as("op_id"), $"role".as("operator_role"),
                                      $"kind".as("operator_kind"), coalesce($"shift".cast("string"), lit("NOT_APPLICABLE")).as("shift"))
save(stableKeys(operNat, "dim_operator", "operator_key", Seq("branch_key", "op_id")).select(operCols.map(col): _*)
  .union(Seq((0, 0, 0, "NO_OPERATOR", "NONE", "NOT_APPLICABLE"), (-1, 0, 0, "UNKNOWN", "UNKNOWN", "NOT_APPLICABLE")).toDF(operCols: _*)), "dim_operator")

val itemNat = gl.select("account", "item", "item_group").distinct()
save(stableKeys(itemNat, "dim_ledger_item", "ledger_item_key", Seq("account", "item")).select("ledger_item_key", "account", "item", "item_group"), "dim_ledger_item")

// re-read what was written so every fact uses exactly the stored keys
def dim(name: String): DataFrame = spark.read.parquet(s"$DW/$name")
val dBranch = dim("dim_branch").select("branch_key", "branch_code")
val dClient = dim("dim_client").where($"client_key" > 0).select($"client_key", $"branch_key".as("c_branch"), $"cod_cl".as("c_cod"))
val dDriver = dim("dim_driver").where($"driver_key" > 0).select($"driver_key", $"branch_key".as("d_branch"), $"cod_drv".as("d_cod"), $"employment_type".as("d_employment"))
val dOper = dim("dim_operator").where($"operator_key" > 0).select($"operator_key", $"branch_key".as("o_branch"), $"op_id".as("o_id"))
val addr = ref("addresses").select($"branch_id".cast("int").as("a_branch"), $"addr_id".cast("int").as("a_id"), $"zone_id".cast("int").as("a_zone"))

// ============================ fact_orders =====================================
val pay = payments.select($"branch_code".as("p_branch"), $"order_id".as("p_order"), $"commission_amount", $"driver_payout")
val looked = orders.join(broadcast(dBranch), Seq("branch_code"))
  .join(dClient, $"branch_key" === $"c_branch" && $"cod_cl" === $"c_cod", "left")
  .join(dDriver, $"branch_key" === $"d_branch" && $"cod_drv" === $"d_cod", "left")
  .join(broadcast(addr.select($"a_branch".as("f_branch"), $"a_id".as("f_id"), $"a_zone".as("pickup_zone"))), $"branch_key" === $"f_branch" && $"addr_from" === $"f_id", "left")
  .join(broadcast(addr.select($"a_branch".as("t_branch"), $"a_id".as("t_id"), $"a_zone".as("dropoff_zone"))), $"branch_key" === $"t_branch" && $"addr_to" === $"t_id", "left")
  .join(broadcast(dim("dim_channel")), $"channel" === $"channel_name", "left")
  .join(broadcast(dim("dim_outcome")), $"outcome" === $"outcome_name", "left")
  .join(broadcast(dim("dim_payment_method")), Seq("payment_method"), "left")
  .join(pay, $"branch_code" === $"p_branch" && $"order_id" === $"p_order", "left")
  .cache()

val notFound = $"client_key".isNull || ($"cod_drv".isNotNull && $"driver_key".isNull)
val exceptions = looked.where(notFound).select($"branch_code", $"order_id", $"cod_cl", $"cod_drv", $"accepted_ts",
  when($"client_key".isNull, "client not in dimension").otherwise("driver not in dimension").as("problem"))
exceptions.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$CUR/_exceptions/fact_orders_unknown_keys")

val completed = $"outcome" === "COMPLETED"
val factOrders = looked.select(
  dateKey($"order_date").as("date_key"), $"order_hour".as("hour_key"), $"branch_key",
  coalesce($"pickup_zone", lit(0)).as("pickup_zone_key"), coalesce($"dropoff_zone", lit(0)).as("dropoff_zone_key"),
  $"cart".as("service_key"), $"channel_key", coalesce($"client_key", lit(-1)).as("client_key"),
  when($"cod_drv".isNull, 0).otherwise(coalesce($"driver_key", lit(-1))).as("driver_key"),
  $"outcome_key", $"payment_method_key", $"order_id",
  lit(1).as("order_count"),
  when(completed, $"pay").as("fare_amount"),                                   // what the client paid (gross)
  when(completed, $"vat_amount").as("vat_amount"),
  when(completed, $"commission_amount").as("commission_amount"),               // the company's share of a contractor trip
  when(completed, $"driver_payout").as("driver_payout"),
  when(completed, when($"d_employment" === "OWN_FLEET", $"pay").otherwise($"commission_amount")).as("company_revenue"),
  $"quoted_pay".as("quoted_amount"), $"distance_km",
  round($"assign_min", 1).as("assign_minutes"), round($"arrive_min", 1).as("arrival_minutes"),
  round($"wait_min", 1).as("wait_minutes"), round($"trip_min", 1).as("trip_minutes"),
  $"is_late".cast("int").as("late_count"), $"is_preorder".cast("int").as("preorder_count"),
  $"branch_code", $"order_month")
factOrders.repartition($"branch_code", $"order_month")
  .write.mode("overwrite").partitionBy("branch_code", "order_month").parquet(s"$DW/fact_orders")

// ============================ fact_fleet_activity ==============================
// grain: one driver, one hour of one day, one zone (on-shift positions only; zone 0 = outside the zone grid)
val gps = spark.read.parquet(s"$CUR/gps_positions")
val activity = gps.groupBy($"branch_code", $"cod_drv", to_date($"received_ts").as("d"), hour($"received_ts").as("hour_key"),
                           coalesce($"zone_id", lit(0)).as("zone_key"))
  .agg(
    count(lit(1)).as("positions"),
    countDistinct(when($"state_id" === 1, $"received_ts")).as("minutes_free"),
    countDistinct(when($"state_id" === 2, $"received_ts")).as("minutes_on_order"),
    countDistinct(when($"state_id".isin(3, 6), $"received_ts")).as("minutes_other"),
    round(avg(when($"state_id" === 2 && $"speed_kmh" > 0, $"speed_kmh")), 1).as("avg_speed_on_order_kmh"),
    sum((!$"in_city").cast("int")).as("positions_outside_city"))
val factFleet = activity.join(broadcast(dBranch), Seq("branch_code"))
  .join(dDriver, $"branch_key" === $"d_branch" && $"cod_drv" === $"d_cod", "left")
  .select(dateKey($"d").as("date_key"), $"hour_key", $"branch_key", coalesce($"driver_key", lit(-1)).as("driver_key"), $"zone_key".cast("int"),
          $"positions".cast("int"), $"minutes_free".cast("int"), $"minutes_on_order".cast("int"), $"minutes_other".cast("int"),
          $"avg_speed_on_order_kmh", $"positions_outside_city".cast("int"))
save(factFleet, "fact_fleet_activity")

// ============================ fact_calls =======================================
val calls = spark.read.parquet(s"$CUR/calls")
val factCalls = calls.join(broadcast(dBranch), Seq("branch_code"))
  .join(broadcast(dOper), $"op_id" === $"o_id" && ($"o_branch" === $"branch_key" || $"o_branch" === 0), "left")
  .select(dateKey($"call_date").as("date_key"), hour($"call_ts").as("hour_key"), $"branch_key",
          when($"op_id".isNull, 0).otherwise(coalesce($"operator_key", lit(-1))).as("operator_key"),
          $"call_type", $"direction", $"disposition", $"recording_uid", $"order_id",
          lit(1).as("call_count"), $"answered".cast("int").as("answered_count"), $"talk_s".as("talk_seconds"),
          $"queue_wait_s".as("queue_wait_seconds"), $"duration_s".as("duration_seconds"), $"branch_code", $"call_month")
factCalls.repartition($"branch_code", $"call_month")
  .write.mode("overwrite").partitionBy("branch_code", "call_month").parquet(s"$DW/fact_calls")

// ============================ fact_market_hourly ================================
// what the company prices against: competitor prices and the traffic index, per city and hour
val comp = spark.read.parquet(s"$CUR/competitor_prices")
val traffic = spark.read.parquet(s"$CUR/traffic_index")
  .select($"branch_code", to_date($"obs_ts").as("d"), hour($"obs_ts").as("hour_key"), $"traffic_index")
val factMarket = comp.groupBy($"branch_code", to_date($"obs_ts").as("d"), hour($"obs_ts").as("hour_key"))
  .agg(round(avg(when($"route_class" === "SHORT_3KM", $"price")), 0).as("competitor_price_3km"),
       round(avg(when($"route_class" === "MEDIUM_8KM", $"price")), 0).as("competitor_price_8km"),
       round(avg(when($"route_class" === "LONG_20KM", $"price")), 0).as("competitor_price_20km"),
       round(avg($"surge"), 2).as("competitor_surge"))
  .join(traffic, Seq("branch_code", "d", "hour_key"), "left")
  .join(broadcast(dBranch), Seq("branch_code"))
  .select(dateKey($"d").as("date_key"), $"hour_key", $"branch_key", $"competitor_price_3km", $"competitor_price_8km", $"competitor_price_20km",
          $"competitor_surge", $"traffic_index")
save(factMarket, "fact_market_hourly")

// ============================ fact_ledger ======================================
val factLedger = gl.join(dim("dim_ledger_item"), Seq("account", "item"))
  .join(broadcast(dBranch), Seq("branch_code"))
  .select(dateKey($"doc_date").as("date_key"), $"branch_key", $"ledger_item_key", $"amount")
save(factLedger, "fact_ledger")

// ============================ load report =======================================
println("warehouse load report")
(Seq("dim_branch", "dim_date", "dim_time_of_day", "dim_zone", "dim_service", "dim_channel", "dim_outcome", "dim_payment_method",
     "dim_client", "dim_driver", "dim_operator", "dim_ledger_item", "fact_orders", "fact_fleet_activity", "fact_calls", "fact_market_hourly", "fact_ledger")
  .map(t => (t, spark.read.parquet(s"$DW/$t").count())) :+ (("orders loaded with an UNKNOWN key (also in the exception file)", exceptions.count())))
  .toDF("table", "rows").show(20, false)

println("control totals: what clients paid, in the fact table, in the payment system and in the accounting export")
val f = spark.read.parquet(s"$DW/fact_orders").groupBy("branch_code").agg(sum("order_count").as("orders"), sum("fare_amount").as("fact_fare_amount"))
val p = payments.groupBy("branch_code").agg(sum("amount").as("payments_amount"))
val g = gl.where($"item_group" === "REVENUE").groupBy("branch_code").agg(sum("amount").as("ledger_revenue"))
f.join(p, "branch_code").join(g, "branch_code")
  .withColumn("fact_minus_payments", $"fact_fare_amount" - $"payments_amount")
  .withColumn("fact_minus_ledger", $"fact_fare_amount" - $"ledger_revenue")
  .orderBy("branch_code").show(false)

System.exit(0)
