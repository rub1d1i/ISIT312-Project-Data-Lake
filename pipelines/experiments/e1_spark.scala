// Experiment E1, Spark part: the report on the input named by the environment
// variables E1_INPUT (HDFS folder) and E1_FORMAT (csv or parquet).
// Called by e1_engines.sh, which times the whole spark-shell run; the time of
// the query alone is printed as well.
import org.apache.spark.sql.functions._
import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")

val input = sys.env("E1_INPUT")
val t0 = System.nanoTime
val rows = if (sys.env("E1_FORMAT") == "csv")
  spark.read.csv(input)                                    // no header in the merged files: columns are _c0 .. _c94
    .groupBy($"branch", substring($"_c10", 1, 10).as("day"), $"_c17".as("status"))
    .agg(count(lit(1)).as("orders"), round(sum($"_c68".cast("double")), 2).as("quoted"))
    .collect().length
else
  spark.read.parquet(input)
    .groupBy("branch_code", "order_date", "status")
    .agg(count(lit(1)).as("orders"), sum("quoted_pay").as("quoted"))
    .collect().length
val seconds = (System.nanoTime - t0) / 1e9
println(f"E1_SPARK rows=$rows   query alone $seconds%.1f s")
System.exit(0)
