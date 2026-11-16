// 08 - Spark SQL <-> Hive metastore. Run with:  spark-shell --master <local[*]|yarn> -i 08_spark_hive.scala
// SMOKE_SUFFIX (environment) keeps the local and the YARN run's tables apart.
import org.apache.spark.sql.functions._

val suffix = sys.env.getOrElse("SMOKE_SUFFIX", "local")
val target = "smoke_spark_agg_" + suffix
println(s"### spark=${spark.version} scala=${util.Properties.versionNumberString} master=${sc.master}")
println("### catalog=" + spark.conf.get("spark.sql.catalogImplementation") +
        " hive.metastore.version=" + spark.conf.get("spark.sql.hive.metastore.version") +
        " defaultFS=" + sc.hadoopConfiguration.get("fs.defaultFS"))

// 1. CSV from HDFS -> DataFrame (relative path = /user/bigdata/... on HDFS)
val emp = spark.read.option("header", "true").option("inferSchema", "true").csv("smoke_spark/emp.csv")
emp.printSchema()
emp.show()

// 2. aggregate
val agg = emp.groupBy("dept").agg(count("*").as("n"), sum("salary").as("total_salary"), avg("salary").as("avg_salary")).orderBy("dept")
agg.show()

// 3. save as a Hive table (managed, Parquet) - visible in Beeline afterwards
agg.write.mode("overwrite").saveAsTable(target)
spark.sql("SHOW TABLES LIKE 'smoke_*'").show(false)

// 4. read a table that was created in Beeline
val dept = spark.table("smoke_beeline_dept")
dept.show()
spark.sql(s"SELECT a.dept, a.n, a.total_salary, d.location FROM $target a JOIN smoke_beeline_dept d ON a.dept = d.dept ORDER BY a.dept").show()

println("### SMOKE08 rows_written=" + spark.table(target).count + " dept_rows=" + dept.count + " suffix=" + suffix)
System.exit(0)
