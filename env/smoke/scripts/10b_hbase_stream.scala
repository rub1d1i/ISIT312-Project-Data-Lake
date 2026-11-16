// 10b - streaming write Kafka -> Spark Structured Streaming -> HBase with foreachBatch.
// Kafka values are CSV lines:  rowkey,name,city
// Run with:  spark-shell --master 'local[*]' -i 10b_hbase_stream.scala
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// The HBase write, kept in a serializable object so it can be shipped to executors.
object HBaseSink extends Serializable {
  def writePartition(rows: Iterator[Row]): Unit = {
    val conn  = ConnectionFactory.createConnection(HBaseConfiguration.create())
    val table = conn.getTable(TableName.valueOf("smoke_spark_hb"))
    try {
      rows.foreach { r =>
        val put = new Put(Bytes.toBytes(r.getAs[String]("rowkey")))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(r.getAs[String]("name")))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("city"), Bytes.toBytes(r.getAs[String]("city")))
        table.put(put)
      }
    } finally { table.close(); conn.close() }
  }
}

val raw = spark.readStream.format("kafka").
  option("kafka.bootstrap.servers", "localhost:9092").
  option("subscribe", "smoke_hb_stream").
  option("startingOffsets", "earliest").
  load()

// parse the CSV value into columns
val parsed = raw.selectExpr("CAST(value AS STRING) AS line").
  select(split($"line", ",").as("f")).
  select($"f".getItem(0).as("rowkey"), $"f".getItem(1).as("name"), $"f".getItem(2).as("city")).
  filter($"rowkey".isNotNull && $"name".isNotNull && $"city".isNotNull)

val q = parsed.writeStream.queryName("smoke_hbase").
  foreachBatch { (batch: DataFrame, batchId: Long) =>
    batch.persist()
    val n = batch.count()
    batch.rdd.foreachPartition(HBaseSink.writePartition _)
    println(s"### SMOKE10B batch $batchId: $n row(s) written to HBase")
    batch.unpersist()
  }.
  option("checkpointLocation", "hdfs://localhost:8020/user/bigdata/smoke_hb_stream/chk").
  trigger(Trigger.ProcessingTime("5 seconds")).start()

println("### STREAM STARTED: " + q.name)
q.awaitTermination(40000)
q.stop()
println("### active streams after stop: " + spark.streams.active.length)
System.exit(0)
