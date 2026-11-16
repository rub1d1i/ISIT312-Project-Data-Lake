// 10a - batch write Spark -> HBase with the HBase client API inside foreachPartition.
// Run with:  spark-shell --master yarn -i 10a_hbase_batch.scala      (or --master 'local[*]')
// No --jars needed: the HBase client jars are on the driver/executor classpath (spark-defaults.conf).
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

println(s"### master=${sc.master}")
val people = Seq(("b1", "alice", "Dubai"), ("b2", "bob", "Sydney"), ("b3", "carol", "Wollongong"), ("b4", "dave", "Dubai")).
  toDF("rowkey", "name", "city").repartition(2)

// One HBase connection per partition (connections are not serializable, so they are created
// on the executor, inside the function), one Put per row.
people.rdd.foreachPartition { rows =>
  val conn  = ConnectionFactory.createConnection(HBaseConfiguration.create())   // reads hbase-site.xml
  val table = conn.getTable(TableName.valueOf("smoke_spark_hb"))
  try {
    rows.foreach { r =>
      val put = new Put(Bytes.toBytes(r.getString(0)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(r.getString(1)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("city"), Bytes.toBytes(r.getString(2)))
      table.put(put)
    }
  } finally { table.close(); conn.close() }
}
println("### SMOKE10A batch rows sent to HBase: " + people.count)
System.exit(0)
