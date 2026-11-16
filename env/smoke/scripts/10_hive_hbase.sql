DROP TABLE IF EXISTS smoke_hive_hbase;

-- Hive external table over the existing HBase table (nothing is copied)
CREATE EXTERNAL TABLE smoke_hive_hbase (rowkey STRING, name STRING, city STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:name,cf:city')
TBLPROPERTIES ('hbase.table.name' = 'smoke_spark_hb');

SELECT * FROM smoke_hive_hbase;

-- an aggregate, so the HBase table is also read by a MapReduce job on YARN
SELECT city, COUNT(*) AS people FROM smoke_hive_hbase GROUP BY city;
