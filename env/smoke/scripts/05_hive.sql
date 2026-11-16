DROP TABLE IF EXISTS smoke_sales;

-- external, partitioned table over an HDFS folder
CREATE EXTERNAL TABLE smoke_sales (id INT, product STRING, amount DOUBLE)
PARTITIONED BY (region STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/bigdata/smoke_hive/sales';

LOAD DATA INPATH '/user/bigdata/smoke_hive/staging/east.csv' INTO TABLE smoke_sales PARTITION (region='east');
LOAD DATA INPATH '/user/bigdata/smoke_hive/staging/west.csv' INTO TABLE smoke_sales PARTITION (region='west');

SHOW PARTITIONS smoke_sales;
SELECT * FROM smoke_sales;

-- GROUP BY ... WITH ROLLUP (runs as a MapReduce job on YARN)
SELECT region, product, SUM(amount) AS total
FROM smoke_sales
GROUP BY region, product WITH ROLLUP;

-- window functions (runs as a MapReduce job on YARN)
SELECT id, region, product, amount,
       RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk,
       SUM(amount) OVER (PARTITION BY region) AS region_total
FROM smoke_sales;
