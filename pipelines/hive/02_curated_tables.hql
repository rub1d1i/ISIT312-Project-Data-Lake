-- ---------------------------------------------------------------------------
-- CURATED zone: Hive external tables over the Parquet files written by the
-- Spark curation jobs (pipelines/spark/20, 21, 22). Spark writes the files,
-- the Hive metastore describes them, so Beeline, Spark SQL and any JDBC tool
-- see the same tables.
-- Run: beeline -u jdbc:hive2://localhost:10000 -n bigdata -f 02_curated_tables.hql
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS lake_curated COMMENT 'curated zone - cleaned, typed, Parquet' LOCATION '/lake/curated/_hive';
USE lake_curated;

DROP TABLE IF EXISTS orders;
CREATE EXTERNAL TABLE orders (
  order_id BIGINT, cod_cl BIGINT, cod_drv INT, op_prn INT, op_otp INT, addr_from INT, addr_to INT, stop_1 INT,
  cart INT, status INT, reazon INT, intake_line INT, intake_letter STRING,
  pay DECIMAL(12,2), quoted_pay DECIMAL(12,2), distance_km DOUBLE, beznal INT, card_payment INT, intercity INT, promised_minutes INT,
  accepted_ts TIMESTAMP, requested_ts TIMESTAMP, assigned_ts TIMESTAMP, arrived_ts TIMESTAMP, pickup_ts TIMESTAMP, completed_ts TIMESTAMP, late_flag_ts TIMESTAMP,
  entrance_raw STRING, comment_category STRING, recording_ref_raw STRING, app_pickup_lat DOUBLE, app_pickup_lon DOUBLE, source_dt STRING,
  recording_uid STRING, entrance_no INT, private_house BOOLEAN, channel STRING, client_segment STRING, vat_amount DECIMAL(12,2),
  outcome STRING, payment_method STRING, is_preorder BOOLEAN,
  dq_clock_skew BOOLEAN, assign_min DOUBLE, arrive_min DOUBLE, wait_min DOUBLE, trip_min DOUBLE, is_late BOOLEAN,
  dq_zero_distance BOOLEAN, dq_price_changed BOOLEAN, order_date DATE, order_hour INT
)
PARTITIONED BY (branch_code STRING, order_month STRING)
STORED AS PARQUET
LOCATION '/lake/curated/orders';
MSCK REPAIR TABLE orders;

DROP TABLE IF EXISTS gps_positions;
CREATE EXTERNAL TABLE gps_positions (
  ping_id BIGINT, cod_drv INT, received_ts TIMESTAMP, lat DOUBLE, lon DOUBLE, speed_ms DOUBLE, dq_speed_glitch BOOLEAN, heading_deg DOUBLE, has_fix BOOLEAN,
  quality INT, state_id INT, terminal_id BIGINT, speed_kmh DOUBLE, seq_in_minute INT, points_in_minute BIGINT, zone_id INT, in_city BOOLEAN
)
PARTITIONED BY (branch_code STRING, dt STRING)
STORED AS PARQUET
LOCATION '/lake/curated/gps_positions';
MSCK REPAIR TABLE gps_positions;

DROP TABLE IF EXISTS calls;
CREATE EXTERNAL TABLE calls (
  recording_uid STRING, call_ts TIMESTAMP, src_hash STRING, queue STRING, direction STRING, call_type STRING, op_id INT,
  duration_s INT, talk_s INT, disposition STRING, queue_wait_s INT, recording_file STRING, order_id BIGINT, answered BOOLEAN, call_date DATE
)
PARTITIONED BY (branch_code STRING, call_month STRING)
STORED AS PARQUET
LOCATION '/lake/curated/calls';
MSCK REPAIR TABLE calls;

DROP TABLE IF EXISTS payments;
CREATE EXTERNAL TABLE payments (
  payment_id BIGINT, order_id BIGINT, paid_ts TIMESTAMP, amount DECIMAL(12,2), currency STRING, method STRING, cod_drv INT,
  commission_amount DECIMAL(12,2), driver_payout DECIMAL(12,2), state STRING
)
PARTITIONED BY (branch_code STRING, pay_month STRING)
STORED AS PARQUET
LOCATION '/lake/curated/payments';
MSCK REPAIR TABLE payments;

DROP TABLE IF EXISTS app_events;
CREATE EXTERNAL TABLE app_events (
  event_id STRING, event_ts TIMESTAMP, event STRING, session_id STRING, cod_cl BIGINT, branch_code STRING, platform STRING, app_version STRING,
  device_model STRING, os_version STRING, lat DOUBLE, lon DOUBLE, accuracy_m BIGINT, order_id BIGINT, quoted_price DOUBLE, surge DOUBLE,
  cart INT, from_zone_id INT, payment STRING, stars INT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/lake/curated/app_events';
MSCK REPAIR TABLE app_events;

DROP TABLE IF EXISTS gl_entries;
CREATE EXTERNAL TABLE gl_entries (
  doc_date DATE, branch_code STRING, account STRING, item STRING, amount DECIMAL(14,2), currency STRING, posting STRING, item_group STRING
)
STORED AS PARQUET
LOCATION '/lake/curated/gl_entries';

DROP TABLE IF EXISTS audio_index;
CREATE EXTERNAL TABLE audio_index (
  order_id BIGINT, recording_uid STRING, file_name STRING, size_bytes BIGINT, container STRING, channels INT, sample_rate_hz INT, bits_per_sample INT,
  duration_s DOUBLE, offset_bytes BIGINT, call_ts TIMESTAMP, op_id INT, call_type STRING, talk_s INT, channel STRING, outcome STRING, cod_cl BIGINT, cod_drv INT,
  branch_code STRING, dt STRING, stored_in STRING
)
STORED AS PARQUET
LOCATION '/lake/curated/media/audio_index';

DROP TABLE IF EXISTS video_index;
CREATE EXTERNAL TABLE video_index (
  file_name STRING, camera_id INT, video_type STRING, size_bytes BIGINT, block_size BIGINT, hdfs_blocks INT, replication INT, hdfs_path STRING,
  start_ts STRING, duration_s INT, width INT, height INT, fps INT, location STRING, request_id STRING, request_reason STRING, car_id INT, order_id BIGINT,
  branch_code STRING, dt STRING
)
STORED AS PARQUET
LOCATION '/lake/curated/media/video_index';

SHOW TABLES;
SELECT branch_code, order_month, COUNT(*) AS orders FROM orders GROUP BY branch_code, order_month ORDER BY branch_code, order_month;
