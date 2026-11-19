-- ---------------------------------------------------------------------------
-- ANALYTICS zone: the dimensional warehouse (fact constellation: five facts
-- sharing dimensions) as Hive external tables over the Parquet written by
-- pipelines/spark/30_dw_build.scala.
-- Fact tables are partitioned by branch and month: a query for one branch and
-- one month reads one folder (partition pruning).
-- Run: beeline -u jdbc:hive2://localhost:10000 -n bigdata -f 03_dw_tables.hql
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS dw COMMENT 'analytics zone - dimensional warehouse' LOCATION '/lake/analytics/_hive';
USE dw;

DROP TABLE IF EXISTS dim_branch;
CREATE EXTERNAL TABLE dim_branch (branch_key INT, branch_code STRING, city_label STRING, branch_role STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_branch';

DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date (date_key INT, full_date DATE, day_of_month INT, day_name STRING, day_of_week INT, is_weekend BOOLEAN,
  week_of_year INT, month_no INT, month_name STRING, year_month STRING, quarter_no INT, year_no INT)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_date';

DROP TABLE IF EXISTS dim_time_of_day;
CREATE EXTERNAL TABLE dim_time_of_day (hour_key INT, day_part STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_time_of_day';

DROP TABLE IF EXISTS dim_zone;
CREATE EXTERNAL TABLE dim_zone (zone_key INT, zone_code STRING, zone_type STRING, branch_key INT, city_label STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_zone';

DROP TABLE IF EXISTS dim_service;
CREATE EXTERNAL TABLE dim_service (service_key INT, service_name STRING, service_line STRING, vehicle_class STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_service';

DROP TABLE IF EXISTS dim_channel;
CREATE EXTERNAL TABLE dim_channel (channel_key INT, channel_name STRING, channel_group STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_channel';

DROP TABLE IF EXISTS dim_outcome;
CREATE EXTERNAL TABLE dim_outcome (outcome_key INT, outcome_name STRING, outcome_group STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_outcome';

DROP TABLE IF EXISTS dim_payment_method;
CREATE EXTERNAL TABLE dim_payment_method (payment_method_key INT, payment_method STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_payment_method';

DROP TABLE IF EXISTS dim_client;
CREATE EXTERNAL TABLE dim_client (client_key INT, branch_key INT, cod_cl BIGINT, client_type STRING, corp_account_id INT, industry STRING, app_user BOOLEAN)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_client';

DROP TABLE IF EXISTS dim_driver;
CREATE EXTERNAL TABLE dim_driver (driver_key INT, branch_key INT, cod_drv INT, employment_type STRING, vehicle_class STRING, hire_year INT, car_id INT)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_driver';

DROP TABLE IF EXISTS dim_operator;
CREATE EXTERNAL TABLE dim_operator (operator_key INT, branch_key INT, op_id INT, operator_role STRING, operator_kind STRING, shift STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_operator';

DROP TABLE IF EXISTS fact_orders;
CREATE EXTERNAL TABLE fact_orders (
  date_key INT, hour_key INT, branch_key INT, pickup_zone_key INT, dropoff_zone_key INT, service_key INT, channel_key INT,
  client_key INT, driver_key INT, outcome_key INT, payment_method_key INT, order_id BIGINT,
  order_count INT, fare_amount DECIMAL(12,2), vat_amount DECIMAL(12,2), commission_amount DECIMAL(12,2), driver_payout DECIMAL(12,2),
  company_revenue DECIMAL(12,2), quoted_amount DECIMAL(12,2), distance_km DOUBLE,
  assign_minutes DOUBLE, arrival_minutes DOUBLE, wait_minutes DOUBLE, trip_minutes DOUBLE, late_count INT, preorder_count INT
)
PARTITIONED BY (branch_code STRING, order_month STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/fact_orders';
MSCK REPAIR TABLE fact_orders;

DROP TABLE IF EXISTS fact_fleet_activity;
CREATE EXTERNAL TABLE fact_fleet_activity (
  date_key INT, hour_key INT, branch_key INT, driver_key INT, zone_key INT, positions INT, minutes_free INT, minutes_on_order INT,
  minutes_other INT, avg_speed_on_order_kmh DOUBLE, positions_outside_city INT
)
STORED AS PARQUET LOCATION '/lake/analytics/dw/fact_fleet_activity';

DROP TABLE IF EXISTS fact_calls;
CREATE EXTERNAL TABLE fact_calls (
  date_key INT, hour_key INT, branch_key INT, operator_key INT, call_type STRING, direction STRING, disposition STRING,
  recording_uid STRING, order_id BIGINT, call_count INT, answered_count INT, talk_seconds INT, queue_wait_seconds INT, duration_seconds INT
)
PARTITIONED BY (branch_code STRING, call_month STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/fact_calls';
MSCK REPAIR TABLE fact_calls;

DROP TABLE IF EXISTS dim_ledger_item;
CREATE EXTERNAL TABLE dim_ledger_item (ledger_item_key INT, account STRING, item STRING, item_group STRING)
STORED AS PARQUET LOCATION '/lake/analytics/dw/dim_ledger_item';

DROP TABLE IF EXISTS fact_market_hourly;
CREATE EXTERNAL TABLE fact_market_hourly (
  date_key INT, hour_key INT, branch_key INT, competitor_price_3km DOUBLE, competitor_price_8km DOUBLE, competitor_price_20km DOUBLE,
  competitor_surge DOUBLE, traffic_index DOUBLE
)
STORED AS PARQUET LOCATION '/lake/analytics/dw/fact_market_hourly';

DROP TABLE IF EXISTS fact_ledger;
CREATE EXTERNAL TABLE fact_ledger (date_key INT, branch_key INT, ledger_item_key INT, amount DECIMAL(14,2))
STORED AS PARQUET LOCATION '/lake/analytics/dw/fact_ledger';

SHOW TABLES;
SHOW PARTITIONS fact_orders;
