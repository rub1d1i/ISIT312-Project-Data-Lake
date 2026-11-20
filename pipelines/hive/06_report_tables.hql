-- ---------------------------------------------------------------------------
-- Report tables for the dashboard (reporting layer on top of the warehouse).
-- Small aggregated tables in the analytics zone, rebuilt by the nightly load,
-- so that a dashboard tile reads a few dozen rows and not a fact table.
--   rpt_monthly_revenue     from the warehouse (generated history of both branches)
--   rpt_orders_hour, rpt_price_km, rpt_fleet_hour
--                           from the REAL extract only (branch A, 16 Nov 2026)
-- Run: beeline -u jdbc:hive2://localhost:10000 -n bigdata -f 06_report_tables.hql
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS reports LOCATION '/lake/analytics/reports';
USE reports;

DROP TABLE IF EXISTS rpt_monthly_revenue;
CREATE TABLE rpt_monthly_revenue STORED AS PARQUET AS
SELECT b.city_label AS city, d.year_month, s.service_line,
       SUM(f.order_count) AS orders,
       SUM(IF(o.outcome_name = 'COMPLETED', 1, 0)) AS completed,
       SUM(IF(o.outcome_name LIKE 'CANCELLED%', 1, 0)) AS cancelled,
       ROUND(SUM(f.fare_amount), 0) AS revenue_rub
FROM dw.fact_orders f
JOIN dw.dim_branch b  ON f.branch_key = b.branch_key
JOIN dw.dim_date d    ON f.date_key = d.date_key
JOIN dw.dim_service s ON f.service_key = s.service_key
JOIN dw.dim_outcome o ON f.outcome_key = o.outcome_key
GROUP BY b.city_label, d.year_month, s.service_line;

DROP TABLE IF EXISTS rpt_orders_hour;
CREATE TABLE rpt_orders_hour STORED AS PARQUET AS
SELECT LPAD(CAST(order_hour AS STRING), 2, '0') AS order_hour,
       CASE WHEN order_hour < 6 THEN '00-05' WHEN order_hour < 9 THEN '06-08' ELSE '09-10' END AS hours,
       CASE channel WHEN 'MOBILE_APP' THEN 'Mobile app' WHEN 'CALL_CENTRE' THEN 'Call centre' ELSE 'Other' END AS channel,
       CASE WHEN outcome = 'COMPLETED' THEN 'Completed' WHEN outcome LIKE 'CANCELLED%' THEN 'Cancelled' ELSE 'Other' END AS outcome,
       COUNT(*) AS orders,
       SUM(IF(is_late, 1, 0)) AS flagged_no_car_in_time
FROM lake_curated.orders
WHERE branch_code = 'A' AND order_date = '2026-11-16'
GROUP BY LPAD(CAST(order_hour AS STRING), 2, '0'),
         CASE WHEN order_hour < 6 THEN '00-05' WHEN order_hour < 9 THEN '06-08' ELSE '09-10' END,
         CASE channel WHEN 'MOBILE_APP' THEN 'Mobile app' WHEN 'CALL_CENTRE' THEN 'Call centre' ELSE 'Other' END,
         CASE WHEN outcome = 'COMPLETED' THEN 'Completed' WHEN outcome LIKE 'CANCELLED%' THEN 'Cancelled' ELSE 'Other' END;

DROP TABLE IF EXISTS rpt_price_km;
CREATE TABLE rpt_price_km STORED AS PARQUET AS
SELECT trip_length, period, COUNT(*) AS trips,
       ROUND(percentile_approx(CAST(pay AS DOUBLE) / distance_km, 0.5), 1) AS median_rub_per_km
FROM (SELECT pay, distance_km,
             CASE WHEN distance_km <= 5 THEN '01-05 km' WHEN distance_km <= 10 THEN '05-10 km'
                  WHEN distance_km <= 20 THEN '10-20 km' ELSE '20+ km' END AS trip_length,
             CASE WHEN order_hour < 6 THEN 'Night 00-06' WHEN order_hour IN (7, 8) THEN 'Peak 07-09' END AS period
      FROM lake_curated.orders
      WHERE branch_code = 'A' AND order_date = '2026-11-16' AND cart = 1 AND outcome = 'COMPLETED' AND distance_km > 1) t
WHERE period IS NOT NULL
GROUP BY trip_length, period;

DROP TABLE IF EXISTS rpt_fleet_hour;
CREATE TABLE rpt_fleet_hour STORED AS PARQUET AS
SELECT LPAD(CAST(hour(received_ts) AS STRING), 2, '0') AS hour_of_day,
       COUNT(DISTINCT cod_drv) AS vehicles_on_shift,
       ROUND(SUM(IF(state_id = 2, 1, 0)) * 100.0 / COUNT(*), 1) AS on_order_pct
FROM lake_curated.gps_positions
WHERE branch_code = 'A' AND dt = '2026-11-16'
GROUP BY LPAD(CAST(hour(received_ts) AS STRING), 2, '0');

SELECT 'rpt_monthly_revenue' AS report_table, COUNT(*) AS row_count FROM rpt_monthly_revenue
UNION ALL SELECT 'rpt_orders_hour', COUNT(*) FROM rpt_orders_hour
UNION ALL SELECT 'rpt_price_km', COUNT(*) FROM rpt_price_km
UNION ALL SELECT 'rpt_fleet_hour', COUNT(*) FROM rpt_fleet_hour;
