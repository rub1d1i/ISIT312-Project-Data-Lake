-- ---------------------------------------------------------------------------
-- Analytical (OLAP) queries on the warehouse. Each one answers a question the
-- company asks today by exporting data and assembling the report by hand.
-- Run: beeline -u jdbc:hive2://localhost:10000 -n bigdata -f 04_olap_queries.hql
-- ---------------------------------------------------------------------------
USE dw;
SET hive.auto.convert.join=true;

-- Q1  ROLLUP: revenue and completed orders by city, month and service line,
--     with subtotals per month, per city and a grand total (monthly management report).
SELECT b.city_label, d.year_month, s.service_line,
       SUM(f.order_count)            AS orders,
       SUM(IF(o.outcome_name = 'COMPLETED', 1, 0)) AS completed,
       ROUND(SUM(f.fare_amount), 0)  AS revenue_rub
FROM fact_orders f
JOIN dim_branch b  ON f.branch_key = b.branch_key
JOIN dim_date d    ON f.date_key = d.date_key
JOIN dim_service s ON f.service_key = s.service_key
JOIN dim_outcome o ON f.outcome_key = o.outcome_key
GROUP BY b.city_label, d.year_month, s.service_line WITH ROLLUP
ORDER BY b.city_label, d.year_month, s.service_line;

-- Q2  CUBE: where are orders lost? Every combination of intake channel and outcome group.
SELECT c.channel_name, o.outcome_group, SUM(f.order_count) AS orders
FROM fact_orders f
JOIN dim_channel c ON f.channel_key = c.channel_key
JOIN dim_outcome o ON f.outcome_key = o.outcome_key
GROUP BY c.channel_name, o.outcome_group WITH CUBE
ORDER BY c.channel_name, o.outcome_group;

-- Q3  GROUPING SETS: demand by city and part of day, by city only, and overall (demand planning).
SELECT b.city_label, t.day_part, SUM(f.order_count) AS orders,
       ROUND(AVG(f.arrival_minutes), 1) AS avg_arrival_min
FROM fact_orders f
JOIN dim_branch b      ON f.branch_key = b.branch_key
JOIN dim_time_of_day t ON f.hour_key = t.hour_key
GROUP BY b.city_label, t.day_part
GROUPING SETS ((b.city_label, t.day_part), (b.city_label), ())
ORDER BY b.city_label, t.day_part;

-- Q4  Window functions: daily revenue per city with a 7-day moving average and the change against the previous day.
SELECT city_label, full_date, revenue_rub,
       ROUND(AVG(revenue_rub) OVER (PARTITION BY city_label ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS avg_7_days,
       revenue_rub - LAG(revenue_rub) OVER (PARTITION BY city_label ORDER BY full_date) AS change_vs_previous_day
FROM (
  SELECT b.city_label, d.full_date, ROUND(SUM(f.fare_amount), 0) AS revenue_rub
  FROM fact_orders f
  JOIN dim_branch b ON f.branch_key = b.branch_key
  JOIN dim_date d   ON f.date_key = d.date_key
  WHERE f.order_month = '2026-11'
  GROUP BY b.city_label, d.full_date
) daily
ORDER BY city_label, full_date;

-- Q5  RANK: the five busiest pickup zones of each city (where to station free cars).
SELECT city_label, zone_code, zone_type, completed_orders, zone_rank
FROM (
  SELECT z.city_label, z.zone_code, z.zone_type, COUNT(*) AS completed_orders,
         RANK() OVER (PARTITION BY z.city_label ORDER BY COUNT(*) DESC) AS zone_rank
  FROM fact_orders f
  JOIN dim_zone z    ON f.pickup_zone_key = z.zone_key
  JOIN dim_outcome o ON f.outcome_key = o.outcome_key
  WHERE o.outcome_name = 'COMPLETED'
  GROUP BY z.city_label, z.zone_code, z.zone_type
) ranked
WHERE zone_rank <= 5
ORDER BY city_label, zone_rank;

-- Q6  Drill-across two fact tables: orders against available driver time, hour by hour, on the day of the extract.
SELECT o.city_label, o.hour_key, o.orders, o.completed, s.drivers_on_line, s.free_minutes, s.busy_minutes,
       ROUND(s.busy_minutes * 100.0 / (s.busy_minutes + s.free_minutes), 1) AS utilisation_pct
FROM (
  SELECT b.city_label, f.branch_key, f.hour_key, SUM(f.order_count) AS orders,
         SUM(IF(f.outcome_key = 1, 1, 0)) AS completed
  FROM fact_orders f JOIN dim_branch b ON f.branch_key = b.branch_key
  WHERE f.date_key = 20261116
  GROUP BY b.city_label, f.branch_key, f.hour_key
) o
JOIN (
  SELECT branch_key, hour_key, COUNT(DISTINCT driver_key) AS drivers_on_line,
         SUM(minutes_free) AS free_minutes, SUM(minutes_on_order) AS busy_minutes
  FROM fact_fleet_activity
  WHERE date_key = 20261116
  GROUP BY branch_key, hour_key
) s ON o.branch_key = s.branch_key AND o.hour_key = s.hour_key
ORDER BY o.city_label, o.hour_key;

-- Q7  Slice + dice: is an order flagged "no car by the promised time" lost more often? Call-centre orders in November.
SELECT b.city_label, IF(f.late_count = 1, 'NO CAR IN TIME', 'NOT FLAGGED') AS arrival, SUM(f.order_count) AS orders,
       ROUND(SUM(IF(o.outcome_group = 'CANCELLED', 1, 0)) * 100.0 / SUM(f.order_count), 1) AS cancelled_pct
FROM fact_orders f
JOIN dim_branch b  ON f.branch_key = b.branch_key
JOIN dim_channel c ON f.channel_key = c.channel_key
JOIN dim_outcome o ON f.outcome_key = o.outcome_key
WHERE f.order_month = '2026-11' AND c.channel_name = 'CALL_CENTRE'
GROUP BY b.city_label, IF(f.late_count = 1, 'NO CAR IN TIME', 'NOT FLAGGED')
ORDER BY b.city_label, arrival;

-- Q8  Call-centre load: calls, share answered and queue wait per part of day (uses the calls fact).
SELECT b.city_label, t.day_part, SUM(c.call_count) AS calls,
       ROUND(SUM(c.answered_count) * 100.0 / SUM(c.call_count), 1) AS answered_pct,
       ROUND(AVG(c.queue_wait_seconds), 1) AS avg_queue_wait_s,
       ROUND(SUM(c.talk_seconds) / 3600.0, 0) AS talk_hours
FROM fact_calls c
JOIN dim_branch b      ON c.branch_key = b.branch_key
JOIN dim_time_of_day t ON c.hour_key = t.hour_key
WHERE c.call_type IN ('ORDER_INTAKE', 'STATUS_INQUIRY', 'COMPLAINT')
GROUP BY b.city_label, t.day_part
ORDER BY b.city_label, t.day_part;

-- Q9  Pricing against the market (drill-across to the market fact): the company's own quote for a
--     standard trip of about 8 km next to the competitors' 8 km price and the traffic index, by part of day.
SELECT b.city_label, t.day_part, SUM(o.trips) AS own_trips,
       ROUND(AVG(o.own_quote), 0) AS own_quote_8km, ROUND(AVG(m.competitor_price_8km), 0) AS competitor_8km,
       ROUND(AVG(o.own_quote) * 100.0 / AVG(m.competitor_price_8km) - 100, 1) AS own_vs_competitor_pct,
       ROUND(AVG(m.traffic_index), 1) AS traffic_index
FROM (
  SELECT date_key, hour_key, branch_key, AVG(quoted_amount) AS own_quote, COUNT(*) AS trips
  FROM fact_orders
  WHERE order_month = '2026-11' AND service_key = 1 AND distance_km BETWEEN 7 AND 9
  GROUP BY date_key, hour_key, branch_key
) o
JOIN fact_market_hourly m ON o.date_key = m.date_key AND o.hour_key = m.hour_key AND o.branch_key = m.branch_key
JOIN dim_branch b      ON o.branch_key = b.branch_key
JOIN dim_time_of_day t ON o.hour_key = t.hour_key
GROUP BY b.city_label, t.day_part
ORDER BY b.city_label, t.day_part;

-- Q10 Budget view from the accounting export (ledger fact), ROLLUP: revenue and cost groups per city and
--     month; the subtotal row of a month is its result.
SELECT b.city_label, d.year_month, i.item_group, ROUND(SUM(l.amount), 0) AS amount_rub
FROM fact_ledger l
JOIN dim_branch b      ON l.branch_key = b.branch_key
JOIN dim_date d        ON l.date_key = d.date_key
JOIN dim_ledger_item i ON l.ledger_item_key = i.ledger_item_key
GROUP BY b.city_label, d.year_month, i.item_group WITH ROLLUP
ORDER BY b.city_label, d.year_month, i.item_group;

-- Q11 What the company keeps: gross fares against the company's own revenue, own fleet and contractors.
SELECT b.city_label, dr.employment_type, SUM(f.order_count) AS trips,
       ROUND(SUM(f.fare_amount), 0) AS gross_fares, ROUND(SUM(f.company_revenue), 0) AS company_revenue,
       ROUND(SUM(f.company_revenue) * 100.0 / SUM(f.fare_amount), 1) AS kept_pct
FROM fact_orders f
JOIN dim_branch b  ON f.branch_key = b.branch_key
JOIN dim_driver dr ON f.driver_key = dr.driver_key
WHERE f.outcome_key = 1
GROUP BY b.city_label, dr.employment_type
ORDER BY b.city_label, dr.employment_type;

-- Q12 Partition pruning: the plan reads only the folder branch_code=B/order_month=2026-11.
EXPLAIN EXTENDED
SELECT COUNT(*) FROM fact_orders WHERE branch_code = 'B' AND order_month = '2026-11';

-- Q13 Drill-across by zone: orders picked up in each zone against free-car minutes in the same zone and hour
--     (supply against demand for pricing; branch A on the morning of the real extract, 07:00-09:59)
SELECT z.zone_code, z.zone_type, o.hour_key, o.orders, o.cancelled, COALESCE(s.free_minutes, 0) AS free_car_minutes,
       ROUND(COALESCE(s.free_minutes, 0) / o.orders, 1) AS free_minutes_per_order
FROM (
  SELECT f.branch_key, f.pickup_zone_key AS zone_key, f.hour_key, SUM(f.order_count) AS orders,
         SUM(IF(oc.outcome_group = 'CANCELLED', 1, 0)) AS cancelled
  FROM fact_orders f JOIN dim_outcome oc ON f.outcome_key = oc.outcome_key
  WHERE f.date_key = 20261116 AND f.branch_key = 1 AND f.hour_key BETWEEN 7 AND 9 AND f.pickup_zone_key > 0
  GROUP BY f.branch_key, f.pickup_zone_key, f.hour_key
) o
LEFT JOIN (
  SELECT branch_key, zone_key, hour_key, SUM(minutes_free) AS free_minutes
  FROM fact_fleet_activity
  WHERE date_key = 20261116 AND branch_key = 1
  GROUP BY branch_key, zone_key, hour_key
) s ON o.branch_key = s.branch_key AND o.zone_key = s.zone_key AND o.hour_key = s.hour_key
JOIN dim_zone z ON o.zone_key = z.zone_key
ORDER BY o.hour_key, o.orders DESC;
