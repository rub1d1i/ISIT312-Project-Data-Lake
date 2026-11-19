-- ---------------------------------------------------------------------------
-- Findings from the REAL extract only: branch A, 16 Nov 2026, 00:00-10:11
-- (851 orders) and the vehicle positions of the same morning.
-- Every other day and all of branch B are generated and are left out here on
-- purpose: the generator was built to resemble this extract in volume and mix,
-- so a finding drawn from generated rows would only show what was put in.
-- One branch and one Monday morning is a small base; read the numbers as
-- questions worth asking of the full history, not as final answers.
-- Run: beeline -u jdbc:hive2://localhost:10000 -n bigdata -f 05_findings_real_extract.hql
-- ---------------------------------------------------------------------------
USE lake_curated;

-- F1  How orders arrive and how many are lost, by intake account
SELECT channel, COUNT(*) AS orders,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS share_pct,
       SUM(IF(outcome LIKE 'CANCELLED%', 1, 0)) AS cancelled,
       ROUND(SUM(IF(outcome LIKE 'CANCELLED%', 1, 0)) * 100.0 / COUNT(*), 1) AS cancelled_pct,
       SUM(IF(recording_uid IS NOT NULL, 1, 0)) AS with_call_recording,
       SUM(IF(app_pickup_lat IS NOT NULL, 1, 0)) AS with_phone_coordinates,
       SUM(card_payment) AS paid_by_card
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16'
GROUP BY channel
ORDER BY orders DESC;

-- F1b Is the gap between the app and the call centre a matter of the hour? Cancelled share by channel within hour bands
SELECT CASE WHEN order_hour < 6 THEN '00-05' WHEN order_hour < 9 THEN '06-08' ELSE '09-10' END AS hours, channel,
       COUNT(*) AS orders,
       ROUND(SUM(IF(outcome LIKE 'CANCELLED%', 1, 0)) * 100.0 / COUNT(*), 1) AS cancelled_pct
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16' AND channel IN ('MOBILE_APP', 'CALL_CENTRE')
GROUP BY CASE WHEN order_hour < 6 THEN '00-05' WHEN order_hour < 9 THEN '06-08' ELSE '09-10' END, channel
ORDER BY hours, channel;

-- F1c Is a cancelled order lost demand? Cancelled orders followed by a new order of the same client within 30 minutes
SELECT c.channel, COUNT(DISTINCT c.order_id) AS cancelled,
       COUNT(DISTINCT IF(n.order_id IS NOT NULL, c.order_id, NULL)) AS reordered_within_30_min,
       ROUND(COUNT(DISTINCT IF(n.order_id IS NOT NULL, c.order_id, NULL)) * 100.0 / COUNT(DISTINCT c.order_id), 1) AS reordered_pct
FROM (SELECT order_id, cod_cl, channel, accepted_ts FROM orders
      WHERE branch_code = 'A' AND order_date = '2026-11-16' AND outcome LIKE 'CANCELLED%' AND cod_cl IS NOT NULL AND cod_cl > 0) c
LEFT JOIN (SELECT order_id, cod_cl, accepted_ts FROM orders WHERE branch_code = 'A' AND order_date = '2026-11-16') n
  ON n.cod_cl = c.cod_cl AND n.order_id <> c.order_id
 AND unix_timestamp(n.accepted_ts) > unix_timestamp(c.accepted_ts)
 AND unix_timestamp(n.accepted_ts) <= unix_timestamp(c.accepted_ts) + 1800
WHERE c.channel IN ('MOBILE_APP', 'CALL_CENTRE')
GROUP BY c.channel
ORDER BY c.channel;

-- F2  Who pays: non-cash (corporate) orders by intake account, and how many carry the 22 % pattern
SELECT channel, client_segment, COUNT(*) AS orders, SUM(IF(vat_amount IS NOT NULL, 1, 0)) AS price_is_round_amount_plus_22pct
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16'
GROUP BY channel, client_segment
ORDER BY channel, client_segment;

-- F3  Orders flagged "no car by the promised time" (D_OPAZD) and the fate of the order
SELECT IF(is_late, 'flagged: no car in time', 'not flagged') AS arrival, COUNT(*) AS orders,
       SUM(IF(outcome LIKE 'CANCELLED%', 1, 0)) AS cancelled,
       ROUND(SUM(IF(outcome LIKE 'CANCELLED%', 1, 0)) * 100.0 / COUNT(*), 1) AS cancelled_pct
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16'
GROUP BY IF(is_late, 'flagged: no car in time', 'not flagged');

-- F3b The flagged orders that were cancelled: was a driver on the order when it was cancelled?
--     (assigned_ts is D_BUSY, the time a driver was assigned; cod_drv is the driver on the order now)
SELECT CASE WHEN cod_drv IS NOT NULL THEN 'driver on the order when cancelled'
            WHEN assigned_ts IS NOT NULL THEN 'driver assigned earlier, no longer on the order'
            ELSE 'no driver ever assigned' END AS driver,
       COUNT(*) AS orders
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16' AND is_late AND outcome LIKE 'CANCELLED%'
GROUP BY CASE WHEN cod_drv IS NOT NULL THEN 'driver on the order when cancelled'
              WHEN assigned_ts IS NOT NULL THEN 'driver assigned earlier, no longer on the order'
              ELSE 'no driver ever assigned' END;

-- F4  Price per kilometre through the night and the morning peak (standard passenger trips, completed)
SELECT order_hour, COUNT(*) AS trips, ROUND(AVG(distance_km), 1) AS avg_km,
       ROUND(percentile_approx(CAST(pay AS DOUBLE) / distance_km, 0.5), 1) AS median_rub_per_km
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16' AND cart = 1 AND outcome = 'COMPLETED' AND distance_km > 1
GROUP BY order_hour
ORDER BY order_hour;

-- F4b The same comparison within distance bands: night (orders accepted 00:00-05:59) against the morning peak (07:00-08:59)
SELECT CASE WHEN distance_km <= 5 THEN '1-5 km' WHEN distance_km <= 10 THEN '5-10 km'
            WHEN distance_km <= 20 THEN '10-20 km' ELSE 'over 20 km' END AS trip_length,
       ROUND(AVG(distance_km), 1) AS avg_km,
       SUM(IF(order_hour < 6, 1, 0)) AS night_trips,
       ROUND(percentile_approx(IF(order_hour < 6, CAST(pay AS DOUBLE) / distance_km, NULL), 0.5), 1) AS night_rub_per_km,
       SUM(IF(order_hour IN (7, 8), 1, 0)) AS peak_trips,
       ROUND(percentile_approx(IF(order_hour IN (7, 8), CAST(pay AS DOUBLE) / distance_km, NULL), 0.5), 1) AS peak_rub_per_km
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16' AND cart = 1 AND outcome = 'COMPLETED' AND distance_km > 1
GROUP BY CASE WHEN distance_km <= 5 THEN '1-5 km' WHEN distance_km <= 10 THEN '5-10 km'
              WHEN distance_km <= 20 THEN '10-20 km' ELSE 'over 20 km' END
ORDER BY avg_km;

-- F5  Service times of completed orders, in minutes (median and 90th percentile)
SELECT COUNT(*) AS completed,
       percentile_approx(assign_min, array(0.5, 0.9)) AS accept_to_assign,
       percentile_approx(arrive_min, array(0.5, 0.9)) AS assign_to_arrival,
       percentile_approx(wait_min, array(0.5, 0.9))   AS arrival_to_pickup,
       percentile_approx(trip_min, array(0.5, 0.9))   AS pickup_to_done
FROM orders
WHERE branch_code = 'A' AND order_date = '2026-11-16' AND outcome = 'COMPLETED';

-- F6  The fleet during the same morning: vehicles on line, share of on-shift time spent on an order
SELECT hour(received_ts) AS hour_of_day, COUNT(DISTINCT cod_drv) AS vehicles_on_shift,
       ROUND(SUM(IF(state_id = 2, 1, 0)) * 100.0 / COUNT(*), 1) AS on_order_pct,
       ROUND(AVG(IF(state_id = 2 AND speed_kmh > 0, speed_kmh, NULL)), 1) AS avg_kmh_on_order,
       SUM(IF(in_city, 0, 1)) AS positions_outside_city
FROM gps_positions
WHERE branch_code = 'A' AND dt = '2026-11-16'
GROUP BY hour(received_ts)
ORDER BY hour_of_day;
