-- ---------------------------------------------------------------------------
-- RAW zone: Hive external tables over the files exactly as they landed.
-- Schema on read: the files are not converted or moved; dropping a table
-- here never deletes data. Every dispatch column is declared STRING because
-- the export mixes types in the same column (see podezd).
-- Run: beeline -u jdbc:hive2://localhost:10000 -n bigdata -f 01_raw_tables.hql
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS lake_raw COMMENT 'raw zone - source format, schema on read' LOCATION '/lake/raw/_hive';
USE lake_raw;

-- dispatch system: order archive, 95 columns as exported by the branch database
DROP TABLE IF EXISTS orders;
CREATE EXTERNAL TABLE orders (
  cod_cl             STRING,
  op_prn             STRING,
  op_otp             STRING,
  cod_drv            STRING,
  cod_car            STRING,
  eaddr_frm          STRING,
  eaddr_to           STRING,
  oaddr_frm          STRING,
  oaddr_to           STRING,
  pay                STRING,
  d_pzakaz           STRING,
  d_vzakaz           STRING,
  d_sparka           STRING,
  d_vprib            STRING,
  d_vipol            STRING,
  d_busy             STRING,
  d_noansw           STRING,
  status             STRING,
  cod_route          STRING,
  zakaz_info         STRING,
  d_podzvon          STRING,
  eaddr_1            STRING,
  eaddr_2            STRING,
  eaddr_3            STRING,
  eaddr_4            STRING,
  eaddr_5            STRING,
  eaddr_6            STRING,
  eaddr_7            STRING,
  eaddr_8            STRING,
  beznal             STRING,
  waite_pay          STRING,
  channel            STRING,
  cid_tel            STRING,
  cart               STRING,
  cod_akc            STRING,
  wavf               STRING,
  plan_id            STRING,
  order_id           STRING,
  tender_num         STRING,
  podezd             STRING,
  reazon             STRING,
  cnt_retr           STRING,
  d_vzyal            STRING,
  ds_status          STRING,
  notify             STRING,
  cmnt               STRING,
  distance           STRING,
  d_inform           STRING,
  drv_podacha        STRING,
  podacha            STRING,
  d_startwait        STRING,
  d_cl_ack           STRING,
  cnt_cl_ack         STRING,
  d_drv_podacha      STRING,
  d_call_try         STRING,
  d_opazd            STRING,
  order_limit_mask   STRING,
  do_propose         STRING,
  drv_near           STRING,
  bonus              STRING,
  rezerv_cod_drv     STRING,
  mejgorod           STRING,
  d_paysms           STRING,
  s_channel          STRING,
  d_wait             STRING,
  c_wait             STRING,
  d_pochas           STRING,
  pochas             STRING,
  s_pay              STRING,
  zakaz_naryad       STRING,
  netoch_addr_id     STRING,
  drv_mejgorod       STRING,
  drv_wait_min       STRING,
  is_taxometr        STRING,
  show_tel           STRING,
  aa_ord             STRING,
  operator_info      STRING,
  wtx_a_id           STRING,
  wtx_p_id           STRING,
  wtx_dready         STRING,
  stavka             STRING,
  free_bonus         STRING,
  show_in_history    STRING,
  ord_prio           STRING,
  contact_phone      STRING,
  adapted_id         STRING,
  eaddr_info_id      STRING,
  eaddr_coords       STRING,
  eaddr_lat          STRING,
  eaddr_lon          STRING,
  chat_client        STRING,
  d_torg             STRING,
  card_payment       STRING,
  eaddr_lat_last     STRING,
  eaddr_lon_last     STRING
)
PARTITIONED BY (branch STRING, dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lake/raw/dispatch/orders'
TBLPROPERTIES ('skip.header.line.count'='1');
MSCK REPAIR TABLE orders;

-- dispatch system: vehicle positions, one-off back-fill of the GPS table up to the cut-over to streaming
DROP TABLE IF EXISTS gps;
CREATE EXTERNAL TABLE gps (
  id BIGINT, cod_drv INT, d_receive STRING, lat DOUBLE, lon DOUBLE, sector_id INT,
  speed DOUBLE, track DOUBLE, fix INT, quality INT, state_gps_id INT, id_terminal BIGINT
)
PARTITIONED BY (branch STRING, dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lake/raw/dispatch/gps'
TBLPROPERTIES ('skip.header.line.count'='1');
MSCK REPAIR TABLE gps;

-- vehicle positions landed by the streaming job since the cut-over (Parquet, many small files)
DROP TABLE IF EXISTS gps_stream;
CREATE EXTERNAL TABLE gps_stream (
  id BIGINT, cod_drv INT, ts STRING, lat DOUBLE, lon DOUBLE, speed DOUBLE, track DOUBLE, fix INT, quality INT, state INT, terminal BIGINT,
  kafka_ts TIMESTAMP, `partition` INT, `offset` BIGINT, event_ts TIMESTAMP
)
PARTITIONED BY (branch STRING, dt STRING)
STORED AS PARQUET
LOCATION '/lake/raw/stream/gps';
MSCK REPAIR TABLE gps_stream;

-- PBX call detail records
DROP TABLE IF EXISTS cdr;
CREATE EXTERNAL TABLE cdr (
  calldate STRING, uniqueid STRING, branch_id INT, src_hash STRING, dst STRING, direction STRING, call_type STRING,
  op_id INT, duration INT, billsec INT, disposition STRING, queue_wait INT, recordingfile STRING, order_id BIGINT
)
PARTITIONED BY (branch STRING, dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lake/raw/pbx/cdr'
TBLPROPERTIES ('skip.header.line.count'='1');
MSCK REPAIR TABLE cdr;

-- billing: payments and driver settlements
DROP TABLE IF EXISTS payments;
CREATE EXTERNAL TABLE payments (
  payment_id BIGINT, branch_id INT, order_id BIGINT, paid_at STRING, amount DOUBLE, currency STRING, method STRING,
  cod_drv INT, commission_amount DOUBLE, driver_payout DOUBLE, state STRING
)
PARTITIONED BY (branch STRING, dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lake/raw/billing/payments'
TBLPROPERTIES ('skip.header.line.count'='1');
MSCK REPAIR TABLE payments;

-- accounting package export: Windows-1251, ';' separated, dd.MM.yyyy dates, decimal comma.
-- The SerDe decodes the code page; dates and amounts stay text until curation.
DROP TABLE IF EXISTS gl_export;
CREATE EXTERNAL TABLE gl_export (
  doc_date STRING, unit STRING, account STRING, item STRING, amount STRING, currency STRING, posting STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=';', 'serialization.encoding'='windows-1251')
STORED AS TEXTFILE
LOCATION '/lake/raw/finance/gl'
TBLPROPERTIES ('skip.header.line.count'='1');

-- SMS gateway log
DROP TABLE IF EXISTS sms;
CREATE EXTERNAL TABLE sms (
  sms_id BIGINT, sent_at STRING, branch_id INT, cod_cl BIGINT, phone_hash STRING, template STRING, order_id BIGINT,
  delivery_status STRING, segments INT, cost_rub DOUBLE
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lake/raw/messaging/sms'
TBLPROPERTIES ('skip.header.line.count'='1');
MSCK REPAIR TABLE sms;

-- semi-structured sources: one JSON document per line, kept as a single string
-- column and read with get_json_object() - no schema has to be agreed up front.
DROP TABLE IF EXISTS app_events;
CREATE EXTERNAL TABLE app_events (line STRING)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION '/lake/raw/app/events';
MSCK REPAIR TABLE app_events;

DROP TABLE IF EXISTS chat_messages;
CREATE EXTERNAL TABLE chat_messages (line STRING)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION '/lake/raw/messaging/chat';
MSCK REPAIR TABLE chat_messages;

DROP TABLE IF EXISTS competitor_prices;
CREATE EXTERNAL TABLE competitor_prices (line STRING)
STORED AS TEXTFILE
LOCATION '/lake/raw/external/competitor_prices';

DROP TABLE IF EXISTS dashcam_requests;
CREATE EXTERNAL TABLE dashcam_requests (line STRING)
STORED AS TEXTFILE
LOCATION '/lake/raw/video/catalog/dashcam_requests';

DROP TABLE IF EXISTS traffic_index;
CREATE EXTERNAL TABLE traffic_index (ts STRING, branch_id INT, traffic_index DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lake/raw/external/traffic'
TBLPROPERTIES ('skip.header.line.count'='1');

SHOW TABLES;

-- a first look through the raw tables: rows per source, straight from the landed files
SELECT 'orders' AS source, COUNT(*) AS row_count FROM orders
UNION ALL SELECT 'gps (back-fill)', COUNT(*) FROM gps
UNION ALL SELECT 'gps (stream)', COUNT(*) FROM gps_stream
UNION ALL SELECT 'cdr', COUNT(*) FROM cdr
UNION ALL SELECT 'payments', COUNT(*) FROM payments
UNION ALL SELECT 'sms', COUNT(*) FROM sms
UNION ALL SELECT 'app_events', COUNT(*) FROM app_events
UNION ALL SELECT 'gl_export', COUNT(*) FROM gl_export;

-- the accounting export is readable although it arrived in Windows-1251 with ';' separators
SELECT doc_date, unit, account, item, amount FROM gl_export LIMIT 4;

-- schema on read for JSON: fields are picked out of the document at query time
SELECT get_json_object(line, '$.event') AS event, get_json_object(line, '$.platform') AS platform, COUNT(*) AS events
FROM app_events WHERE dt = '2026-11-16'
GROUP BY get_json_object(line, '$.event'), get_json_object(line, '$.platform')
ORDER BY events DESC LIMIT 6;
