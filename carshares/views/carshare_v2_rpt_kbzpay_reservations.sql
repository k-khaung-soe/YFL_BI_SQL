CREATE OR REPLACE VIEW "carshare_v2_rpt_kbzpay_reservations" AS 
SELECT
  r.reservation_id
, r.invoice_number
, u.username
, r.type reservation_type
, r.payment_type rental_payment_type
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.merch_code') AS varchar), '"', '') merchant_code
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.merch_order_id') AS varchar), '"', '') order_id
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.mm_order_id') AS varchar), '"', '') kbzpay_order_id
, CAST(replace(CAST(at_timezone(CAST(from_unixtime(CAST(replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.notify_time') AS varchar), '"', '') AS double)) AS timestamp), 'Asia/Yangon') AS varchar), 'Asia/Yangon', '') AS timestamp) notification_time
, s.amount transaction_amount
, s.type transaction_type
, s.payment_type transaction_payment_type
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.trade_status') AS varchar), '"', '') transaction_status
, CAST(replace(CAST(at_timezone(CAST(from_unixtime(CAST(replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.trans_end_time') AS varchar), '"', '') AS double)) AS timestamp), 'Asia/Yangon') AS varchar), 'Asia/Yangon', '') AS timestamp) transaction_end_time
, p.erp_account_number
, v.name vehicle_name
, r.pickup_at start_date_time
, r.return_at end_date_time
, r.actual_end_at actual_end_date_time
, l1.name pickup_location
, l2.name return_location
, round(r.total_price) total_price
, r.status
, r.payment_status
, r.created_at
, r.updated_at
FROM
  ((((((carshare_v2_staging_reservations r
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_subscriptions s ON (s.reservation_id = r.reservation_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
LEFT JOIN carshare_v2_staging_locations l1 ON (l1.location_id = r.pickup_location_id))
LEFT JOIN carshare_v2_staging_locations l2 ON (l2.location_id = r.return_location_id))
WHERE ((s.payment_type = 'KPAY') AND (r.deleted_at IS NULL))
UNION SELECT
  r.id reservation_id
, r.invoice_number
, u.username
, r.type reservation_type
, r.payment_type rental_payment_type
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.merch_code') AS varchar), '"', '') merchant_code
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.merch_order_id') AS varchar), '"', '') order_id
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.mm_order_id') AS varchar), '"', '') kbzpay_order_id
, CAST(replace(CAST(at_timezone(CAST(from_unixtime(CAST(replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.notify_time') AS varchar), '"', '') AS double)) AS timestamp), 'Asia/Yangon') AS varchar), 'Asia/Yangon', '') AS timestamp) notification_time
, s.amount transaction_amount
, s.type transaction_type
, s.payment_type transaction_payment_type
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.trade_status') AS varchar), '"', '') transaction_status
, CAST(replace(CAST(at_timezone(CAST(from_unixtime(CAST(replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.trans_end_time') AS varchar), '"', '') AS double)) AS timestamp), 'Asia/Yangon') AS varchar), 'Asia/Yangon', '') AS timestamp) transaction_end_time
, replace(CAST(json_extract(u.profile, '$.erp_account_number') AS varchar), '"', '') erp_account_number
, v.name vehicle_name
, r.pickup_at start_date_time
, r.return_at end_date_time
, r.actual_end_at actual_end_date_time
, replace(CAST(json_extract(r.pickup_location, '$.name') AS varchar), '"', '') pickup_location
, replace(CAST(json_extract(r.return_location, '$.name') AS varchar), '"', '') return_location
, round(r.total_price) total_price
, r.status
, r.payment_status
, r.reservation_created_at created_at
, r.reservation_updated_at updated_at
FROM
  (((carshare_v2_staging_archived_reservations_v2 r
LEFT JOIN carshare_v2_staging_archived_users u ON (u.reservation_id = r.id))
LEFT JOIN carshare_v2_staging_archived_subscriptions s ON (s.reservation_id = r.id))
LEFT JOIN carshare_v2_staging_archived_vehicles v ON (v.reservation_id = r.id))
WHERE (s.payment_type = 'KPAY')