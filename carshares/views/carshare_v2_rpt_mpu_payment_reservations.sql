CREATE OR REPLACE VIEW "carshare_v2_rpt_mpu_payment_reservations" AS 
SELECT
  a.reservation_id
, a.invoice_number
, a.username
, a.reservation_type
, a.payment_type rental_payment_type
, a.merchant_id
, a.mpu_invoice
, a.transaction_reference
, a.approval_code
, (CASE WHEN (length(a.mpu_date_time) < 14) THEN CAST(date_format(from_unixtime(CAST(a.mpu_date_time AS bigint)), '%Y-%m-%d %H:%i:%s') AS timestamp) ELSE CAST(concat(SUBSTR(a.mpu_date_time, 1, 4), '-', SUBSTR(a.mpu_date_time, 5, 2), '-', SUBSTR(a.mpu_date_time, 7, 2), ' ', SUBSTR(a.mpu_date_time, 9, 2), ':', SUBSTR(a.mpu_date_time, 11, 2), ':', SUBSTR(a.mpu_date_time, 13, 2)) AS timestamp) END) mpu_date_time
, a.mpu_status
, a.erp_account_number
, a.vehicle_name
, a.start_date_time
, a.end_date_time
, a.actual_end_date_time
, a.pickup_location
, a.return_location
, a.total_price
, a.status
, a.payment_status
, a.transaction_amount
, a.type transaction_type
, a.transaction_status
, a.transaction_payment_type
, a.reservation_created_date_time
, a.updated_at
FROM
  (
   SELECT
     r.reservation_id
   , r.invoice_number
   , u.username
   , r.type reservation_type
   , r.payment_type
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.merchantID') AS varchar), '"', '') merchant_id
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.invoiceNo') AS varchar), '"', '') mpu_invoice
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.tranRef') AS varchar), '"', '') transaction_reference
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.approvalCode') AS varchar), '"', '') approval_code
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.dateTime') AS varchar), '"', '') mpu_date_time
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.status') AS varchar), '"', '') mpu_status
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
   , s.amount transaction_amount
   , s.status transaction_status
   , r.created_at reservation_created_date_time
   , r.updated_at updated_at
   , s.type
   , s.payment_type transaction_payment_type
   FROM
     ((((((carshare_v2_staging_reservations r
   LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
   LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
   LEFT JOIN carshare_v2_staging_subscriptions s ON (s.reservation_id = r.reservation_id))
   LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
   LEFT JOIN carshare_v2_staging_locations l1 ON (l1.location_id = r.pickup_location_id))
   LEFT JOIN carshare_v2_staging_locations l2 ON (l2.location_id = r.return_location_id))
   WHERE (s.payment_type = 'MPU')
UNION    SELECT
     r.id reservation_id
   , r.invoice_number
   , u.username
   , r.type reservation_type
   , r.payment_type
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.merchantID') AS varchar), '"', '') merchant_id
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.invoiceNo') AS varchar), '"', '') mpu_invoice
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.tranRef') AS varchar), '"', '') transaction_reference
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.approvalCode') AS varchar), '"', '') approval_code
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.dateTime') AS varchar), '"', '') mpu_date_time
   , replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.status') AS varchar), '"', '') mpu_status
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
   , s.amount transaction_amount
   , s.status transaction_status
   , r.reservation_created_at reservation_created_date_time
   , r.reservation_updated_at updated_at
   , s.type
   , s.payment_type transaction_payment_type
   FROM
     (((carshare_v2_staging_archived_reservations_v2 r
   LEFT JOIN carshare_v2_staging_archived_users u ON (u.reservation_id = r.id))
   LEFT JOIN carshare_v2_staging_archived_subscriptions s ON (s.reservation_id = r.id))
   LEFT JOIN carshare_v2_staging_archived_vehicles v ON (v.reservation_id = r.id))
   WHERE (s.payment_type = 'MPU')
)  a
