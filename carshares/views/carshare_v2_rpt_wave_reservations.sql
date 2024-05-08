CREATE OR REPLACE VIEW "carshare_v2_rpt_wave_reservations" AS 
SELECT
  r.reservation_id
, r.invoice_number
, u.username name
, r.type reservation_type
, r.payment_type rental_payment_type
, s.payment_reference reference
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.initiatorMsisdn') AS varchar), '"', '') msisdn
, CAST(round(s.amount) AS int) amount
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.orderId') AS varchar), '"', '') order_id
, s.created_at wave_date_time
, s.status wave_status
, p.erp_account_number
, v.name vehicle_name
, r.pickup_at start_date_time
, r.return_at end_date_time
, r.actual_end_at actual_end_date_time
, l1.name pickup_location
, l2.name return_location
, CAST(round(r.total_price) AS int) total_price
, r.status
, r.payment_status
FROM
  ((((((carshare_v2_staging_subscriptions s
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = s.reservation_id))
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
LEFT JOIN carshare_v2_staging_locations l1 ON (l1.location_id = r.pickup_location_id))
LEFT JOIN carshare_v2_staging_locations l2 ON (l2.location_id = r.return_location_id))
WHERE ((s.payment_type = 'WAVE') AND (r.deleted_at IS NULL))
UNION SELECT
  r.id reservation_id
, r.invoice_number
, u.username name
, r.type reservation_type
, r.payment_type rental_payment_type
, s.payment_reference reference
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.initiatorMsisdn') AS varchar), '"', '') msisdn
, CAST(round(s.amount) AS int) amount
, replace(CAST(json_extract(json_extract_scalar(s.response, '$.pre_authorize_result'), '$.orderId') AS varchar), '"', '') order_id
, s.issued_at wave_date_time
, s.status wave_status
, replace(CAST(json_extract(u.profile, '$.erp_account_number') AS varchar), '"', '') erp_account_number
, v.name vehicle_name
, r.pickup_at start_date_time
, r.return_at end_date_time
, r.actual_end_at actual_end_date_time
, replace(CAST(json_extract(r.pickup_location, '$.name') AS varchar), '"', '') pickup_location
, replace(CAST(json_extract(r.return_location, '$.name') AS varchar), '"', '') return_location
, CAST(round(r.total_price) AS int) total_price
, r.status
, r.payment_status
FROM
  (((carshare_v2_staging_archived_reservations_v2 r
LEFT JOIN carshare_v2_staging_archived_users u ON (u.reservation_id = r.id))
LEFT JOIN carshare_v2_staging_archived_subscriptions s ON (s.reservation_id = r.id))
LEFT JOIN carshare_v2_staging_archived_vehicles v ON (v.reservation_id = r.id))
WHERE (s.payment_type = 'WAVE')