CREATE OR REPLACE VIEW "carshare_v2_rpt_outstanding_details" AS 
SELECT
  r.reservation_id
, r.invoice_number invoice_number
, u.username name
, p.erp_account_number
, v.name vehicle_name
, r.pickup_at start_date_time
, r.return_at end_date_time
, l.name pickup_location
, r.pickup_at pick_up_time
, round(CAST(or2.amount AS bigint)) amount
, or2.created_at created_at
, r.payment_type
, r.payment_status status
, or2.updated_at
FROM
  (((((carshare_v2_staging_outstanding_reservations or2
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = or2.reservation_id))
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
LEFT JOIN carshare_v2_staging_locations l ON (l.location_id = r.pickup_location_id))