CREATE OR REPLACE VIEW "carshare_v2_rpt_reservations_details" AS 
SELECT
  r.reservation_id
, r.invoice_number invoice_number
, u.username name
, r.payment_type
, r.type reservation_type
, v.name vehicle_name
, r.pickup_at start_date_time
, r.return_at end_date_time
, l.name pickup_location
, l1.name return_location
, CAST(round(r.total_price) AS int) total_price
, u.email
, r.status
, r.payment_status
, ed.name extra_driver_name
, ed.email extra_driver_email
, ed.phone extra_driver_contact_number
, p.erp_account_number
, r.created_at
, r.updated_at
FROM
  (((((((carshare_v2_staging_reservations r
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
LEFT JOIN carshare_v2_staging_locations l ON (l.location_id = r.pickup_location_id))
LEFT JOIN carshare_v2_staging_locations l1 ON (l1.location_id = r.return_location_id))
LEFT JOIN carshare_v2_staging_reservation_drivers rd ON (rd.reservation_id = r.reservation_id))
LEFT JOIN carshare_v2_staging_extra_drivers ed ON (ed.extra_driver_id = rd.extra_driver_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = r.user_id))
WHERE ((r.status <> 'CANCELLED') AND (r.deleted_at IS NULL))
