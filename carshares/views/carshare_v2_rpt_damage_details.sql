CREATE OR REPLACE VIEW "carshare_v2_rpt_damage_details" AS 
SELECT
  r.reservation_id reservation_id
, r.invoice_number invoice_number
, v.name
, v.license_plate_number
, p.erp_account_number
, u.username
, CAST(r.pickup_at AS date) start_date
, CAST(r.return_at AS date) end_date
, u.email
, replace(replace(rr.description, chr(10), ' '), chr(13), ' ') description
, rr.type
, rr.created_at created_at
, rr.status
FROM
  ((((carshare_v2_staging_reservation_reports rr
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = rr.reservation_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = r.user_id))
WHERE ((rr.type IN ('DAMAGE', 'CHECK_CAR')) AND (rr.deleted_at IS NULL))
UNION SELECT
  ar.id reservation_id
, ar.invoice_number invoice_number
, av.name vehicle_name
, av.license_plate_number
, CAST(json_extract(au.profile, '$.erp_account_number') AS varchar) erp_account_number
, au.username
, CAST(ar.pickup_at AS date) start_date
, CAST(ar.return_at AS date) end_date
, au.email
, replace(replace(replace(CAST(JSON_EXTRACT(JSON_EXTRACT(reservation_reports, '$[0]'), '$.description') AS varchar), '"', ''), chr(10), ' '), chr(13), ' ') description
, replace(CAST(JSON_EXTRACT(JSON_EXTRACT(reservation_reports, '$[0]'), '$.type') AS varchar), '"', '') type
, ar.reservation_created_at created_at
, replace(CAST(JSON_EXTRACT(JSON_EXTRACT(reservation_reports, '$[0]'), '$.status') AS varchar), '"', '') status
FROM
  ((carshare_v2_staging_archived_reservations_v2 ar
LEFT JOIN carshare_v2_staging_archived_vehicles av ON (av.reservation_id = CAST(JSON_EXTRACT(JSON_EXTRACT(reservation_reports, '$[0]'), '$.reservation_id') AS bigint)))
LEFT JOIN carshare_v2_staging_archived_users au ON (au.reservation_id = CAST(JSON_EXTRACT(JSON_EXTRACT(reservation_reports, '$[0]'), '$.reservation_id') AS bigint)))
WHERE ((replace(CAST(JSON_EXTRACT(JSON_EXTRACT(reservation_reports, '$[0]'), '$.type') AS varchar), '"', '') IN ('DAMAGE', 'CHECK_CAR')) AND (ar.deleted_at IS NULL))