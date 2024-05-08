CREATE OR REPLACE VIEW "carshare_v2_rpt_reservation_additional_charges" AS 
SELECT
  aac.reservation_id
, ac.invoice_number
, u.username
, u.email
, replace(CAST(json_extract(u.coporate_account, '$.name') AS varchar), '"', '') company
, replace(CAST(json_extract(u.coporate_account, '$.erp_trade_receivable_number') AS varchar), '"', '') erp_trade_receivable_number
, as4.card_type
, ac.payment_type
, replace(CAST(json_extract(u.profile, '$.erp_account_number') AS varchar), '"', '') erp_account_number
, v.license_plate_number
, v.name vehicle_name
, ac.pickup_at start_date_time
, ac.return_at end_date_time
, ac.extend_start_at extend_start_date_time
, ac.extend_end_at extend_end_date_time
, ac.pickup_at actual_start_date_time
, ac.actual_end_at actual_end_date_time
, aac.amount
, aac.description
, aac.type
, (CASE WHEN (aac.apply_tax = 1) THEN 'Yes' ELSE 'No' END) apply_tax
, ac.status reservation_status
, ac.type rent_type
, ac.payment_status
, ac.reservation_created_at
, ac.created_at archived_date
, as2.payment_reference reference
FROM
  (((((carshare_v2_staging_archived_reservation_additional_charges aac
LEFT JOIN carshare_v2_staging_archived_reservations_v2 ac ON (aac.reservation_id = ac.id))
LEFT JOIN carshare_v2_staging_archived_users u ON (u.reservation_id = aac.reservation_id))
LEFT JOIN (
   SELECT
     reservation_id
   , replace(CAST(json_extract(json_extract(json_extract(json_extract(json_extract(response, '$.payment_capture_result'), '$.sourceOfFunds'), '$.provided'), '$.card'), '$.brand') AS varchar), '"', '') card_type
   , row_number() OVER (PARTITION BY reservation_id ORDER BY issued_at DESC) rn
   FROM
     carshare_v2_staging_archived_subscriptions
)  as4 ON ((as4.reservation_id = ac.id) AND (as4.rn = 1)))
LEFT JOIN carshare_v2_staging_archived_vehicles v ON (v.reservation_id = ac.id))
LEFT JOIN (
   SELECT
     reservation_id
   , payment_type
   , payment_reference
   , row_number() OVER (PARTITION BY reservation_id ORDER BY issued_at DESC) rn
   FROM
     carshare_v2_staging_archived_subscriptions
)  as2 ON ((as2.reservation_id = ac.id) AND (as2.rn = 1)))
UNION ALL SELECT
  r.reservation_id
, r.invoice_number
, u.username
, u.email
, c.name company
, c.erp_trade_receivable_number
, p.brand card_type
, r.payment_type
, p2.erp_account_number
, v.license_plate_number
, v.name vehicle_name
, r.pickup_at start_date_time
, r.return_at end_date_time
, r.extend_start_at extend_start_date_time
, r.extend_end_at extend_end_date_time
, r.pickup_at actual_start_date_time
, r.actual_end_at actual_end_date_time
, rac.amount
, rac.description
, rac.type
, (CASE WHEN (rac.apply_tax = 1) THEN 'Yes' ELSE 'No' END) apply_tax
, r.status reservation_status
, r.type rent_type
, r.payment_status payment_status
, r.created_at reservation_created_at
, null archived_date
, s.payment_reference reference
FROM
  ((((((((carshare_v2_staging_reservation_additional_charges rac
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = rac.reservation_id))
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_companies c ON (c.company_id = ca.company_id))
LEFT JOIN (
   SELECT
     reservation_id
   , payment_type
   , payment_reference
   , row_number() OVER (PARTITION BY reservation_id ORDER BY created_at DESC) rn
   FROM
     carshare_v2_staging_subscriptions
)  s ON ((s.reservation_id = r.reservation_id) AND (s.rn = 1)))
LEFT JOIN carshare_v2_staging_profiles p2 ON (p2.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
LEFT JOIN carshare_v2_staging_payments p ON (CAST(p.id AS varchar) = s.payment_reference))