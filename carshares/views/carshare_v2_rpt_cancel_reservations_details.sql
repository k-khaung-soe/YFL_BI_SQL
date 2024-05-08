CREATE OR REPLACE VIEW "carshare_v2_rpt_cancel_reservations_details" AS 
SELECT
  r.reservation_id reservation_id
, r.invoice_number invoice_number
, u.username name
, r.payment_type
, v.name vehicle_name
, r.pickup_at start_date_time
, return_at end_date_time
, l.name pick_up_location
, l1.name return_location
, CAST(round(r.total_price) AS int) total_price
, u.email
, r.payment_status
, rh.description cancellation_reason
, CAST(round(s.amount) AS int) cancellation_fee
, s.payment_type cancellation_fee_payment_type
, s.status cancellation_fee_payment_status
, p.erp_account_number
, com.erp_trade_receivable_number
, rh.created_at cancellation_time
, r.updated_at
FROM
  (((((((((carshare_v2_staging_reservations r
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_companies com ON (com.company_id = ca.company_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.vehicle_id))
LEFT JOIN carshare_v2_staging_locations l ON (l.location_id = r.pickup_location_id))
LEFT JOIN carshare_v2_staging_locations l1 ON (l1.location_id = r.return_location_id))
LEFT JOIN (
   SELECT rh1.*
   FROM
     (
      SELECT
        *
      , row_number() OVER (PARTITION BY reservation_id ORDER BY created_at DESC) rn
      FROM
        carshare_v2_staging_reservation_histories
      WHERE (status = 'CANCELLED')
   )  rh1
   WHERE (rh1.rn = 1)
)  rh ON (rh.reservation_id = r.reservation_id))
LEFT JOIN (
   SELECT
     s1.reservation_id
   , s1.amount
   , s1.status
   , s1.payment_type
   FROM
     (
      SELECT
        reservation_id
      , amount
      , status
      , created_at
      , payment_type
      , row_number() OVER (PARTITION BY reservation_id ORDER BY created_at DESC) rn
      FROM
        carshare_v2_staging_subscriptions
      WHERE (type = 'CANCELLED')
   )  s1
   WHERE (s1.rn = 1)
)  s ON (s.reservation_id = r.reservation_id))
WHERE ((r.status = 'CANCELLED') AND (r.deleted_at IS NULL))
ORDER BY 1 ASC