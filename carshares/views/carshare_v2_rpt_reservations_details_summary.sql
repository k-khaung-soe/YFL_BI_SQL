CREATE OR REPLACE VIEW "carshare_v2_rpt_reservations_details_summary" AS 
SELECT
  r.reservation_id
, r.invoice_number invoice_number
, u.username name
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
, r.pickup_at actual_start_date_time
, r.extend_start_at extend_start_date_time
, r.extend_end_at extend_end_date_time
, r.actual_end_at actual_end_date_time
, CAST(replace(replace(CAST(json_extract(r2.rate, '$.daily_rate.unit') AS varchar), '"', ''), ' days', '') AS int) calculated_days
, (CASE WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 1) LIKE '%hr%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 1), 'hr', ''), ' ', '') AS int) ELSE null END) calculated_hours
, (CASE WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 1) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 1), 'min', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 2) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 2), 'min', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 3) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', ''), ',', 3), 'min', ''), ' ', '') AS int) ELSE null END) calculated_mins
, (CASE WHEN (replace(CAST(json_extract(r2.rate, '$.daily_rate.unit') AS varchar), '"', '') IS NOT NULL) THEN CAST(round(CAST(replace(CAST(json_extract(r2.rate, '$.daily_rate.total') AS varchar), '"', '') AS double)) AS int) END) calculated_day_rate
, (CASE WHEN (replace(CAST(json_extract(r2.rate, '$.hourly_rate.unit') AS varchar), '"', '') IS NOT NULL) THEN CAST(round(CAST(replace(CAST(json_extract(r2.rate, '$.hourly_rate.total') AS varchar), '"', '') AS double)) AS int) END) calculated_hour_rate
, r.km_out
, r.km_in
, (r.km_in - r.km_out) km_usage
, CAST(replace(CAST(json_extract(json_extract(r2.rate, '$.acriss_rate'), '$.price_per_kilometers') AS varchar), '"', '') AS double) km_rate
, CAST(round(CAST(replace(CAST(json_extract(json_extract(r2.rate, '$.kilo_rate'), '$.total') AS varchar), '"', '') AS double)) AS int) km_charges
, (CASE WHEN (r.extend_start_at IS NOT NULL) THEN 'Yes' ELSE 'No' END) is_extended
, (CASE WHEN (r.extend_start_at IS NOT NULL) THEN (date_diff('minute', r.return_at, r.extend_end_at) / 1440) END) extended_number_of_days
, (CASE WHEN (r.extend_start_at IS NOT NULL) THEN ((date_diff('minute', r.return_at, r.extend_end_at) % 1440) / 60) END) extended_number_of_hours
, (CASE WHEN (r.extend_start_at IS NOT NULL) THEN ((date_diff('minute', r.return_at, r.extend_end_at) % 1440) % 60) END) extended_number_of_minutes
, (CASE WHEN (r.pickup_location_id <> r.return_location_id) THEN 'Yes' ELSE 'No' END) is_owr
, (CASE WHEN (round(CAST(replace(CAST(json_extract(r2.rate, '$.owr_rate.total') AS varchar), '"', '') AS double)) > 0) THEN CAST(round(CAST(replace(CAST(json_extract(r2.rate, '$.owr_rate.total') AS varchar), '"', '') AS double)) AS int) END) owr_rate
, (CASE WHEN (r.pickup_location_id <> r.return_location_id) THEN l2.name END) owr_location
, l1.name pickup_location
, l2.name return_location
, 'NA' fuel_out
, 'NA' fuel_in
, (CASE WHEN (round(CAST(replace(CAST(json_extract(r2.rate, '$.fuel_cost') AS varchar), '"', '') AS double)) > 0) THEN 'Yes' ELSE 'No' END) is_fuel_claim
, (CASE WHEN (round(CAST(replace(CAST(json_extract(r2.rate, '$.fuel_cost') AS varchar), '"', '') AS double)) > 0) THEN round(CAST(replace(CAST(json_extract(r2.rate, '$.fuel_cost') AS varchar), '"', '') AS double)) END) fuel_claim_amount
, (CASE WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 1) LIKE '%day%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 1), 'day', ''), ' ', '') AS int) ELSE null END) calculated_late_days
, (CASE WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 1) LIKE '%hr%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 1), 'hr', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 2) LIKE '%hr%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 2), 'hr', ''), ' ', '') AS int) ELSE null END) calculated_late_hours
, (CASE WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 1) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 1), 'min', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 2) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 2), 'min', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 3) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(r2.rate, '$.late_fee.unit') AS varchar), '"', ''), ',', 3), 'min', ''), ' ', '') AS int) ELSE null END) calculated_late_mins
, CAST(round(CAST(replace(CAST(json_extract(r2.rate, '$.late_fee.total') AS varchar), '"', '') AS double)) AS int) calculated_late_hour_rate
, (CASE WHEN (rd.discount_type = 'PROMO') THEN 'Yes' ELSE 'No' END) is_promo
, (CASE WHEN (rd.discount_type = 'PROMO') THEN replace(CAST(json_extract(rd.discount_detail, '$.code') AS varchar), '"', '') END) promo_code
, (CASE WHEN (rd.discount_type = 'PROMO') THEN CAST(replace(CAST(json_extract(rd.discount_detail, '$.amount') AS varchar), '"', '') AS double) END) promo_amount
, (CASE WHEN (rd.discount_type = 'PROMO') THEN replace(CAST(json_extract(rd.discount_detail, '$.type') AS varchar), '"', '') END) promo_type
, (CASE WHEN (rd.discount_type = 'PROMO') THEN CAST(round(CAST(replace(CAST(json_extract(r2.rate, '$.discount.total') AS varchar), '"', '') AS double)) AS int) END) promo_rate
, (CASE WHEN (rd.discount_type = 'POINT') THEN 'Yes' ELSE 'No' END) is_point
, (CASE WHEN (rd.discount_type = 'POINT') THEN rd.amount END) number_of_points
, (CASE WHEN (rd.discount_type = 'COUPON') THEN 'Yes' ELSE 'No' END) is_coupon
, (CASE WHEN (rd.discount_type = 'COUPON') THEN replace(CAST(json_extract(rd.discount_detail, '$.type') AS varchar), '"', '') END) coupon_type
, (CASE WHEN (rd.discount_type = 'COUPON') THEN rd.amount END) coupon_amount
, (CASE WHEN (rd.discount_type = 'COUPON') THEN CAST(round(CAST(replace(CAST(json_extract(r2.rate, '$.discount.total') AS varchar), '"', '') AS double)) AS int) END) coupon_rate
, CAST(round(CAST(replace(CAST(json_extract(r2.rate, '$.sub_total') AS varchar), '"', '') AS double)) AS int) invoice_amount_before_tax
, CAST(round(CAST(replace(CAST(json_extract(json_extract(r2.rate, '$.commercial_tax'), '$.total') AS varchar), '"', '') AS double)) AS int) tax_amount
, rac.reservation_additional_charges
, rac.name additional_charges_description
, CAST(round(r.total_price) AS int) total_price
, r.status status
, r.type rent_type
, r.payment_status payment_status
, r.created_at reservation_created_at
, r.updated_at
, s.payment_reference reference
, uph.total points_earned_from_reservation
, (CASE WHEN (r.transaction_type = 'RENTAL') THEN 'No' ELSE 'Yes' END) rental_like_yes_or_no
, paid_date.paid_date paid_changed_date
FROM
  ((((((((((((((carshare_v2_staging_reservations r
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
LEFT JOIN (
   SELECT
     reservation_id
   , rate
   , row_number() OVER (PARTITION BY reservation_id ORDER BY id DESC) rn
   FROM
     carshare_v2_staging_reservation_rates
)  r2 ON ((r2.reservation_id = r.reservation_id) AND (r2.rn = 1)))
LEFT JOIN carshare_v2_staging_locations l1 ON (l1.location_id = r.pickup_location_id))
LEFT JOIN carshare_v2_staging_locations l2 ON (l2.location_id = r.return_location_id))
LEFT JOIN (
   SELECT
     reservation_id
   , discount_type
   , type
   , discount_detail
   , CAST(amount AS decimal) amount
   , row_number() OVER (PARTITION BY reservation_id ORDER BY created_at DESC) rn
   FROM
     carshare_v2_staging_reservation_discounts
)  rd ON ((rd.reservation_id = r.reservation_id) AND (rd.rn = 1)))
LEFT JOIN carshare_v2_staging_payments p ON (CAST(p.id AS varchar) = s.payment_reference))
LEFT JOIN (
   SELECT
     reference
   , point_type
   , point_id
   , total
   , row_number() OVER (PARTITION BY reference, point_id ORDER BY created_at DESC) rn
   FROM
     carshare_v2_staging_user_point_histories
   WHERE ((point_type = 'EARNED') AND (point_id = 2))
)  uph ON ((uph.reference = r.invoice_number) AND (uph.rn = 1)))
LEFT JOIN (
   SELECT
     reservation_id
   , array_join(array_agg(DISTINCT COALESCE(description, '')), '/') name
   , sum(amount) reservation_additional_charges
   FROM
     carshare_v2_staging_reservation_additional_charges
   WHERE (reservation_sub_invoice_id IS NULL)
   GROUP BY 1
)  rac ON (rac.reservation_id = r.reservation_id))
LEFT JOIN (
   SELECT
     r.reservation_id
   , r.updated_at
   , (CASE WHEN ((r.reservation_id = rental_paid.reservation_id) AND (NOT (r.reservation_id IN (SELECT reservation_id
FROM
  carshare_v2_staging_subscriptions
WHERE ((type = 'OUTSTANDING') AND (status = 'SUCCESS'))
)))) THEN rental_paid.updated_at WHEN (r.reservation_id = outs.reservation_id) THEN outs.updated_at ELSE null END) paid_date
   FROM
     ((carshare_v2_staging_reservations r
   LEFT JOIN (
      SELECT
        reservation_id
      , updated_at
      , ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY updated_at DESC) rn
      FROM
        carshare_v2_staging_reservation_histories
      WHERE (status = 'CLOSED')
   )  rental_paid ON ((rental_paid.reservation_id = r.reservation_id) AND (rental_paid.rn = 1)))
   LEFT JOIN (
      SELECT
        reservation_id
      , updated_at
      , ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY updated_at DESC) rn
      FROM
        carshare_v2_staging_subscriptions
      WHERE ((type = 'OUTSTANDING') AND (status = 'SUCCESS'))
   )  outs ON ((outs.reservation_id = r.reservation_id) AND (outs.rn = 1)))
   WHERE (r.payment_status = 'PAID')
)  paid_date ON (paid_date.reservation_id = r.reservation_id))