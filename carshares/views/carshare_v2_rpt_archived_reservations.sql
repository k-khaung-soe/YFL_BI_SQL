CREATE OR REPLACE VIEW "carshare_v2_rpt_archived_reservations" AS 
SELECT
  ar.id reservation_id
, CAST(ar.invoice_number AS varchar) invoice_number
, au.username name
, au.email
, replace(CAST(json_extract(au.coporate_account, '$.name') AS varchar), '"', '') company
, replace(CAST(json_extract(au.coporate_account, '$.erp_trade_receivable_number') AS varchar), '"', '') erp_trade_receivable_number
, as4.card_type
, ar.payment_type
, replace(CAST(json_extract(au.profile, '$.erp_account_number') AS varchar), '"', '') erp_account_number
, av.license_plate_number
, av.name vehicle_name
, ar.pickup_at start_date_time
, ar.return_at end_date_time
, ar.extend_start_at extend_start_date_time
, ar.extend_end_at extend_end_date_time
, ar.pickup_at actual_start_date_time
, ar.actual_end_at actual_end_date_time
, CAST(replace(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.daily_rate.unit') AS varchar), '"', ''), ' days', '') AS int) calculated_days
, CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.hourly_rate.unit') AS varchar), '"', ''), ',', 1), 'hr', ''), ' ', '') AS int) calculated_hours
, (CASE WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.hourly_rate.unit') AS varchar), '"', ''), ',', 1) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.hourly_rate.unit') AS varchar), '"', ''), ',', 1), 'min', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.hourly_rate.unit') AS varchar), '"', ''), ',', 2) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.hourly_rate.unit') AS varchar), '"', ''), ',', 2), 'min', ''), ' ', '') AS int) ELSE null END) calculated_mins
, (CASE WHEN (replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.daily_rate.unit') AS varchar), '"', '') IS NOT NULL) THEN CAST(round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.daily_rate.total') AS varchar), '"', '') AS double)) AS int) END) calculated_day_rate
, (CASE WHEN (replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.hourly_rate.unit') AS varchar), '"', '') IS NOT NULL) THEN CAST(round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.hourly_rate.total') AS varchar), '"', '') AS double)) AS int) END) calculated_hour_rate
, ar.km_out
, ar.km_in
, (ar.km_in - ar.km_out) km_usage
, CAST(replace(CAST(json_extract(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate'), '$.price_per_kilometers') AS varchar), '"', '') AS double) km_rate
, CAST(round(CAST(replace(CAST(json_extract(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.kilo_rate'), '$.total') AS varchar), '"', '') AS double)) AS int) km_charges
, (CASE WHEN (ar.extend_start_at IS NOT NULL) THEN 'Yes' ELSE 'No' END) is_extended
, (CASE WHEN (ar.extend_start_at IS NOT NULL) THEN (date_diff('minute', ar.return_at, ar.extend_end_at) / 1440) END) extended_number_of_days
, (CASE WHEN (ar.extend_start_at IS NOT NULL) THEN ((date_diff('minute', ar.return_at, ar.extend_end_at) % 1440) / 60) END) extended_number_of_hours
, (CASE WHEN (ar.extend_start_at IS NOT NULL) THEN ((date_diff('minute', ar.return_at, ar.extend_end_at) % 1440) % 60) END) extended_number_of_minutes
, (CASE WHEN (ar.pickup_location_id <> ar.return_location_id) THEN 'Yes' ELSE 'No' END) is_owr
, (CASE WHEN (round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.owr_rate.total') AS varchar), '"', '') AS double)) > 0) THEN round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.owr_rate.total') AS varchar), '"', '') AS double)) END) owr_rate
, (CASE WHEN (round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.owr_rate.total') AS varchar), '"', '') AS double)) > 0) THEN replace(CAST(json_extract(ar.return_location, '$.name') AS varchar), '"', '') END) owr_location
, replace(CAST(json_extract(ar.pickup_location, '$.name') AS varchar), '"', '') pickup_location
, replace(CAST(json_extract(ar.return_location, '$.name') AS varchar), '"', '') return_location
, 'NA' fuel_out
, 'NA' fuel_in
, (CASE WHEN (round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.fuel_cost') AS varchar), '"', '') AS double)) > 0) THEN 'Yes' ELSE 'No' END) is_fuel_claim
, (CASE WHEN (round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.fuel_cost') AS varchar), '"', '') AS double)) > 0) THEN round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.fuel_cost') AS varchar), '"', '') AS int)) END) fuel_claim_amount
, (CASE WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 1) LIKE '%day%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 1), 'day', ''), ' ', '') AS int) ELSE null END) calculated_late_days
, (CASE WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 1) LIKE '%hr%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 1), 'hr', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 2) LIKE '%hr%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 2), 'hr', ''), ' ', '') AS int) ELSE null END) calculated_late_hours
, (CASE WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 1) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 1), 'min', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 2) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 2), 'min', ''), ' ', '') AS int) WHEN (split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 3) LIKE '%min%') THEN CAST(replace(replace(split_part(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.unit') AS varchar), '"', ''), ',', 3), 'min', ''), ' ', '') AS int) ELSE null END) calculated_late_mins
, CAST(round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.late_fee.total') AS varchar), '"', '') AS double)) AS int) calculated_late_hour_rate
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'PROMO') THEN 'Yes' ELSE 'No' END) is_promo
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'PROMO') THEN replace(CAST(json_extract(json_extract(json_array_get(ar.discounts, 0), '$.discount_detail'), '$.code') AS varchar), '"', '') ELSE null END) promo_code
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'PROMO') THEN replace(CAST(json_extract(json_extract(json_array_get(ar.discounts, 0), '$.discount_detail'), '$.amount') AS varchar), '"', '') ELSE null END) promo_amount
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'PROMO') THEN replace(CAST(json_extract(json_extract(json_array_get(ar.discounts, 0), '$.discount_detail'), '$.type') AS varchar), '"', '') ELSE null END) promo_type
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'PROMO') THEN CAST(round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.discount.total') AS varchar), '"', '') AS double)) AS int) END) promo_rate
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'POINT') THEN 'Yes' ELSE 'No' END) is_point
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'POINT') THEN CAST(replace(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.discount.total') AS varchar), '"(', ''), ')"', '') AS int) END) number_of_points
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'COUPON') THEN 'Yes' ELSE 'No' END) is_coupon
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'COUPON') THEN replace(CAST(json_extract(json_extract(json_array_get(ar.discounts, 0), '$.discount_detail'), '$.discount_type') AS varchar), '"', '') END) coupon_type
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'COUPON') THEN replace(CAST(json_extract(json_array_get(ar.discounts, 0), '$.amount') AS varchar), '"', '') END) coupon_amount
, (CASE WHEN (replace(CAST(JSON_EXTRACT(json_array_get(ar.discounts, 0), '$.discount_type') AS varchar), '"', '') = 'COUPON') THEN replace(CAST(json_extract(json_extract(json_array_get(ar.discounts, 0), '$.discount_detail'), '$.value') AS varchar), '"', '') END) coupon_rate
, CAST(round(CAST(replace(CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.sub_total') AS varchar), '"', '') AS double)) AS int) invoice_amount_before_tax
, CAST(round(CAST(replace(CAST(json_extract(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.commercial_tax'), '$.total') AS varchar), '"', '') AS double)) AS int) tax_amount
, CAST(round(ar.total_price) AS int) total_price
, ar.status status
, ar.type rent_type
, ar.payment_status payment_status
, uph.total points_earned_from_reservation
, (CASE WHEN (ar.transaction_type = 'RENTAL') THEN 'No' ELSE 'Yes' END) rental_like_yes_or_no
, paid_date.paid_date paid_changed_date
, ar.reservation_created_at
, as2.payment_reference reference
, ar.created_at archived_date_time
FROM
  ((((((carshare_v2_staging_archived_reservations_v2 ar
LEFT JOIN carshare_v2_staging_archived_users au ON (au.reservation_id = ar.id))
LEFT JOIN (
   SELECT
     reservation_id
   , payment_type
   , payment_reference
   , row_number() OVER (PARTITION BY reservation_id ORDER BY issued_at DESC) rn
   FROM
     carshare_v2_staging_archived_subscriptions
)  as2 ON ((as2.reservation_id = ar.id) AND (as2.rn = 1)))
LEFT JOIN carshare_v2_staging_archived_vehicles av ON (av.reservation_id = ar.id))
LEFT JOIN (
   SELECT
     reservation_id
   , replace(CAST(json_extract(json_extract(json_extract(json_extract(json_extract(response, '$.payment_capture_result'), '$.sourceOfFunds'), '$.provided'), '$.card'), '$.brand') AS varchar), '"', '') card_type
   , row_number() OVER (PARTITION BY reservation_id ORDER BY issued_at DESC) rn
   FROM
     carshare_v2_staging_archived_subscriptions
)  as4 ON ((as4.reservation_id = ar.id) AND (as4.rn = 1)))
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
)  uph ON ((uph.reference = ar.invoice_number) AND (uph.rn = 1)))
LEFT JOIN (
   SELECT
     r.id
   , r.updated_at
   , (CASE WHEN ((r.id = CAST(rental_paid.id AS int)) AND (NOT (r.id IN (SELECT reservation_id
FROM
  carshare_v2_staging_archived_subscriptions
WHERE ((type = 'OUTSTANDING') AND (status = 'SUCCESS'))
)))) THEN CAST(rental_paid.updated_at AS timestamp) WHEN (r.id = outs.reservation_id) THEN CAST(outs.subscription_updated_at AS timestamp) ELSE null END) paid_date
   FROM
     ((carshare_v2_staging_archived_reservations_v2 r
   LEFT JOIN (
      SELECT
        id
      , json_extract_scalar(json_array_get(history, -1), '$.updated_at') updated_at
      FROM
        carshare_v2_staging_archived_reservations_v2
   )  rental_paid ON (rental_paid.id = r.id))
   LEFT JOIN (
      SELECT
        reservation_id
      , subscription_updated_at
      , ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY updated_at DESC) rn
      FROM
        carshare_v2_staging_archived_subscriptions
      WHERE ((type = 'OUTSTANDING') AND (status = 'SUCCESS'))
   )  outs ON ((outs.reservation_id = r.id) AND (outs.rn = 1)))
   WHERE (r.payment_status = 'PAID')
)  paid_date ON (paid_date.id = ar.id))

