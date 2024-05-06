/* carshare_rpt_reservations_summary */
CREATE OR REPLACE VIEW "carshare_rpt_reservations_summary" AS 
SELECT
  a.*
, (CASE WHEN (a.total_reserved_hour < 24) THEN 'hourly' WHEN (a.total_reserved_hour BETWEEN 25 AND 144) THEN 'days 1 - 6' WHEN (a.total_reserved_hour BETWEEN 145 AND 312) THEN 'days 7-13' ELSE 'days 14 and above' END) reservations_by_hour_day
, (CASE WHEN (a.age BETWEEN 18 AND 24) THEN '18 - 24' WHEN (a.age BETWEEN 25 AND 34) THEN '25 - 34' WHEN (a.age BETWEEN 35 AND 44) THEN '35 - 44' WHEN (a.age BETWEEN 45 AND 54) THEN '45 - 54' WHEN (a.age BETWEEN 55 AND 64) THEN '55 - 64' ELSE '65 and above' END) age_categories
, (CASE WHEN (a.pickup_city <> a.return_city) THEN 'Y' ELSE 'N' END) owr_location
, (a.total_reserved_hour / 24) total_reserved_day
, (CASE WHEN ((a.extend_date_time IS NOT NULL) AND (a.actual_extend_date_time IS NOT NULL)) THEN 'Y' ELSE 'N' END) is_extended
, (CASE WHEN (a.original_end_date_time < a.actual_end_date_time) THEN 'Y' ELSE 'N' END) is_late
FROM
  (
   SELECT
     ar.id
   , ar.user_id
   , ar.confirmation_number
   , ar.invoice_number
   , ar.start_date_time
   , ar.end_date_time
   , ar.original_end_date_time
   , ar.actual_start_date_time
   , ar.actual_end_date_time
   , ar.extend_date_time
   , ar.actual_extend_date_time
   , CAST("json_extract"(ar.pickup_location, '$.name') AS varchar) pickup_location
   , CAST("json_extract"(ar.return_location, '$.name') AS varchar) return_location
   , CAST("json_extract"(ar.pickup_location, '$.city') AS varchar) pickup_city
   , CAST("json_extract"(ar.return_location, '$.city') AS varchar) return_city
   , ar.total_price
   , CAST("json_extract"(ar.vehicle, '$.name') AS varchar) vehicle_name
   , CAST("json_extract"(ar.reserved_vehicle, '$.name') AS varchar) reserved_vehicle_name
   , ar.email
   , ar.contact_number
   , ar.customer_type
   , ar.rent_type
   , ar.km_out
   , ar.km_in
   , ar.fuel_out
   , ar.fuel_in
   , ar.payment_type
   , ar.payment_status
   , ar.replacement_reservation_id
   , ar.created_at
   , ar.updated_at
   , ar.deleted_at
   , ar.loaded_at
   , u.user_name
   , u.account_type
   , u.status
   , p.date_of_birth
   , co.company_id
   , co.approval_emails
   , com.company_name
   , "date_diff"('hour', ar.actual_start_date_time, ar.actual_end_date_time) total_reserved_hour
   , "date_diff"('year', p.date_of_birth, "now"()) age
   FROM
     ((((carshare_staging_archived_reservations ar
   LEFT JOIN carshare_staging_users u ON (u.user_id = ar.user_id))
   LEFT JOIN carshare_staging_profile p ON (p.user_id = u.user_id))
   LEFT JOIN carshare_staging_corporate co ON (co.user_id = u.user_id))
   LEFT JOIN carshare_staging_companies com ON (com.company_id = co.company_id))
)  a

/* carshare_v2_rpt_archived_reservations */
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

/* carshare_v2_rpt_archived_reservations_v1 */
CREATE OR REPLACE VIEW "carshare_v2_rpt_archived_reservations_v1" AS 
SELECT
  ar.confirmation_number
, ar.invoice_number invoice_number
, replace(CAST(json_extract(ar.user, '$.name') AS varchar), '"', '') name
, replace(CAST(json_extract(ar.user, '$.email') AS varchar), '"', '') email
, (CASE WHEN (replace(CAST(json_extract(ar.user, '$.corporate.company_name') AS varchar), '"', '') IS NOT NULL) THEN replace(CAST(json_extract(ar.user, '$.corporate.company_name') AS varchar), '"', '') WHEN (replace(CAST(json_extract(ar.user, '$.corporate.company_name') AS varchar), '"', '') IS NULL) THEN replace(CAST(json_extract(ar.user, '$.corporate.company.name') AS varchar), '"', '') END) company
, replace(CAST(json_extract(ar.payment, '$.brand') AS varchar), '"', '') card_type
, ar.payment_type payment_type
, replace(CAST(JSON_EXTRACT(ar.user, '$.nfc_id') AS varchar), '"', '') tap_card_number
, concat(replace(CAST(JSON_EXTRACT(ar.vehicle, '$.name') AS varchar), '"', ''), ' (', replace(CAST(JSON_EXTRACT(ar.vehicle, '$.license_plate_number') AS varchar), '"', ''), ')') vehicle_name_license
, replace(CAST(JSON_EXTRACT(ar.vehicle, '$.name') AS varchar), '"', '') vehicle_name
, ar.start_date_time start_date_time
, ar.end_date_time end_date_time
, ar.actual_start_date_time actual_start_date_time
, ar.actual_end_date_time actual_end_date_time
, CAST(JSON_EXTRACT(ar.summary_params, '$.date_interval.days') AS double) calculated_day
, CAST(JSON_EXTRACT(ar.summary_params, '$.date_interval.h') AS double) calculated_hour
, CAST(JSON_EXTRACT(ar.summary_params, '$.daily_rate') AS double) calculated_day_rate
, CAST(JSON_EXTRACT(ar.summary_params, '$.hourly_rate') AS double) calculated_hour_rate
, ar.km_out km_out
, ar.km_in km_in
, (ar.km_in - ar.km_out) km_usage
, CAST(JSON_EXTRACT(ar.summary_params, '$.km_rate') AS double) km_rate
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.extend_fee') AS varchar), '"', '') AS double) > 0) THEN 'Yes' ELSE 'No' END) is_extended
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.extend_fee') AS varchar), '"', '') AS double) > 0) THEN CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.extend_fee') AS varchar), '"', '') AS double) ELSE 0 END) extended_fee
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.owr_rate') AS varchar), '"', '') AS double) > 0) THEN 'Yes' ELSE 'No' END) is_owr
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.owr_rate') AS varchar), '"', '') AS double) > 0) THEN CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.owr_rate') AS varchar), '"', '') AS double) ELSE 0 END) owr_rate
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.owr_rate') AS varchar), '"', '') AS double) > 0) THEN replace(CAST(JSON_EXTRACT(ar.return_location, '$.name') AS varchar), '"', '') END) owr_location
, (CASE WHEN (ar.is_transit IS NOT NULL) THEN 'Yes' ELSE 'No' END) is_transit
, (CASE WHEN (ar.is_transit IS NOT NULL) THEN l.name END) transit_location
, replace(CAST(JSON_EXTRACT(ar.pickup_location, '$.name') AS varchar), '"', '') pickup_location
, replace(CAST(JSON_EXTRACT(ar.return_location, '$.name') AS varchar), '"', '') return_location
, (CASE WHEN (ar.damage_reports <> '[]') THEN 'Yes' ELSE 'No' END) is_damage
, ar.fuel_out fuel_out
, ar.fuel_in fuel_in
, (CASE WHEN (ar.receives <> '[]') THEN 'Yes' ELSE 'No' END) is_fuel_claim
, (CASE WHEN (ar.receives <> '[]') THEN CAST(replace(CAST(json_extract(JSON_EXTRACT(ar.receives, '$[0]'), '$.amount') AS varchar), '"', '') AS double) ELSE 0 END) fuel_claim
, CAST(((COALESCE((CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.late_interval.days') AS varchar), '"', '') AS double) * 24), 0) + COALESCE(CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.late_interval.h') AS varchar), '"', '') AS double), 0)) + COALESCE((CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.late_interval.i') AS varchar), '"', '') AS double) / 60), 0)) AS double) calculated_late_hour
, CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.total_late_fee') AS varchar), '"', '') AS double) calculated_late_hour_rate
, (CASE WHEN (ar.additional_charges <> '[]') THEN 'Yes' ELSE 'No' END) is_recharge
, (CASE WHEN (ar.additional_charges <> '[]') THEN (((((((((COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[0].amount') AS double), 0) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[1].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[2].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[3].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[4].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[5].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[6].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[7].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[8].amount') AS double), 0)) + COALESCE(CAST(JSON_EXTRACT(additional_charges, '$[9].amount') AS double), 0)) ELSE 0 END) recharge_fee
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.promotion_rate') AS varchar), '"', '') AS double) > 0) THEN 'Yes' ELSE 'No' END) is_promo
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.promotion_rate') AS varchar), '"', '') AS double) > 0) THEN p.code END) promo_code
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.promotion_rate') AS varchar), '"', '') AS double) > 0) THEN p.discount END) promo_amount
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.promotion_rate') AS varchar), '"', '') AS double) > 0) THEN p.discount_type END) promo_type
, (CASE WHEN (CAST(replace(CAST(JSON_EXTRACT(ar.summary_params, '$.promotion_rate') AS varchar), '"', '') AS double) > 0) THEN replace(CAST(JSON_EXTRACT(ar.summary_params, '$.promotion_rate') AS varchar), '"', '') END) promo_rate
, ar.total_price total_price
, 'Closed' status
, ar.rent_type rent_type
, ar.payment_status payment_status
, ar.confirmed_at reservation_created_date_time
, ar.reference reference_or_card_type
, ar.created_at archived_date_time
FROM
  ((carshare_v2_staging_archived_reservations_v1 ar
LEFT JOIN carshare_staging_locations l ON (l.location_id = ar.is_transit))
LEFT JOIN carshare_staging_promotion p ON (p.promotion_id = CAST(replace(CAST(JSON_EXTRACT(ar.promotion, '$.id') AS varchar), '"', '') AS int)))

/* carshare_v2_rpt_branch_utilization */
CREATE OR REPLACE VIEW "carshare_v2_rpt_branch_utilization" AS 
SELECT
  created_date
, location_name
, region
, status location_status
, COALESCE(assign_fleet, 0) assign_fleet
, COALESCE(available_utilization_hour, 0) available_utilization_hour
, COALESCE(idle_in_yard, 0) idle_in_yard
, COALESCE(on_rent, 0) on_rent
, COALESCE(replacement, 0) replacement
, COALESCE(drive_car, 0) drive_car
, COALESCE(cleaning_process, 0) cleaning_process
, COALESCE(relocation, 0) relocation
, COALESCE(transit, 0) transit
, COALESCE(panel_shop, 0) panel_shop
, COALESCE(maintenance, 0) maintenance
, COALESCE(pending, 0) pending
, COALESCE(overdue, 0) overdue
, concat(COALESCE(CAST(round(((utilization / available_utilization_hour) * 100)) AS varchar), '0'), '%') utilization
, concat(COALESCE(CAST(round(((drive_car / available_utilization_hour) * 100)) AS varchar), '0'), '%') drive_car_utilization
, concat(COALESCE(CAST(round(((ops_utilization / available_utilization_hour) * 100)) AS varchar), '0'), '%') ops_utilization
, concat(COALESCE(CAST(round(((idle_in_yard / available_utilization_hour) * 100)) AS varchar), '0'), '%') lost_utilization
FROM
  (
   SELECT
     (CASE WHEN (du.created_date IS NOT NULL) THEN CAST(du.created_date AS date) ELSE csl.date END) created_date
   , csl.name location_name
   , csl.region
   , csl.status
   , count(DISTINCT du.vehicle_id) assign_fleet
   , sum(du.available_utilization_hour) available_utilization_hour
   , sum(du.idle_in_yard) idle_in_yard
   , sum(du.on_rent) on_rent
   , sum(du.replacement) replacement
   , sum(du.drive_car) drive_car
   , sum(du.cleaning_process) cleaning_process
   , sum(du.relocation) relocation
   , sum(du.transit) transit
   , sum(du.panel_shop) panel_shop
   , sum(du.maintenance) maintenance
   , sum(du.pending) pending
   , sum(du.overdue) overdue
   , sum(((du.on_rent + du.replacement) + du.overdue)) utilization
   , sum((((((du.cleaning_process + du.relocation) + du.transit) + du.panel_shop) + du.maintenance) + du.pending)) ops_utilization
   FROM
     (carshare_v2_rpt_daily_utilization du
   RIGHT JOIN (
      SELECT
        dt.date
      , l.name
      , l.region
      , l.status
      FROM
        ((
         SELECT DISTINCT CAST(start_time AS date) date
         FROM
           carshare_v2_staging_vehicle_histories
      )  dt
      CROSS JOIN (
         SELECT
           name
         , region
         , status
         FROM
           carshare_v2_staging_locations
      )  l)
      WHERE (dt.date IS NOT NULL)
   )  csl ON ((csl.name = du.location_name) AND (CAST(du.created_date AS date) = csl.date)))
   GROUP BY 1, 2, 3, 4
) 
ORDER BY 1 ASC

/* carshare_v2_rpt_branch_utilization_phoo */
CREATE OR REPLACE VIEW "carshare_v2_rpt_branch_utilization_phoo" AS 
SELECT
  created_date
, location_name
, region
, COALESCE(assign_fleet, 0) assign_fleet
, COALESCE(available_utilization_hour, 0) available_utilization_hour
, COALESCE(idle_in_yard, 0) idle_in_yard
, COALESCE(on_rent, 0) on_rent
, COALESCE(replacement, 0) replacement
, COALESCE(drive_car, 0) drive_car
, COALESCE(cleaning_process, 0) cleaning_process
, COALESCE(relocation, 0) relocation
, COALESCE(transit, 0) transit
, COALESCE(panel_shop, 0) panel_shop
, COALESCE(maintenance, 0) maintenance
, COALESCE(pending, 0) pending
, COALESCE(overdue, 0) overdue
, concat(COALESCE(CAST(round(((utilization / available_utilization_hour) * 100)) AS varchar), '0'), '%') utilization
, concat(COALESCE(CAST(round(((drive_car / available_utilization_hour) * 100)) AS varchar), '0'), '%') drive_car_utilization
, concat(COALESCE(CAST(round(((ops_utilization / available_utilization_hour) * 100)) AS varchar), '0'), '%') ops_utilization
, concat(COALESCE(CAST(round(((idle_in_yard / available_utilization_hour) * 100)) AS varchar), '0'), '%') lost_utilization
FROM
  (
   SELECT
     (CASE WHEN (du.created_date IS NOT NULL) THEN CAST(du.created_date AS date) ELSE csl.date END) created_date
   , csl.name location_name
   , csl.region
   , count(DISTINCT du.vehicle_id) assign_fleet
   , sum(du.available_utilization_hour) available_utilization_hour
   , sum(du.idle_in_yard) idle_in_yard
   , sum(du.on_rent) on_rent
   , sum(du.replacement) replacement
   , sum(du.drive_car) drive_car
   , sum(du.cleaning_process) cleaning_process
   , sum(du.relocation) relocation
   , sum(du.transit) transit
   , sum(du.panel_shop) panel_shop
   , sum(du.maintenance) maintenance
   , sum(du.pending) pending
   , sum(du.overdue) overdue
   , sum(((du.on_rent + du.replacement) + du.overdue)) utilization
   , sum((((((du.cleaning_process + du.relocation) + du.transit) + du.panel_shop) + du.maintenance) + du.pending)) ops_utilization
   FROM
     (carshare_v2_rpt_daily_utilization du
   RIGHT JOIN (
      SELECT
        dt.date
      , l.name
      , l.region
      FROM
        ((
         SELECT DISTINCT CAST(start_time AS date) date
         FROM
           carshare_v2_staging_vehicle_histories
         WHERE (CAST(start_time AS date) >= CAST('2022-10-01' AS date))
      )  dt
      CROSS JOIN (
         SELECT
           name
         , region
         FROM
           carshare_v2_staging_locations
         WHERE (status = 'ENABLE')
      )  l)
   )  csl ON ((csl.name = du.location_name) AND (CAST(du.created_date AS date) = csl.date)))
   GROUP BY 1, 2, 3
) 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
ORDER BY 1 ASC

/* carshare_v2_rpt_call_center_users_management */
CREATE OR REPLACE VIEW "carshare_v2_rpt_call_center_users_management" AS 
SELECT
  u.user_id user_id
, u.username name
, u.email email
, p.date_of_birth date_of_birth
, p.contact_number contact_number
, u.account_type account_type
, p.erp_account_number erp_account_number
, cp.erp_trade_receivable_number erp_trade_receivable_number
, dl.expiration_date license_expiry_date
, (CASE WHEN (u.verified_at IS NOT NULL) THEN 'Yes' ELSE 'No' END) confirmed
, (CASE WHEN (u.status = 'APPROVED') THEN 'Yes' ELSE 'No' END) approved
, (CASE WHEN (u.status = 'APPROVED') THEN 'Active' WHEN (u.status IN ('PENDING', 'TERMINATED')) THEN 'Inactive' END) user_status
, replace(replace(p.address, chr(10), ' '), chr(13), ' ') address
, cp.name company_name
, u.created_at joining_date
, DATE_DIFF('year', CAST(p.date_of_birth AS date), current_date) calculated_age
, dl.issue_country license_issuing_country
, array_join(array_agg(DISTINCT bi.brand), '/') payment
, u.updated_at overview_last_updated_at
, p.updated_at corporate_info_last_updated_at
, p.updated_at profile_info_last_updated_at
, dl.updated_at license_info_last_updated_at
, ic.updated_at identification_info_last_updated_at
, max(bi.updated_at) payment_info_last_updated_at
, u.deleted_at user_deleted_at
FROM
  ((((((carshare_v2_staging_users u
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_licenses dl ON (dl.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_corporate_accounts co ON (co.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_companies cp ON (cp.company_id = co.company_id))
LEFT JOIN carshare_v2_staging_payments bi ON (bi.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_identifications ic ON (ic.user_id = u.user_id))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23, 25

/* carshare_v2_rpt_cancel_reservations_details */
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

/* carshare_v2_rpt_changed_vehicle_reservations_rates */
CREATE OR REPLACE VIEW "carshare_v2_rpt_changed_vehicle_reservations_rates" AS 
SELECT
  r.reservation_id
, r.invoice_number
, u.username
, u.email
, c.name company_name
, c.erp_trade_receivable_number
, r.payment_type
, p.erp_account_number
, v.name reserved_vehicle_name
, v.license_plate_number reserved_license_plate_number
, (CAST(COALESCE(rev_rates.hourly_rate, rev_rates.daily_rate) AS double) * rev_rates.exchange_rate) reserved_vehicle_based_rate
, (CAST(rev_rates.km_charges AS double) * rev_rates.exchange_rate) reserved_vehicle_km_charges
, r.created_at reserved_date
, chg_v.name changed_vehicle_name
, chg_v.license_plate_number changed_vehicle_license_plate_number
, (CAST(COALESCE(chg_rates.hourly_rate, chg_rates.daily_rate) AS double) * chg_rates.exchange_rate) changed_vehicle_based_rate
, (CAST(chg_rates.km_charges AS double) * chg_rates.exchange_rate) changed_vehicle_km_charges
FROM
  ((((((((carshare_v2_staging_reservations r
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_companies c ON (c.company_id = ca.company_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_vehicles v ON (v.vehicle_id = r.reserved_vehicle_id))
LEFT JOIN carshare_v2_staging_vehicles chg_v ON (chg_v.vehicle_id = r.vehicle_id))
LEFT JOIN (
   SELECT
     r1.reservation_id
   , json_extract(r1.rate, '$.hourly_rate.rate') hourly_rate
   , json_extract(r1.rate, '$.daily_rate.rate') daily_rate
   , json_extract(r1.rate, '$.acriss_rate.price_per_kilometers') km_charges
   , er.rate exchange_rate
   , row_number() OVER (PARTITION BY r1.reservation_id ORDER BY r1.id ASC) rn
   FROM
     (carshare_v2_staging_reservation_rates r1
   LEFT JOIN carshare_v2_staging_exchange_rates er ON (CAST(json_extract(r1.rate, '$.exchangeRate') AS int) = er.exchange_rate_id))
)  rev_rates ON ((rev_rates.reservation_id = r.reservation_id) AND (rev_rates.rn = 1)))
LEFT JOIN (
   SELECT
     r1.reservation_id
   , json_extract(r1.rate, '$.hourly_rate.rate') hourly_rate
   , json_extract(r1.rate, '$.daily_rate.rate') daily_rate
   , json_extract(r1.rate, '$.acriss_rate.price_per_kilometers') km_charges
   , er.rate exchange_rate
   , row_number() OVER (PARTITION BY r1.reservation_id ORDER BY r1.id DESC) rn
   FROM
     (carshare_v2_staging_reservation_rates r1
   LEFT JOIN carshare_v2_staging_exchange_rates er ON (CAST(json_extract(r1.rate, '$.exchangeRate') AS int) = er.exchange_rate_id))
)  chg_rates ON ((chg_rates.reservation_id = r.reservation_id) AND (chg_rates.rn = 1)))
WHERE (r.reserved_vehicle_id <> r.vehicle_id)

/* carshare_v2_rpt_corporate_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_corporate_details" AS 
SELECT
  c.name company_name
, c.company_reg_id
, c.erp_trade_receivable_number
, CAST(c.credit_value AS int) credit_value
, c.credit_terms
, c.industry
, c.number_of_employees
, replace(replace(c.address, chr(10), ' '), chr(13), ' ') address
, c.city
, c.phone phone_number
, c.key_account_person
, c.contact_person
, c.updated_at
, count(DISTINCT ca.user_id) number_of_users
, r.total_reservations
, r.total_revenue
, CAST(max(r1.created_at) AS date) last_rented_date
, round(CAST(os.amount AS bigint)) outstanding_amount
, recharge.number_of_recharge_invoices
, recharge.total_recharge_amount
, c.created_at
FROM
  (((((carshare_v2_staging_companies c
LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.company_id = c.company_id))
LEFT JOIN (
   SELECT
     com.company_id
   , count(DISTINCT re.invoice_number) total_reservations
   , round(CAST(sum(re.total_price) AS bigint)) total_revenue
   FROM
     ((carshare_v2_staging_reservations re
   LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.user_id = re.user_id))
   LEFT JOIN carshare_v2_staging_companies com ON (com.company_id = ca.company_id))
   WHERE ((re.deleted_at IS NULL) AND (re.status = 'CLOSED'))
   GROUP BY 1
)  r ON (r.company_id = c.company_id))
LEFT JOIN carshare_v2_staging_reservations r1 ON (ca.user_id = r1.user_id))
LEFT JOIN (
   SELECT
     a.company_id
   , sum(a.amount) amount
   FROM
     (
      SELECT
        ca2.company_id
      , (CASE WHEN (or1.reservation_sub_invoice_id IN (SELECT sub_invoice_id
FROM
  carshare_v2_staging_reservation_sub_invoices rsi
WHERE ((rsi.transaction_type <> 'REFUND') AND (rsi.payment_status <> 'PAID'))
)) THEN or1.amount ELSE or1.amount END) amount
      FROM
        ((carshare_v2_staging_outstanding_reservations or1
      LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = or1.reservation_id))
      LEFT JOIN carshare_v2_staging_corporate_accounts ca2 ON (ca2.user_id = r.user_id))
   )  a
   GROUP BY 1
)  os ON (os.company_id = ca.company_id))
LEFT JOIN (
   SELECT
     a.company_id
   , count(DISTINCT a.sub_invoice_id) number_of_recharge_invoices
   , sum(a.amount) total_recharge_amount
   FROM
     (
      SELECT
        c.company_id
      , rsi.sub_invoice_id
      , (CASE WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 0)) THEN rac.amount WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 1)) THEN (rac.amount + (rac.amount * 5E-2)) END) amount
      FROM
        ((((carshare_v2_staging_reservations r
      LEFT JOIN carshare_v2_staging_reservation_sub_invoices rsi ON (rsi.reservation_id = r.reservation_id))
      LEFT JOIN carshare_v2_staging_reservation_additional_charges rac ON (rac.reservation_sub_invoice_id = rsi.sub_invoice_id))
      LEFT JOIN carshare_v2_staging_corporate_accounts ca ON (ca.user_id = r.user_id))
      LEFT JOIN carshare_v2_staging_companies c ON (c.company_id = ca.company_id))
      WHERE ((rsi.transaction_type = 'RECHARGE') AND (r.type = 'CORPORATE'))
   )  a
   GROUP BY 1
)  recharge ON (recharge.company_id = c.company_id))
WHERE (c.deleted_at IS NULL)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 18, 19, 20, 21

/* carshare_v2_rpt_corporate_inquiries */
CREATE OR REPLACE VIEW "carshare_v2_rpt_corporate_inquiries" AS 
SELECT
  type inquiry_type
, replace(CAST(json_extract(detail, '$.company_name') AS varchar), '"', '') company_name
, replace(CAST(json_extract(detail, '$.industry') AS varchar), '"', '') industry
, replace(CAST(json_extract(detail, '$.contact_person') AS varchar), '"', '') contact_person
, replace(CAST(json_extract(detail, '$.job_title') AS varchar), '"', '') job_title
, email
, phone phone
, status
, created_at
FROM
  carshare_v2_staging_enquiries
WHERE (user_type = 'CORPORATE')

/* carshare_v2_rpt_coupon_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_coupon_details" AS 
SELECT
  c.code
, c.type coupon_category
, CAST(c.value AS double) discount
, c.start_date
, c.end_date
, c.discount_type
, c.expiration_date
, count(cu.coupon_id) number_of_coupon_distributed
, count((CASE WHEN (cu.status = 'INACTIVE') THEN cu.coupon_id END)) number_of_coupon_used
, sum((CASE WHEN (cu.status = 'INACTIVE') THEN CAST(c.value AS int) END)) total_amount_discounted
, max(cu.updated_at) updated_at
FROM
  (carshare_v2_staging_coupons c
LEFT JOIN carshare_v2_staging_coupon_user cu ON (cu.coupon_id = c.coupon_id))
WHERE (c.deleted_at IS NULL)
GROUP BY 1, 2, 3, 4, 5, 6, 7

/* carshare_v2_rpt_damage_details */
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

/* carshare_v2_rpt_fx_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_fx_details" AS 
SELECT
  currency_code
, rate
, created_at
, updated_at
FROM
  carshare_v2_staging_exchange_rates

/* carshare_v2_rpt_invoices_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_invoices_details" AS 
SELECT
  r.reservation_id
, rsi.invoice_number invoice_number
, rsi.transaction_type
, CAST(round(sum((CASE WHEN (rsi.sub_invoice_id = or2.reservation_sub_invoice_id) THEN or2.amount WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 0)) THEN rac.amount WHEN ((rsi.sub_invoice_id = rac.reservation_sub_invoice_id) AND (rsi.apply_tax = 1)) THEN (rac.amount + (rac.amount * 5E-2)) END))) AS int) amount
, r.invoice_number reservation_invoice_number
, rsi.payment_status
, rsi.created_at created_at
, u.username customer_name
, p.erp_account_number
, rsi.memo
, rsi.apply_tax
, rsi.updated_at
FROM
  (((((carshare_v2_staging_reservation_sub_invoices rsi
LEFT JOIN carshare_v2_staging_outstanding_reservations or2 ON (or2.reservation_sub_invoice_id = rsi.sub_invoice_id))
LEFT JOIN carshare_v2_staging_reservation_additional_charges rac ON (rac.reservation_sub_invoice_id = rsi.sub_invoice_id))
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = rsi.reservation_id))
LEFT JOIN carshare_v2_staging_users u ON (u.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
GROUP BY 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12

/* carshare_v2_rpt_kbzpay_reservations */
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

/* carshare_v2_rpt_locations_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_locations_details" AS 
SELECT
  name
, replace(replace(address, chr(10), ' '), chr(13), ' ') address
, contact_number contact_number
, city
, region
, status location_status
, contract_start_date
, contract_end_date
, updated_at
FROM
  carshare_v2_staging_locations
WHERE (deleted_at IS NULL)

/* carshare_v2_rpt_mpu_payment_reservations */
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

/* carshare_v2_rpt_outstanding_details */
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

/* carshare_v2_rpt_points_and_coupons_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_points_and_coupons_details" AS 
SELECT
  user_info.user_id
, user_info.username
, user_info.erp_account_number
, user_info.payment_type
, (CASE WHEN (uph.point_type = 'EARNED') THEN uph.created_at END) points_earned_date
, (CASE WHEN (uph.point_type = 'EARNED') THEN uph.total END) points_earned
, (CASE WHEN (uph.point_type = 'EARNED') THEN uph.expiry_at END) points_expiry_date
, (CASE WHEN (uph.point_type = 'EARNED') THEN po.point_type END) points_earned_type
, (CASE WHEN (uph.point_type = 'USED') THEN uph.total END) points_used
, (CASE WHEN (uph.point_type = 'USED') THEN uph.created_at END) points_used_date
, null coupon_awarded_date
, null coupon_code
, null coupon_type
, null coupon_expiry_date
, null coupon_status
, null coupon_used
, null coupon_used_date
FROM
  (((
   SELECT
     u.user_id user_id
   , u.username
   , p.erp_account_number
   , array_join(array_agg(DISTINCT p2.brand), '/') payment_type
   FROM
     ((carshare_v2_staging_users u
   LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
   LEFT JOIN carshare_v2_staging_payments p2 ON (p2.user_id = u.user_id))
   GROUP BY 1, 2, 3
)  user_info
LEFT JOIN carshare_v2_staging_user_point_histories uph ON (uph.user_id = user_info.user_id))
LEFT JOIN carshare_v2_staging_points po ON (po.point_id = uph.point_id))
UNION SELECT
  user_info.user_id
, user_info.username
, user_info.erp_account_number
, user_info.payment_type
, null points_earned_date
, null points_earned
, null points_expiry_date
, null points_earned_type
, null points_used
, null points_used_date
, cu.created_at coupon_awarded_date
, c.code coupon_code
, c.type coupon_type
, c.expiration_date coupon_expiry_date
, cu.status coupon_status
, (CASE WHEN (cu.status = 'INACTIVE') THEN c.code END) coupon_used
, (CASE WHEN (cu.updated_at IS NOT NULL) THEN cu.updated_at END) coupon_used_date
FROM
  (((
   SELECT
     u.user_id user_id
   , u.username
   , p.erp_account_number
   , array_join(array_agg(DISTINCT p2.brand), '/') payment_type
   FROM
     ((carshare_v2_staging_users u
   LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
   LEFT JOIN carshare_v2_staging_payments p2 ON (p2.user_id = u.user_id))
   GROUP BY 1, 2, 3
)  user_info
LEFT JOIN carshare_v2_staging_coupon_user cu ON (cu.user_id = user_info.user_id))
LEFT JOIN carshare_v2_staging_coupons c ON (c.coupon_id = cu.coupon_id))
ORDER BY 1 ASC

/* carshare_v2_rpt_promotion_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_promotion_details" AS 
SELECT
  p.code
, p.category category_type
, p.type discount_type
, CAST(p.amount AS double) discount
, p.start_date
, p.end_date
, p.expiration_date
, CAST(p.created_at AS date) created_date
, count(json_extract(rd.discount_detail, '$.id')) number_of_usage
, sum(round(CAST(json_extract(rr.rate, '$.discount.total') AS int))) total_amount_discounted
, max(rd.updated_at) updated_at
FROM
  (((carshare_v2_staging_promotions p
LEFT JOIN (
   SELECT
     reservation_id
   , discount_detail
   , updated_at
   , row_number() OVER (PARTITION BY reservation_id ORDER BY created_at DESC) rn
   FROM
     carshare_v2_staging_reservation_discounts
   WHERE (discount_type = 'PROMO')
)  rd ON ((CAST(json_extract(rd.discount_detail, '$.id') AS int) = p.promotion_id) AND (rd.rn = 1)))
LEFT JOIN (
   SELECT
     reservation_id
   , rate
   , row_number() OVER (PARTITION BY reservation_id ORDER BY id DESC) rn
   FROM
     carshare_v2_staging_reservation_rates
)  rr ON ((rr.reservation_id = rd.reservation_id) AND (rr.rn = 1)))
LEFT JOIN carshare_v2_staging_reservations r ON ((r.reservation_id = rd.reservation_id) AND (r.status = 'CLOSED')))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

/* carshare_v2_rpt_rate_book_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_rate_book_details" AS 
SELECT
  a.code acriss_code
, CAST(r.hourly_2000_0559 AS decimal(10, 4)) hourly_2000_0559
, CAST(r.hourly_0600_0859 AS decimal(10, 4)) hourly_0600_0859
, CAST(r.hourly_0900_1659 AS decimal(10, 4)) hourly_0900_1659
, CAST(r.hourly_1700_1959 AS decimal(10, 4)) hourly_1700_1959
, CAST(r.days_1_6 AS decimal(10, 4)) days_1_6
, CAST(r.days_7_13 AS decimal(10, 4)) days_7_13
, CAST(r.days_14_20 AS decimal(10, 4)) days_14_20
, CAST(r.days_21_30 AS decimal(10, 4)) days_21_30
, CAST(r.price_per_kilometers AS decimal(10, 4)) price_per_kilometers
, r.demand
, r.status
, r.created_at created_at
, r.updated_at
FROM
  (carshare_v2_staging_rates r
LEFT JOIN carshare_v2_staging_acrisses a ON (a.acriss_id = r.acriss_id))
WHERE (r.deleted_at IS NULL)

/* carshare_v2_rpt_reservation_additional_charges */
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

/* carshare_v2_rpt_reservation_rates_ref */
CREATE OR REPLACE VIEW "carshare_v2_rpt_reservation_rates_ref" AS 
SELECT
  ar.id reservation_id
, acr.code
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.days_1_6') AS double) days_1_6
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.days_7_13') AS double) days_7_13
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.days_14_20') AS double) days_14_20
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.days_21_30') AS double) days_21_30
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.hourly_0600_0859') AS double) hourly_0600_0859
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.hourly_0900_1659') AS double) hourly_0900_1659
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.hourly_1700_1959') AS double) hourly_1700_1959
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.hourly_2000_0559') AS double) hourly_2000_0559
, CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.price_per_kilometers') AS double) price_per_kilometers
FROM
  (carshare_v2_staging_archived_reservations_v2 ar
LEFT JOIN carshare_v2_staging_acrisses acr ON (acr.acriss_id = CAST(json_extract(json_array_get(ar.reservation_rates, 0), '$.rate.acriss_rate.acriss_id') AS int)))
UNION SELECT
  ar.reservation_id
, acr.code
, CAST(json_extract(rr.rate, '$.acriss_rate.days_1_6') AS double) days_1_6
, CAST(json_extract(rr.rate, '$.acriss_rate.days_7_13') AS double) days_7_13
, CAST(json_extract(rr.rate, '$.acriss_rate.days_14_20') AS double) days_14_20
, CAST(json_extract(rr.rate, '$.acriss_rate.days_21_30') AS double) days_21_30
, CAST(json_extract(rr.rate, '$.acriss_rate.hourly_0600_0859') AS double) hourly_0600_0859
, CAST(json_extract(rr.rate, '$.acriss_rate.hourly_0900_1659') AS double) hourly_0900_1659
, CAST(json_extract(rr.rate, '$.acriss_rate.hourly_1700_1959') AS double) hourly_1700_1959
, CAST(json_extract(rr.rate, '$.acriss_rate.hourly_2000_0559') AS double) hourly_2000_0559
, CAST(json_extract(rr.rate, '$.acriss_rate.price_per_kilometers') AS double) price_per_kilometers
FROM
  ((carshare_v2_staging_reservations ar
LEFT JOIN (
   SELECT
     reservation_id
   , rate
   , row_number() OVER (PARTITION BY reservation_id ORDER BY id DESC) rn
   FROM
     carshare_v2_staging_reservation_rates
)  rr ON ((ar.reservation_id = rr.reservation_id) AND (rr.rn = 1)))
LEFT JOIN carshare_v2_staging_acrisses acr ON (acr.acriss_id = CAST(json_extract(rr.rate, '$.acriss_rate.acriss_id') AS int)))
ORDER BY 1 ASC

/* carshare_v2_rpt_reservations_details */
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

/* carshare_v2_rpt_reservations_details_summary */
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

/* carshare_v2_rpt_reviewed_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_reviewed_details" AS 
SELECT
  r.reservation_id
, r.invoice_number invoice_number
, u.username name
, u.email
, p.erp_account_number
, title topic
, CAST(rating AS int) rating
, replace(replace(comment, chr(10), ' '), chr(13), ' ') description
, CAST(uph.total AS int) points
, CAST(rf.created_at AS date) created_at
FROM
  ((((carshare_v2_staging_reservation_feedbacks rf
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = rf.reservation_id))
LEFT JOIN carshare_v2_staging_users u ON (r.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_user_point_histories uph ON (uph.reference = r.invoice_number))
WHERE (uph.point_id = 1)
UNION SELECT
  rf.id reservation_id
, rf.invoice_number invoice_number
, u.username name
, u.email
, CAST(json_extract(u.profile, '$.erp_account_number') AS varchar) erp_account_number
, CAST(json_extract(reservation_feedbacks, '$.title') AS varchar) topic
, CAST(json_extract(reservation_feedbacks, '$.rating') AS int) rating
, CAST(json_extract(reservation_feedbacks, '$.comment') AS varchar) description
, CAST(uph.total AS int) points
, CAST('2999-12-31' AS date) created_at
FROM
  ((carshare_v2_staging_archived_reservations_v2 rf
LEFT JOIN carshare_v2_staging_archived_users u ON (CAST(json_extract(rf.reservation_feedbacks, '$.reservation_id') AS bigint) = u.reservation_id))
LEFT JOIN carshare_v2_staging_user_point_histories uph ON (uph.reference = rf.invoice_number))
WHERE (uph.point_id = 1)

/* carshare_v2_rpt_vehicles_details */
CREATE OR REPLACE VIEW "carshare_v2_rpt_vehicles_details" AS 
SELECT
  v.name car_name
, ac.code acriss_code
, l.name location_name
, v.license_plate_number
, v.status
, v.car_share_status
, CAST(v.odometer AS int) odometer
, replace(CAST(json_extract(vehicle_attributes, '$.value[0]') AS varchar), '"', '') year
, replace(CAST(json_extract(vehicle_attributes, '$.value[1]') AS varchar), '"', '') fuel_type
, replace(CAST(json_extract(vehicle_attributes, '$.value[2]') AS varchar), '"', '') vehicle_category
, replace(CAST(json_extract(vehicle_attributes, '$.value[3]') AS varchar), '"', '') transmission
, replace(CAST(json_extract(vehicle_attributes, '$.value[4]') AS varchar), '"', '') color
, replace(CAST(json_extract(vehicle_attributes, '$.value[5]') AS varchar), '"', '') engine
, replace(CAST(json_extract(vehicle_attributes, '$.value[6]') AS varchar), '"', '') build_type
, replace(CAST(json_extract(vehicle_attributes, '$.value[7]') AS varchar), '"', '') steering
, replace(CAST(json_extract(vehicle_attributes, '$.value[8]') AS varchar), '"', '') registered_state
, replace(CAST(json_extract(vehicle_attributes, '$.value[9]') AS varchar), '"', '') interior_color
, replace(CAST(json_extract(vehicle_attributes, '$.value[10]') AS varchar), '"', '') grade
, v.created_at created_at
, v.deleted_at deleted_at
, replace(CAST(json_extract(vehicle_attributes, '$.value[11]') AS varchar), '"', '') registration_date
, replace(CAST(json_extract(vehicle_attributes, '$.value[12]') AS varchar), '"', '') registration_renewal_due_date
, replace(CAST(json_extract(vehicle_attributes, '$.value[13]') AS varchar), '"', '') date_due_off_fleet
FROM
  ((carshare_v2_staging_vehicles v
LEFT JOIN carshare_v2_staging_acrisses ac ON (ac.acriss_id = v.acriss_id))
LEFT JOIN carshare_v2_staging_locations l ON (l.location_id = v.location_id))

/* carshare_v2_rpt_wave_reservations */
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

/* carshare_v2_playground_reservations */
CREATE OR REPLACE VIEW "carshare_v2_playground_reservations" AS 
SELECT
  invoice_number
, name
, email
, company
, erp_trade_receivable_number
, card_type
, payment_type
, erp_account_number
, license_plate_number
, vehicle_name
, start_date_time
, end_date_time
, actual_start_date_time
, actual_end_date_time
, calculated_days
, calculated_hours
, calculated_mins
, calculated_day_rate
, calculated_hour_rate
, km_out
, km_in
, km_usage
, km_rate
, km_charges
, is_extended
, extended_number_of_days
, extended_number_of_hours
, extended_number_of_minutes
, is_owr
, owr_rate
, owr_location
, pickup_location
, return_location
, fuel_out
, fuel_in
, is_fuel_claim
, fuel_claim_amount
, calculated_late_days
, calculated_late_hours
, calculated_late_mins
, calculated_late_hour_rate
, is_promo
, promo_code
, promo_amount
, promo_type
, promo_rate
, is_point
, CAST(number_of_points AS int) number_of_points
, is_coupon
, coupon_type
, CAST(coupon_amount AS int) coupon_amount
, coupon_rate
, invoice_amount_before_tax
, tax_amount
, total_price
, status
, rent_type
, payment_status
, points_earned_from_reservation
, reservation_created_at
FROM
  carshare_v2_rpt_reservations_details_summary
UNION SELECT
  invoice_number
, name
, email
, company
, erp_trade_receivable_number
, card_type
, payment_type
, erp_account_number
, license_plate_number
, vehicle_name
, start_date_time
, end_date_time
, actual_start_date_time
, actual_end_date_time
, calculated_days
, calculated_hours
, calculated_mins
, calculated_day_rate
, calculated_hour_rate
, km_out
, km_in
, km_usage
, km_rate
, km_charges
, is_extended
, extended_number_of_days
, extended_number_of_hours
, extended_number_of_minutes
, is_owr
, owr_rate
, owr_location
, pickup_location
, return_location
, fuel_out
, fuel_in
, is_fuel_claim
, fuel_claim_amount
, calculated_late_days
, calculated_late_hours
, calculated_late_mins
, calculated_late_hour_rate
, is_promo
, promo_code
, CAST(CAST(promo_amount AS double) AS int) promo_amount
, promo_type
, promo_rate
, is_point
, CAST(CAST(number_of_points AS double) AS int) number_of_points
, is_coupon
, coupon_type
, CAST(CAST(coupon_amount AS double) AS int) coupon_amount
, coupon_rate
, invoice_amount_before_tax
, tax_amount
, total_price
, status
, rent_type
, payment_status
, points_earned_from_reservation
, reservation_created_at
FROM
  carshare_v2_rpt_archived_reservations
