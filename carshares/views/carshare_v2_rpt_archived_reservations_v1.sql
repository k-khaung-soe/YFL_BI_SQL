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
