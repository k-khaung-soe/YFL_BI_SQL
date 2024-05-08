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
