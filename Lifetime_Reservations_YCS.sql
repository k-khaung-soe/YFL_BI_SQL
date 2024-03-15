select  a.*,b.user_id from 
(
select  distinct * FROM
(SELECT 
r2.id Reservation_id,
r1.confirmation_number,
r1.invoice_number,
r1.name,
r1.email,
r1.company,
null as erp_trade_receivable_number,
r1.card_type,
r1.payment_type,
null as erp_account_number,
r1.tap_card_number,
r1.vehicle_name_license,
r1.vehicle_name,
r1.start_date_time,
r1.end_date_time,
r1.actual_start_date_time,
r1.actual_end_date_time,
null as extend_start_date_time,
null as extend_end_date_time,
r1.calculated_day,
r1.calculated_hour,
0 as calculated_mins,
r1.calculated_day_rate,
r1.calculated_hour_rate,
r1.km_out,
r1.km_in,
r1.km_usage,
r1.km_rate,
0 as km_charges,
r1.is_extended,
r1.extended_fee,
0 as extended_number_of_days,
0 as extended_number_of_hours,
0 as extended_number_of_minutes,
r1.is_owr, 
r1.owr_rate,
r1.owr_location,
r1.is_transit,
r1.transit_location,
r1.pickup_location,
r1.return_location,
r1.is_damage,
cast(r1.fuel_out as varchar),
cast(r1.fuel_in as varchar),
r1.is_fuel_claim,
r1.fuel_claim as fuel_claim_amount,
null as calculated_late_days,
r1.calculated_late_hour,
0 as calculated_late_mins,
r1.calculated_late_hour_rate,
r1.is_recharge,
r1.recharge_fee,
r1.is_promo,
r1.promo_code,
cast(r1.promo_amount as double),
r1.promo_type,
cast(r1.promo_rate as varchar),
null as is_point,
0 as number_of_points,
null as is_coupon,
null as coupon_type,
0 as coupon_amount,
0 as coupon_rate,
null as invoice_amount_before_tax,
null as tax_amount,
null as reservation_additional_charges,
null as additional_charges_description,
r1.total_price,
r1.status,
r1.rent_type,
r1.payment_status,
case
when r1.reservation_created_date_time is null
then r1.start_date_time
else r1.reservation_created_date_time
end
as reservation_created_at,
r1.reference_or_card_type,
null as points_earned_from_reservations,
null as rental_like_yes_or_no


FROM (select * from "yf_edwh"."carshare_v2_rpt_archived_reservations_v1"

) r1 
left join (select * from carshare_v2_staging_archived_reservations_v1
--union select * from carshare_v2_staging_archived_reservations_v2
) r2
on r1.confirmation_number =r2.confirmation_number

union 

select 
reservation_id,
null as confirmation_number,
invoice_number,
name,
email,
company,
cast(erp_trade_receivable_number as varchar),
card_type,
payment_type,
erp_account_number,
null as tap_card_number,
license_plate_number as vehicle_name_license,
vehicle_name,
start_date_time,
end_date_time,
actual_start_date_time,
actual_end_date_time,
extend_start_date_time,
extend_end_date_time,
calculated_days,
calculated_hours,
calculated_mins,
calculated_day_rate,
calculated_hour_rate,
km_out,
km_in,
km_usage,
km_rate,
km_charges,
is_extended,
0 as extended_fee,
extended_number_of_days,
extended_number_of_hours,
extended_number_of_minutes,
is_owr,owr_rate, owr_location,
null as is_transit,
null as transit_location,
pickup_location,
return_location,
null as is_damage,
fuel_out, fuel_in,
is_fuel_claim,
fuel_claim_amount,
calculated_late_days,
calculated_late_hours,
calculated_late_mins,
calculated_late_hour_rate,
null as is_recharge,
null as recharge_fee,
is_promo,
promo_code,
cast(promo_amount as double),
promo_type,
cast(promo_rate as varchar),
is_point,
number_of_points,
is_coupon,
coupon_type,
cast(coupon_amount as int),
cast(coupon_rate as int),
invoice_amount_before_tax,
tax_amount,
0 as reservation_additional_charges,
NULL as additional_charges_description,
total_price,
status,
rent_type,
payment_status,
case when reservation_created_at is null
then start_date_time
else reservation_created_at
end as reservation_created_at,

reference,
points_earned_from_reservation,
rental_like_yes_or_no
from carshare_v2_rpt_archived_reservations


union

select 
reservation_id,
null as confirmation_number,
invoice_number,
name,
email,
company,
cast(erp_trade_receivable_number as varchar),
card_type,
payment_type,
erp_account_number,
null as tap_card_number,
license_plate_number as vehicle_name_license,
vehicle_name,
start_date_time,
end_date_time,
actual_start_date_time,
actual_end_date_time,
extend_start_date_time,
extend_end_date_time,
calculated_days,
calculated_hours,
calculated_mins,
calculated_day_rate,
calculated_hour_rate,
km_out,
km_in,
km_usage,
km_rate,
km_charges,
is_extended,
0 as extended_fee,
extended_number_of_days,
extended_number_of_hours,
extended_number_of_minutes,
is_owr,owr_rate, owr_location,
null as is_transit,
null as transit_location,
pickup_location,
return_location,
null as is_damage,
fuel_out, fuel_in,
is_fuel_claim,
fuel_claim_amount,
calculated_late_days,
calculated_late_hours,
calculated_late_mins,
calculated_late_hour_rate,
null as is_recharge,
null as recharge_fee,
is_promo,
promo_code,
promo_amount,
promo_type,
cast(promo_rate as varchar),
is_point,
number_of_points,
is_coupon,
coupon_type,
coupon_amount,
coupon_rate,
invoice_amount_before_tax,
tax_amount,
reservation_additional_charges,
additional_charges_description,
total_price,
status,
rent_type,
payment_status,
case when reservation_created_at is null 
then start_date_time
else reservation_created_at
end as reservation_created_at,

reference,
points_earned_from_reservation,
rental_like_yes_or_no
from (select * from carshare_v2_rpt_reservations_details_summary
)
)
order by invoice_number desc
)
a 
left join
(select distinct reservation_id,invoice_number, user_id
from
(
select id reservation_id, invoice_number,user_id from carshare_staging_archived_reservations

union

select id reservation_id, invoice_number, user_id from carshare_v2_staging_archived_reservations_v1

union

select id reservation_id, invoice_number, user_id from carshare_v2_staging_archived_reservations_v2

union

select reservation_Id, invoice_number, user_id from carshare_v2_staging_reservations
)
) b
on a.invoice_number = b.invoice_number
--where date_format(cast(start_date_time as date), '%Y_%m')< date_format(current_timestamp, '%Y_%m')

order by reservation_created_at asc






