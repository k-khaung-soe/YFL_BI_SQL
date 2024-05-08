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