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