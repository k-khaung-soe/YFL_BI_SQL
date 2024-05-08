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