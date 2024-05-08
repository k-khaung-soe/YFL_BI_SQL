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