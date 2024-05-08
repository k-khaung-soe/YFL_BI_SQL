CREATE OR REPLACE VIEW "carshare_v2_rpt_fx_details" AS 
SELECT
  currency_code
, rate
, created_at
, updated_at
FROM
  carshare_v2_staging_exchange_rates