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
