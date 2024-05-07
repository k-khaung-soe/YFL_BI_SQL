CREATE OR REPLACE VIEW "carsales_rpt_vehicles" AS 
SELECT
  vehicle_name
, license_plate_number
, status
, b.name location
, a.created_at
, a.updated_at
, a.deleted_at
FROM
  (carsales_staging_vehicles a
LEFT JOIN carsales_staging_locations b ON (b.location_id = a.location_id))