CREATE OR REPLACE VIEW "carshare_v2_rpt_vehicles_details" AS 
SELECT
  v.name car_name
, ac.code acriss_code
, l.name location_name
, v.license_plate_number
, v.status
, v.car_share_status
, CAST(v.odometer AS int) odometer
, replace(CAST(json_extract(vehicle_attributes, '$.value[0]') AS varchar), '"', '') year
, replace(CAST(json_extract(vehicle_attributes, '$.value[1]') AS varchar), '"', '') fuel_type
, replace(CAST(json_extract(vehicle_attributes, '$.value[2]') AS varchar), '"', '') vehicle_category
, replace(CAST(json_extract(vehicle_attributes, '$.value[3]') AS varchar), '"', '') transmission
, replace(CAST(json_extract(vehicle_attributes, '$.value[4]') AS varchar), '"', '') color
, replace(CAST(json_extract(vehicle_attributes, '$.value[5]') AS varchar), '"', '') engine
, replace(CAST(json_extract(vehicle_attributes, '$.value[6]') AS varchar), '"', '') build_type
, replace(CAST(json_extract(vehicle_attributes, '$.value[7]') AS varchar), '"', '') steering
, replace(CAST(json_extract(vehicle_attributes, '$.value[8]') AS varchar), '"', '') registered_state
, replace(CAST(json_extract(vehicle_attributes, '$.value[9]') AS varchar), '"', '') interior_color
, replace(CAST(json_extract(vehicle_attributes, '$.value[10]') AS varchar), '"', '') grade
, v.created_at created_at
, v.deleted_at deleted_at
, replace(CAST(json_extract(vehicle_attributes, '$.value[11]') AS varchar), '"', '') registration_date
, replace(CAST(json_extract(vehicle_attributes, '$.value[12]') AS varchar), '"', '') registration_renewal_due_date
, replace(CAST(json_extract(vehicle_attributes, '$.value[13]') AS varchar), '"', '') date_due_off_fleet
FROM
  ((carshare_v2_staging_vehicles v
LEFT JOIN carshare_v2_staging_acrisses ac ON (ac.acriss_id = v.acriss_id))
LEFT JOIN carshare_v2_staging_locations l ON (l.location_id = v.location_id))