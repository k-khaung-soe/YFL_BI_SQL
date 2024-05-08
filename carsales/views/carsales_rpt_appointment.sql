CREATE OR REPLACE VIEW "carsales_rpt_appointment" AS 
SELECT
  csa.lot_number
, csv.vehicle_name
, csu.name user_name
, csu.email contact_info
, csu.phone
, cse.appointment_date
, cse.message comment
FROM
  (((carsales_staging_enquiries cse
LEFT JOIN carsales_staging_auctions csa ON (csa.auction_id = cse.auction_id))
LEFT JOIN carsales_staging_users csu ON (csu.user_id = cse.user_id))
LEFT JOIN carsales_staging_vehicles csv ON (csv.vehicle_id = csa.vehicle_id))
WHERE (cse.type = 'appointment')