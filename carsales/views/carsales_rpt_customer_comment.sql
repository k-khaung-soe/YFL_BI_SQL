CREATE OR REPLACE VIEW "carsales_rpt_customer_comment" AS 
SELECT
  csa.lot_number
, csv.vehicle_name
, csu.name user_name
, csu.phone
, cse.message comment
FROM
  (((carsales_staging_enquiries cse
LEFT JOIN carsales_staging_auctions csa ON (csa.auction_id = cse.auction_id))
LEFT JOIN carsales_staging_users csu ON (csu.user_id = cse.user_id))
LEFT JOIN carsales_staging_vehicles csv ON (csv.vehicle_id = csa.vehicle_id))
WHERE (cse.type = 'comment')