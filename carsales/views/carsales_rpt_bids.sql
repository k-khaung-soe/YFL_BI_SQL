CREATE OR REPLACE VIEW "carsales_rpt_bids" AS 
SELECT
  csa.lot_number
, csv.vehicle_name
, csv.license_plate_number
, csu.name customer_name
, "concat"("concat"(csu.email, ','), csu.phone) contact_info
, csb.amount
, csb.status
, csb.created_at
FROM
  (((carsales_staging_bids csb
LEFT JOIN carsales_staging_auctions csa ON (csa.auction_id = csb.auction_id))
LEFT JOIN carsales_staging_vehicles csv ON (csv.vehicle_id = csa.vehicle_id))
LEFT JOIN carsales_staging_users csu ON (csu.user_id = csb.user_id))
ORDER BY csa.lot_number ASC