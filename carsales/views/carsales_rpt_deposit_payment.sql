CREATE OR REPLACE VIEW "carsales_rpt_deposit_payment" AS 
SELECT
  csa.lot_number
, csu.name customer_name
, csv.vehicle_name
, casp.status
, csu.phone
, casp.amount
, casp.transaction_number
, casp.created_at
FROM
  (((carsales_staging_auction_payments casp
LEFT JOIN carsales_staging_auctions csa ON (csa.auction_id = casp.auction_id))
LEFT JOIN carsales_staging_users csu ON (csu.user_id = casp.user_id))
LEFT JOIN carsales_staging_vehicles csv ON (csv.vehicle_id = csa.vehicle_id))
WHERE (casp.status = 'deposit')