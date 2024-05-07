CREATE OR REPLACE VIEW "carsales_rpt_auctions" AS 
SELECT
  au.lot_number
, v.vehicle_name
, v.license_plate_number
, au.status
, au.extend_auction
, au.reserve_amount
, b.status bid_status
, b.amount bid_amount
, (CASE WHEN (b.status = 'win') THEN b.amount END) bid_amount_that_won_auction
, au.start_date auction_start_datetime
, au.end_date auction_end_datetime
, u.name bidder_name
, u.email bidder_email
, (CASE WHEN (b.status = 'win') THEN u.name END) winner_user_name
FROM
  (((carsales_staging_auctions au
LEFT JOIN carsales_staging_vehicles v ON (v.vehicle_id = au.vehicle_id))
LEFT JOIN carsales_staging_bids b ON (au.auction_id = b.auction_id))
LEFT JOIN carsales_staging_users u ON (u.user_id = b.user_id))