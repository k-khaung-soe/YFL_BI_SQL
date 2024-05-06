/* carsales_rpt_appointment */
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

/* carsales_rpt_auctions */
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

/* carsales_rpt_bids */
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

/* carsales_rpt_customer_comment */
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

/* carsales_rpt_customer_management */
CREATE OR REPLACE VIEW "carsales_rpt_customer_management" AS 
SELECT
  csu.user_id customer_id
, csu.name
, csu.email
, csu.phone
, (CASE WHEN (csu.verified_at IS NOT NULL) THEN 'yes' ELSE 'no' END) verified
, csu.status
, csu.updated_at
, csp.address
, csp.date_of_birth
, csu.created_at
, csu.last_login_date
FROM
  (carsales_staging_users csu
LEFT JOIN carsales_staging_profiles csp ON (csp.user_id = csu.user_id))
WHERE (csu.customer_type = 'customer')

/* carsales_rpt_deposit_payment */
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

/* carsales_rpt_vehicles */
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