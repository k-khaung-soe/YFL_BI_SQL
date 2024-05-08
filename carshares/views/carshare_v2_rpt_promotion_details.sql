CREATE OR REPLACE VIEW "carshare_v2_rpt_promotion_details" AS 
SELECT
  p.code
, p.category category_type
, p.type discount_type
, CAST(p.amount AS double) discount
, p.start_date
, p.end_date
, p.expiration_date
, CAST(p.created_at AS date) created_date
, count(json_extract(rd.discount_detail, '$.id')) number_of_usage
, sum(round(CAST(json_extract(rr.rate, '$.discount.total') AS int))) total_amount_discounted
, max(rd.updated_at) updated_at
FROM
  (((carshare_v2_staging_promotions p
LEFT JOIN (
   SELECT
     reservation_id
   , discount_detail
   , updated_at
   , row_number() OVER (PARTITION BY reservation_id ORDER BY created_at DESC) rn
   FROM
     carshare_v2_staging_reservation_discounts
   WHERE (discount_type = 'PROMO')
)  rd ON ((CAST(json_extract(rd.discount_detail, '$.id') AS int) = p.promotion_id) AND (rd.rn = 1)))
LEFT JOIN (
   SELECT
     reservation_id
   , rate
   , row_number() OVER (PARTITION BY reservation_id ORDER BY id DESC) rn
   FROM
     carshare_v2_staging_reservation_rates
)  rr ON ((rr.reservation_id = rd.reservation_id) AND (rr.rn = 1)))
LEFT JOIN carshare_v2_staging_reservations r ON ((r.reservation_id = rd.reservation_id) AND (r.status = 'CLOSED')))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
