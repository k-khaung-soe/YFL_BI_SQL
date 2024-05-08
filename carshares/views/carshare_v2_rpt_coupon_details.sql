CREATE OR REPLACE VIEW "carshare_v2_rpt_coupon_details" AS 
SELECT
  c.code
, c.type coupon_category
, CAST(c.value AS double) discount
, c.start_date
, c.end_date
, c.discount_type
, c.expiration_date
, count(cu.coupon_id) number_of_coupon_distributed
, count((CASE WHEN (cu.status = 'INACTIVE') THEN cu.coupon_id END)) number_of_coupon_used
, sum((CASE WHEN (cu.status = 'INACTIVE') THEN CAST(c.value AS int) END)) total_amount_discounted
, max(cu.updated_at) updated_at
FROM
  (carshare_v2_staging_coupons c
LEFT JOIN carshare_v2_staging_coupon_user cu ON (cu.coupon_id = c.coupon_id))
WHERE (c.deleted_at IS NULL)
GROUP BY 1, 2, 3, 4, 5, 6, 7