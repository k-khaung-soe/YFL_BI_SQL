CREATE OR REPLACE VIEW "carshare_v2_rpt_points_and_coupons_details" AS 
SELECT
  user_info.user_id
, user_info.username
, user_info.erp_account_number
, user_info.payment_type
, (CASE WHEN (uph.point_type = 'EARNED') THEN uph.created_at END) points_earned_date
, (CASE WHEN (uph.point_type = 'EARNED') THEN uph.total END) points_earned
, (CASE WHEN (uph.point_type = 'EARNED') THEN uph.expiry_at END) points_expiry_date
, (CASE WHEN (uph.point_type = 'EARNED') THEN po.point_type END) points_earned_type
, (CASE WHEN (uph.point_type = 'USED') THEN uph.total END) points_used
, (CASE WHEN (uph.point_type = 'USED') THEN uph.created_at END) points_used_date
, null coupon_awarded_date
, null coupon_code
, null coupon_type
, null coupon_expiry_date
, null coupon_status
, null coupon_used
, null coupon_used_date
FROM
  (((
   SELECT
     u.user_id user_id
   , u.username
   , p.erp_account_number
   , array_join(array_agg(DISTINCT p2.brand), '/') payment_type
   FROM
     ((carshare_v2_staging_users u
   LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
   LEFT JOIN carshare_v2_staging_payments p2 ON (p2.user_id = u.user_id))
   GROUP BY 1, 2, 3
)  user_info
LEFT JOIN carshare_v2_staging_user_point_histories uph ON (uph.user_id = user_info.user_id))
LEFT JOIN carshare_v2_staging_points po ON (po.point_id = uph.point_id))
UNION SELECT
  user_info.user_id
, user_info.username
, user_info.erp_account_number
, user_info.payment_type
, null points_earned_date
, null points_earned
, null points_expiry_date
, null points_earned_type
, null points_used
, null points_used_date
, cu.created_at coupon_awarded_date
, c.code coupon_code
, c.type coupon_type
, c.expiration_date coupon_expiry_date
, cu.status coupon_status
, (CASE WHEN (cu.status = 'INACTIVE') THEN c.code END) coupon_used
, (CASE WHEN (cu.updated_at IS NOT NULL) THEN cu.updated_at END) coupon_used_date
FROM
  (((
   SELECT
     u.user_id user_id
   , u.username
   , p.erp_account_number
   , array_join(array_agg(DISTINCT p2.brand), '/') payment_type
   FROM
     ((carshare_v2_staging_users u
   LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
   LEFT JOIN carshare_v2_staging_payments p2 ON (p2.user_id = u.user_id))
   GROUP BY 1, 2, 3
)  user_info
LEFT JOIN carshare_v2_staging_coupon_user cu ON (cu.user_id = user_info.user_id))
LEFT JOIN carshare_v2_staging_coupons c ON (c.coupon_id = cu.coupon_id))
ORDER BY 1 ASC