CREATE OR REPLACE VIEW "yomaplus_rpt_user_info" AS 
SELECT DISTINCT
  u.user_id
, u.user_name
, u.email
, u.phone
, u.status user_status
, o.name organization_name
, u.customer_type
, (CASE WHEN (u.user_id IN (SELECT user_id
FROM
  yomaplus_staging_orders
)) THEN 'ordered_user' WHEN (NOT (u.user_id IN (SELECT user_id
FROM
  yomaplus_staging_orders
))) THEN 'non_ordered_user' END) user_type
, u.last_login_date
, u.total_login
, u.verified_at
, u.created_at
, u.updated_at
, u.deleted_at
, "date_format"(u.created_at, '%Y-%M') month
, CAST(u.created_at AS date) date
FROM
  (((yomaplus_staging_users u
LEFT JOIN yomaplus_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN yomaplus_staging_organizations o ON (o.organization_id = p.organization_id))
LEFT JOIN yomaplus_staging_orders ord ON (ord.user_id = u.user_id))
ORDER BY u.created_at DESC
