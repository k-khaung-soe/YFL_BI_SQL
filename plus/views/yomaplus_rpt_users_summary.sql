CREATE OR REPLACE VIEW "yomaplus_rpt_users_summary" AS 
SELECT
  e.employee_id
, e.employee_card_id
, e.entity
, e.name
, e.date_of_birth
, e.id_type
, e.identification_number
, e.phone
, e.email
, e.position
, e.length_of_employment
, e.employee_status
, u.user_id
, u.status user_status
, u.customer_type
, u.last_login_date
, u.total_login
, u.verified_at
, org.name organization_name
, (CASE WHEN (u.user_id IN (SELECT user_id
FROM
  yomaplus_staging_orders
WHERE (status IN ('approved', 'pending', 'ordered', 'shipping', 'ready', 'collected', 'active'))
)) THEN 'ordered_user' WHEN (NOT (u.user_id IN (SELECT user_id
FROM
  yomaplus_staging_orders
WHERE (status IN ('approved', 'pending', 'ordered', 'shipping', 'ready', 'collected', 'active'))
))) THEN 'non_ordered_user' END) user_type
, e.created_at employee_created_at
, e.updated_at employee_updated_at
, e.deleted_at employee_deleted_at
, u.created_at user_created_at
, u.updated_at user_updated_at
, u.deleted_at user_deleted_at
, (CASE WHEN (u.user_id IS NOT NULL) THEN 1 ELSE 0 END) user_count
, (CASE WHEN (e.employee_id IS NOT NULL) THEN 1 ELSE 0 END) employee_count
FROM
  (((yomaplus_staging_employees e
LEFT JOIN yomaplus_staging_profiles p ON ("upper"(p.identification_number) = "upper"(e.identification_number)))
LEFT JOIN yomaplus_staging_users u ON (u.user_id = p.user_id))
LEFT JOIN yomaplus_staging_organizations org ON (org.organization_id = e.organization_id))
