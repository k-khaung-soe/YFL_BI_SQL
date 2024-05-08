CREATE OR REPLACE VIEW "yomaplus_rpt_employee_info" AS 
SELECT
  a.*
, (CASE WHEN (a.user_id IS NULL) THEN 'non_registered_user' WHEN (a.user_id IS NOT NULL) THEN 'registered_user' END) registered_or_not
FROM
  (
   SELECT DISTINCT
     e.employee_id
   , (CASE WHEN (e.identification_number = p.identification_number) THEN u.user_id ELSE null END) user_id
   , u.status user_status
   , e.employee_card_id
   , e.entity
   , org.name organization_name
   , e.name employee_name
   , e.date_of_birth
   , e.id_type
   , e.identification_number
   , e.phone
   , e.email
   , e.entity_hr_email
   , e.position
   , e.employee_status
   , e.created_at employee_created_at
   , u.created_at user_created_at
   , e.updated_at
   , u.deleted_at user_deleted_at
   , e.deleted_at employee_deleted_at
   , (CASE WHEN (u.user_id IN (SELECT user_id
FROM
  yomaplus_staging_orders
)) THEN 'ordered_user' WHEN (NOT (u.user_id IN (SELECT user_id
FROM
  yomaplus_staging_orders
))) THEN 'non_ordered_user' END) plus_user_type
   FROM
     ((((yomaplus_staging_employees e
   LEFT JOIN yomaplus_staging_organizations org ON (org.organization_id = e.organization_id))
   LEFT JOIN yomaplus_staging_profiles p ON ("upper"(p.identification_number) = "upper"(e.identification_number)))
   LEFT JOIN yomaplus_staging_users u ON (u.user_id = p.user_id))
   LEFT JOIN yomaplus_staging_orders o ON (o.user_id = u.user_id))
)  a