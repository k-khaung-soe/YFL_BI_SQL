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
