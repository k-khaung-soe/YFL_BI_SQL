CREATE OR REPLACE VIEW "carshare_v2_rpt_corporate_inquiries" AS 
SELECT
  type inquiry_type
, replace(CAST(json_extract(detail, '$.company_name') AS varchar), '"', '') company_name
, replace(CAST(json_extract(detail, '$.industry') AS varchar), '"', '') industry
, replace(CAST(json_extract(detail, '$.contact_person') AS varchar), '"', '') contact_person
, replace(CAST(json_extract(detail, '$.job_title') AS varchar), '"', '') job_title
, email
, phone phone
, status
, created_at
FROM
  carshare_v2_staging_enquiries
WHERE (user_type = 'CORPORATE')