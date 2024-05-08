CREATE OR REPLACE VIEW "carshare_v2_rpt_call_center_users_management" AS 
SELECT
  u.user_id user_id
, u.username name
, u.email email
, p.date_of_birth date_of_birth
, p.contact_number contact_number
, u.account_type account_type
, p.erp_account_number erp_account_number
, cp.erp_trade_receivable_number erp_trade_receivable_number
, dl.expiration_date license_expiry_date
, (CASE WHEN (u.verified_at IS NOT NULL) THEN 'Yes' ELSE 'No' END) confirmed
, (CASE WHEN (u.status = 'APPROVED') THEN 'Yes' ELSE 'No' END) approved
, (CASE WHEN (u.status = 'APPROVED') THEN 'Active' WHEN (u.status IN ('PENDING', 'TERMINATED')) THEN 'Inactive' END) user_status
, replace(replace(p.address, chr(10), ' '), chr(13), ' ') address
, cp.name company_name
, u.created_at joining_date
, DATE_DIFF('year', CAST(p.date_of_birth AS date), current_date) calculated_age
, dl.issue_country license_issuing_country
, array_join(array_agg(DISTINCT bi.brand), '/') payment
, u.updated_at overview_last_updated_at
, p.updated_at corporate_info_last_updated_at
, p.updated_at profile_info_last_updated_at
, dl.updated_at license_info_last_updated_at
, ic.updated_at identification_info_last_updated_at
, max(bi.updated_at) payment_info_last_updated_at
, u.deleted_at user_deleted_at
FROM
  ((((((carshare_v2_staging_users u
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_licenses dl ON (dl.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_corporate_accounts co ON (co.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_companies cp ON (cp.company_id = co.company_id))
LEFT JOIN carshare_v2_staging_payments bi ON (bi.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_identifications ic ON (ic.user_id = u.user_id))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23, 25