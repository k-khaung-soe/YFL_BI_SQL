CREATE OR REPLACE VIEW "carshare_v2_rpt_reviewed_details" AS 
SELECT
  r.reservation_id
, r.invoice_number invoice_number
, u.username name
, u.email
, p.erp_account_number
, title topic
, CAST(rating AS int) rating
, replace(replace(comment, chr(10), ' '), chr(13), ' ') description
, CAST(uph.total AS int) points
, CAST(rf.created_at AS date) created_at
FROM
  ((((carshare_v2_staging_reservation_feedbacks rf
LEFT JOIN carshare_v2_staging_reservations r ON (r.reservation_id = rf.reservation_id))
LEFT JOIN carshare_v2_staging_users u ON (r.user_id = u.user_id))
LEFT JOIN carshare_v2_staging_profiles p ON (p.user_id = r.user_id))
LEFT JOIN carshare_v2_staging_user_point_histories uph ON (uph.reference = r.invoice_number))
WHERE (uph.point_id = 1)
UNION SELECT
  rf.id reservation_id
, rf.invoice_number invoice_number
, u.username name
, u.email
, CAST(json_extract(u.profile, '$.erp_account_number') AS varchar) erp_account_number
, CAST(json_extract(reservation_feedbacks, '$.title') AS varchar) topic
, CAST(json_extract(reservation_feedbacks, '$.rating') AS int) rating
, CAST(json_extract(reservation_feedbacks, '$.comment') AS varchar) description
, CAST(uph.total AS int) points
, CAST('2999-12-31' AS date) created_at
FROM
  ((carshare_v2_staging_archived_reservations_v2 rf
LEFT JOIN carshare_v2_staging_archived_users u ON (CAST(json_extract(rf.reservation_feedbacks, '$.reservation_id') AS bigint) = u.reservation_id))
LEFT JOIN carshare_v2_staging_user_point_histories uph ON (uph.reference = rf.invoice_number))
WHERE (uph.point_id = 1)