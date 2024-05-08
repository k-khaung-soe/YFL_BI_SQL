CREATE OR REPLACE VIEW "yomaplus_rpt_employees_management" AS 
SELECT
  e.created_at
, e.employee_card_id employee_id
, u.user_id
, e.name employee_name
, o.name organization_name
, e.entity
, e.email
, e.identification_number
, e.phone
, e.date_of_birth
, DATE_DIFF('year', CAST(e.date_of_birth AS date), current_timestamp) age
, e.entity_hr_email
, e.position
, e.length_of_employment
, u.created_at registered_date
, od_summary.no_of_active_collected_subscription
, od_summary.no_of_shipping_ready_order
, od_summary.no_of_approved_order
, od_summary.no_of_pending_order
, od_summary.no_of_rejected_order
, (CASE WHEN ((od_summary.total_draft > od_summary.no_of_deleted_draft_order) AND (CAST(u.last_login_date AS date) < (current_date - INTERVAL  '30' DAY))) THEN 'Yes' ELSE 'No' END) has_draft_order_but_has_not_login_past_30_days
, (CASE WHEN ((NOT (u.user_id IN (SELECT user_id
FROM
  yomaplus_staging_orders
WHERE (status = 'active')
))) AND (CAST(u.last_login_date AS DATE) > (current_date - INTERVAL  '30' DAY))) THEN 'Yes' ELSE 'No' END) no_order_but_login_past_30_days
, (CASE WHEN (u.total_login > 1) THEN 'No' ELSE 'Yes' END) registered_but_never_login_again
, u.total_login
, u.last_login_date
, od_summary.total_draft
, od_summary.no_of_deleted_draft_order
, u.deleted_at user_deleted_at
, e.deleted_at employee_deleted_at
, (CASE WHEN (e.payroll_access = '1') THEN 'Yes' ELSE 'No' END) yoma_bank_yes_no
FROM
  ((((yomaplus_staging_employees e
LEFT JOIN yomaplus_staging_organizations o ON (o.organization_id = e.organization_id))
LEFT JOIN yomaplus_staging_profiles p ON (p.identification_number = e.identification_number))
LEFT JOIN yomaplus_staging_users u ON (u.user_id = p.user_id))
LEFT JOIN (
   SELECT
     od.user_id
   , count((CASE WHEN ((od.status = 'active') AND (od.deleted_at IS NULL)) THEN od.order_id END)) no_of_active_collected_subscription
   , count((CASE WHEN ((od.status = 'ready') AND (od.deleted_at IS NULL)) THEN od.order_id END)) no_of_shipping_ready_order
   , count((CASE WHEN ((od.status = 'approved') AND (od.deleted_at IS NULL)) THEN od.order_id END)) no_of_approved_order
   , count((CASE WHEN ((od.status = 'pending') AND (od.deleted_at IS NULL)) THEN od.order_id END)) no_of_pending_order
   , count((CASE WHEN ((od.status = 'rejected') AND (od.deleted_at IS NULL)) THEN od.order_id END)) no_of_rejected_order
   , count((CASE WHEN ((od.status = 'draft') AND (CAST(od.updated_at AS date) > (current_date - INTERVAL  '30' DAY)) AND (od.deleted_at IS NULL)) THEN od.order_id END)) no_of_draft_order_in_past_30_days
   , count((CASE WHEN ((od.status = 'draft') AND (od.deleted_at IS NULL)) THEN od.order_id END)) total_draft
   , count((CASE WHEN ((od.status = 'draft') AND (od.deleted_at IS NOT NULL)) THEN od.order_id END)) no_of_deleted_draft_order
   FROM
     yomaplus_staging_orders od
   GROUP BY 1
)  od_summary ON (od_summary.user_id = u.user_id))