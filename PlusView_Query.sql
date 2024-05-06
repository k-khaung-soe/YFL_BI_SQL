/* yomaplus_rpt_employee_info */
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

/* yomaplus_rpt_employees_management */
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

/* yomaplus_rpt_orders_management */
CREATE OR REPLACE VIEW "yomaplus_rpt_orders_management" AS 
SELECT
  o.order_id order_id
, o.order_number
, u.user_name employee_name
, p.identification_number nric_passport_number
, p.entity entity_name
, u.phone phone_number
, u.email
, e.entity_hr_email
, o.total_amount price_mmk
, od.deposit deposit_percent
, ((od.retail_price * od.deposit) / 100) deposit_amount
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.status') AS varchar), '"', '') ELSE null END) payment_status
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.initiatorMsisdn') AS varchar), '"', '') ELSE null END) msisdn
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.merchantReferenceId') AS varchar), '"', '') ELSE null END) reference_id
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.transactionId') AS varchar), '"', '') ELSE null END) transaction_id
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.requestTime') AS varchar), '"', '') ELSE null END) request_time
, od.min_salary min_salary_mmk
, o.total_min_salary total_min_salary_mmk
, od.monthly_price monthly_deduction_mmk
, cat.name product_type
, pro.name product
, od.product_sku sku_name
, array_join(array_agg(DISTINCT COALESCE(colors, '')), '') color
, array_join(array_agg(DISTINCT COALESCE(storage_capacity, '')), '') storage_capacity
, array_join(array_agg(DISTINCT COALESCE(memory, '')), '') memory
, array_join(array_agg(DISTINCT COALESCE(size, '')), '') size
, array_join(array_agg(DISTINCT COALESCE(sim_type, '')), '') dual_or_single_sim
, array_join(array_agg(DISTINCT COALESCE(connectivity, '')), '') connectvity_4g_wifi_etc
, ' ' liter
, ' ' horsepower
, ' ' processor
, b.name pickup_location
, s.name supplier
, o.status
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) OR (o.contract_type IS NULL))) THEN 'Yes' ELSE 'No' END) draft_without_id_selife_or_contract_uploaded
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NOT NULL) AND (o.contract_type IS NULL))) THEN 'Yes' ELSE 'No' END) draft_with_id_selfie_without_contract
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) AND (o.contract_type IS NOT NULL))) THEN 'Yes' ELSE 'No' END) draft_with_contract_uploaded_without_id
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) AND (o.contract_type IS NOT NULL)) AND (od.deposit IS NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_and_draft_with_no_deposit
, (CASE WHEN ((o.status = 'draft') AND (p.identity_front_image IS NULL) AND (od.deposit IS NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_and_draft_with_no_deposit_and_id_selfie
, (CASE WHEN ((o.status = 'draft') AND (o.contract_type IS NOT NULL) AND (od.deposit IS NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_and_draft_with_no_deposit_and_contract_uploaded
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) AND (o.contract_type IS NULL)) AND (od.deposit IS NOT NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_but_done_nothing
, o.created_at applied_date
, pending.pending_date
, approved.approved_date
, rejected.rejected_date
, ordered.ordered_date
, shipping.shipping_date
, ready.ready_date
, collected.collected_date
, cancelled.cancelled_date
, active.active_date
, (CASE WHEN (o.status IN ('closed', 'archived')) THEN o.updated_at ELSE null END) closed_date
, ao.created_at archived_date
, o.deleted_at deleted_date
, org.name organization_name
, o.start_date
, o.end_date
, od.service_fees admin_fees
, od.repayment_period loan_term
, rb.interest_rate
, cat1.name category_name
FROM
  (((((((((((((((((((((((yomaplus_staging_orders o
LEFT JOIN yomaplus_staging_order_details od ON (od.order_id = o.order_id))
LEFT JOIN yomaplus_staging_users u ON (u.user_id = o.user_id))
LEFT JOIN yomaplus_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN yomaplus_staging_employees e ON ((e.identification_number = p.identification_number) AND (e.deleted_at IS NULL) AND (e.organization_id = p.organization_id)))
LEFT JOIN yomaplus_staging_products pro ON (pro.product_id = od.product_id))
LEFT JOIN yomaplus_staging_categories cat ON (cat.category_id = pro.category_id))
LEFT JOIN yomaplus_staging_categories cat1 ON (cat1.category_id = cat.category_parent_id))
LEFT JOIN yomaplus_staging_organizations org ON (org.organization_id = p.organization_id))
LEFT JOIN (
   SELECT
     psku2.sku_name
   , (CASE WHEN (psku2.attribute_key = 'colors') THEN psku2.attribute_value ELSE null END) colors
   , (CASE WHEN (psku2.attribute_key = 'storage_capacity') THEN psku2.attribute_value ELSE null END) storage_capacity
   , (CASE WHEN (psku2.attribute_key = 'memory') THEN psku2.attribute_value ELSE null END) memory
   , (CASE WHEN (psku2.attribute_key = 'size') THEN psku2.attribute_value ELSE null END) size
   , (CASE WHEN (psku2.attribute_key = 'sim_type') THEN psku2.attribute_value ELSE null END) sim_type
   , (CASE WHEN (psku2.attribute_key = 'connectivity') THEN psku2.attribute_value ELSE null END) connectivity
   FROM
     (
      SELECT
        psku.product_id product_id
      , psku.sku_id sku_id
      , psku.sku_name
      , pav.variation_name attribute_value
      , pa.variation_name attribute_key
      , psku.price
      , psku.min_salary
      , psku.stock
      FROM
        (((yomaplus_staging_product_skus psku
      LEFT JOIN yomaplus_staging_sku_attribute_values sav ON (sav.product_sku_id = psku.sku_id))
      LEFT JOIN yomaplus_staging_product_attribute_values pav ON (pav.product_attribute_value_id = sav.product_attribute_value_id))
      LEFT JOIN yomaplus_staging_product_attributes pa ON (pa.product_attribute_id = pav.product_attribute_id))
   )  psku2
)  psku1 ON (psku1.sku_name = od.product_sku))
LEFT JOIN yomaplus_staging_branches b ON (b.branch_id = od.pickup_id))
LEFT JOIN yomaplus_staging_suppliers s ON (s.supplier_id = o.supplier_id))
LEFT JOIN (
   SELECT
     order_id
   , updated_at pending_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'pending')
)  pending ON ((pending.order_id = o.order_id) AND (pending.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at approved_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'approved')
)  approved ON ((approved.order_id = o.order_id) AND (approved.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at rejected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'rejected')
)  rejected ON ((rejected.order_id = o.order_id) AND (rejected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ordered_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ordered')
)  ordered ON ((ordered.order_id = o.order_id) AND (ordered.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at shipping_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'shipping')
)  shipping ON ((shipping.order_id = o.order_id) AND (shipping.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ready_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ready')
)  ready ON ((ready.order_id = o.order_id) AND (ready.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at collected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'collected')
)  collected ON ((collected.order_id = o.order_id) AND (collected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at cancelled_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'cancelled')
)  cancelled ON ((cancelled.order_id = o.order_id) AND (cancelled.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at active_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'active')
)  active ON ((active.order_id = o.order_id) AND (active.rn = 1)))
LEFT JOIN yomaplus_staging_archived_orders ao ON (ao.order_id = o.order_id))
LEFT JOIN yomaplus_staging_category_ratebooks cr ON ((cr.category_id = pro.category_id) AND (cr.organization_id = p.organization_id)))
LEFT JOIN yomaplus_staging_rate_books rb ON (rb.rate_book_id = cr.rate_book_id))
WHERE (o.status <> 'archived')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61
UNION SELECT
  ao.order_id
, ao.order_number
, replace(CAST(json_extract(ao.user, '$.name') AS varchar), '"', '') employee_name
, replace(CAST(json_extract(ao.user, '$.profile.identification_number') AS varchar), '"', '') nric_passport_number
, replace(CAST(json_extract(ao.user, '$.profile.entity') AS varchar), '"', '') entity_name
, replace(CAST(json_extract(ao.user, '$.phone') AS varchar), '"', '') phone_number
, replace(CAST(json_extract(ao.user, '$.email') AS varchar), '"', '') email
, replace(CAST(json_extract(ao.user, '$.profile.entity_email') AS varchar), '"', '') entity_hr_email
, ao.total_amount price_mmk
, CAST(replace(CAST(json_extract(ao.order_detail, '$.deposit') AS varchar), '"', '') AS double) deposit_percent
, ((CAST(replace(CAST(json_extract(ao.order_detail, '$.retail_price') AS varchar), '"', '') AS int) * CAST(replace(CAST(json_extract(ao.order_detail, '$.deposit') AS varchar), '"', '') AS int)) / 100) deposit_amount
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.status') AS varchar), '"', '') ELSE null END) payment_status
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.initiatorMsisdn') AS varchar), '"', '') ELSE null END) msisdn
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.merchantReferenceId') AS varchar), '"', '') ELSE null END) reference_id
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.transactionId') AS varchar), '"', '') ELSE null END) transaction_id
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.requestTime') AS varchar), '"', '') ELSE null END) request_time
, CAST(replace(CAST(json_extract(ao.order_detail, '$.min_salary') AS varchar), '"', '') AS int) min_salary_mmk
, CAST(replace(CAST(json_extract(ao.order_detail, '$.total_min_salary') AS varchar), '"', '') AS int) total_min_salary_mmk
, CAST(replace(CAST(json_extract(ao.order_detail, '$.monthly_price') AS varchar), '"', '') AS double) monthly_deduction_mmk
, c.name product_type
, CAST(json_extract(ao.order_detail, '$.product.name') AS varchar) product
, CAST(json_extract(ao.order_detail, '$.product_sku') AS varchar) sku_name
, array_join(array_agg(DISTINCT COALESCE(colors, '')), '') color
, array_join(array_agg(DISTINCT COALESCE(storage_capacity, '')), '') storage_capacity
, array_join(array_agg(DISTINCT COALESCE(memory, '')), '') memory
, array_join(array_agg(DISTINCT COALESCE(size, '')), '') size
, array_join(array_agg(DISTINCT COALESCE(sim_type, '')), '') dual_or_single_sim
, array_join(array_agg(DISTINCT COALESCE(connectivity, '')), '') connectvity_4g_wifi_etc
, ' ' liter
, ' ' horsepower
, ' ' processor
, b.name pickup_location
, CAST(json_extract(ao.supplier, '$.name') AS varchar) supplier
, ao.status
, 'No' draft_without_id_selife_or_contract_uploaded
, 'No' draft_with_id_selfie_without_contract
, 'No' draft_with_contract_uploaded_without_id
, 'No' chose_deposit_and_draft_with_no_deposit
, 'No' chose_deposit_and_draft_with_no_deposit_and_id_selfie
, 'No' chose_deposit_and_draft_with_no_deposit_and_contract_uploaded
, 'No' chose_deposit_but_done_nothing
, CAST(ao.applied_date AS timestamp) applied_date
, pending.pending_date
, approved.approved_date
, rejected.rejected_date
, ordered.ordered_date
, shipping.shipping_date
, ready.ready_date
, collected.collected_date
, cancelled.cancelled_date
, active.active_date
, closed.closed_date
, ao.created_at archived_date
, null deleted_date
, org.name organization_name
, ao.start_date
, ao.end_date
, null admin_fees
, CAST(json_extract(ao.order_detail, '$.repayment_period') AS int) loan_term
, rb.interest_rate
, c1.name categroy_name
FROM
  (((((((((((((((((yomaplus_staging_archived_orders ao
LEFT JOIN yomaplus_staging_categories c ON (CAST(json_extract(ao.order_detail, '$.product.category_id') AS int) = c.category_id))
LEFT JOIN yomaplus_staging_categories c1 ON (CAST(json_extract(ao.order_detail, '$.product.category_id') AS int) = c1.category_parent_id))
LEFT JOIN (
   SELECT
     psku2.sku_name
   , (CASE WHEN (psku2.attribute_key = 'colors') THEN psku2.attribute_value ELSE null END) colors
   , (CASE WHEN (psku2.attribute_key = 'storage_capacity') THEN psku2.attribute_value ELSE null END) storage_capacity
   , (CASE WHEN (psku2.attribute_key = 'memory') THEN psku2.attribute_value ELSE null END) memory
   , (CASE WHEN (psku2.attribute_key = 'size') THEN psku2.attribute_value ELSE null END) size
   , (CASE WHEN (psku2.attribute_key = 'sim_type') THEN psku2.attribute_value ELSE null END) sim_type
   , (CASE WHEN (psku2.attribute_key = 'connectivity') THEN psku2.attribute_value ELSE null END) connectivity
   FROM
     (
      SELECT
        psku.product_id product_id
      , psku.sku_id sku_id
      , psku.sku_name
      , pav.variation_name attribute_value
      , pa.variation_name attribute_key
      , psku.price
      , psku.min_salary
      , psku.stock
      FROM
        (((yomaplus_staging_product_skus psku
      LEFT JOIN yomaplus_staging_sku_attribute_values sav ON (sav.product_sku_id = psku.sku_id))
      LEFT JOIN yomaplus_staging_product_attribute_values pav ON (pav.product_attribute_value_id = sav.product_attribute_value_id))
      LEFT JOIN yomaplus_staging_product_attributes pa ON (pa.product_attribute_id = pav.product_attribute_id))
   )  psku2
)  psku1 ON (psku1.sku_name = CAST(json_extract(ao.order_detail, '$.product_sku') AS varchar)))
LEFT JOIN yomaplus_staging_branches b ON (b.branch_id = CAST(json_extract(ao.order_detail, '$.pickup_id') AS int)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at pending_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'pending')
)  pending ON ((pending.order_id = ao.order_id) AND (pending.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at approved_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'approved')
)  approved ON ((approved.order_id = ao.order_id) AND (approved.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at rejected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'rejected')
)  rejected ON ((rejected.order_id = ao.order_id) AND (rejected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ordered_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ordered')
)  ordered ON ((ordered.order_id = ao.order_id) AND (ordered.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at shipping_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'shipping')
)  shipping ON ((shipping.order_id = ao.order_id) AND (shipping.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ready_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ready')
)  ready ON ((ready.order_id = ao.order_id) AND (ready.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at collected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'collected')
)  collected ON ((collected.order_id = ao.order_id) AND (collected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at cancelled_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'cancelled')
)  cancelled ON ((cancelled.order_id = ao.order_id) AND (cancelled.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at active_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'active')
)  active ON ((active.order_id = ao.order_id) AND (active.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at closed_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'closed')
)  closed ON ((closed.order_id = ao.order_id) AND (closed.rn = 1)))
LEFT JOIN yomaplus_staging_organizations org ON (org.organization_id = CAST(json_extract(ao.user, '$.profile.organization_id') AS int)))
LEFT JOIN yomaplus_staging_category_ratebooks cr ON ((cr.category_id = CAST(json_extract(ao.product, '$.category_id') AS int)) AND (cr.organization_id = CAST(json_extract(ao.user, '$.profile.organization_id') AS int))))
LEFT JOIN yomaplus_staging_rate_books rb ON (rb.rate_book_id = cr.rate_book_id))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61

/* yomaplus_rpt_orders_summary */
CREATE OR REPLACE VIEW "yomaplus_rpt_orders_summary" AS 
SELECT
  o.order_id
, o.order_number
, p.name product_name
, b.name product_brand
, c.name product_category
, u.user_name
, u.email
, u.phone
, org.name user_organization
, u.status user_status
, od.product_sku
, o.contract_type
, o.total_quantity
, od.repayment_period
, o.total_amount
, od.monthly_price
, od.currency
, o.total_min_salary
, od.min_salary
, o.status order_status
, od.deposit_status
, od.deposit
, od.deposit_amount
, CAST(o.start_date AS date) order_start_date
, CAST(o.end_date AS date) order_end_date
, o.created_at
, o.deleted_at
, s.name supplier_name
, s.email supplier_email
, bran.name pickup_branch
, (CASE WHEN (o.order_id IS NOT NULL) THEN 1 END) order_count
FROM
  (((((((((yomaplus_staging_orders o
LEFT JOIN yomaplus_staging_order_details od ON (od.order_id = o.order_id))
LEFT JOIN yomaplus_staging_products p ON (p.product_id = od.product_id))
LEFT JOIN yomaplus_staging_users u ON (u.user_id = o.user_id))
LEFT JOIN yomaplus_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN yomaplus_staging_organizations org ON (org.organization_id = p.organization_id))
LEFT JOIN yomaplus_staging_suppliers s ON (s.supplier_id = o.supplier_id))
LEFT JOIN yomaplus_staging_categories c ON (c.category_id = p.category_id))
LEFT JOIN yomaplus_staging_brands b ON (b.id = p.brand_id))
LEFT JOIN yomaplus_staging_branches bran ON (bran.branch_id = od.pickup_id))

/* yomaplus_rpt_products_info */
CREATE OR REPLACE VIEW "yomaplus_rpt_products_info" AS 
SELECT
  p.product_id
, sku.sku_id product_sku_id
, sku.sku_name
, p.name product_name
, p.status product_status
, (CASE WHEN (pro_able.productable_type = 'suppliers') THEN s.name WHEN (pro_able.productable_type = 'users') THEN u.user_name END) supplier_name
, pro_able.productable_type
, (CASE WHEN (pro_able.productable_type = 'suppliers') THEN s.phone WHEN (pro_able.productable_type = 'users') THEN u.phone END) supplier_phone
, (CASE WHEN (pro_able.productable_type = 'suppliers') THEN s.email WHEN (pro_able.productable_type = 'users') THEN u.email END) supplier_email
, s.organization_type
, (CASE WHEN (pro_able.productable_type = 'suppliers') THEN s.status WHEN (pro_able.productable_type = 'users') THEN u.status END) supplier_status
, p.created_at
, p.deleted_at
, p.updated_at
FROM
  ((((yomaplus_staging_products p
LEFT JOIN yomaplus_staging_productables pro_able ON (pro_able.product_id = p.product_id))
LEFT JOIN yomaplus_staging_suppliers s ON ((s.supplier_id = pro_able.productable_id) AND (pro_able.productable_type = 'suppliers')))
LEFT JOIN yomaplus_staging_users u ON ((u.user_id = pro_able.productable_id) AND (pro_able.productable_type = 'users')))
LEFT JOIN yomaplus_staging_product_skus sku ON (sku.product_id = p.product_id))

/* yomaplus_rpt_products_management */
CREATE OR REPLACE VIEW "yomaplus_rpt_products_management" AS 
SELECT
  a.created_at
, a.supplier supplier
, a.product_type
, a.brand
, a.name
, a.model
, "array_join"("array_agg"(DISTINCT COALESCE(colors, '')), '') color
, "array_join"("array_agg"(DISTINCT COALESCE(storage_capacity, '')), '') storage_capacity
, "array_join"("array_agg"(DISTINCT COALESCE(memory, '')), '') memory
, "array_join"("array_agg"(DISTINCT COALESCE(size, '')), '') size
, "array_join"("array_agg"(DISTINCT COALESCE(sim_type, '')), '') dual_or_single_sim
, "array_join"("array_agg"(DISTINCT COALESCE(connectivity, '')), '') connectvity_4g_wifi_etc
, ' ' liter
, ' ' horsepower
, ' ' processor
, a.price
, a.min_salary
, a.status
, a.sku_id
, a.sku_name
, a.stock number_of_stock
, a.deleted_at
FROM
  (
   SELECT
     psku1.sku_id
   , s.name supplier
   , c.name product_type
   , b.name brand
   , p.name
   , psku1.sku_name
   , bm.name model
   , psku1.price
   , psku1.min_salary
   , p.status
   , psku1.stock
   , (CASE WHEN (psku1.attribute_key = 'colors') THEN psku1.attribute_value ELSE null END) colors
   , (CASE WHEN (psku1.attribute_key = 'storage_capacity') THEN psku1.attribute_value ELSE null END) storage_capacity
   , (CASE WHEN (psku1.attribute_key = 'memory') THEN psku1.attribute_value ELSE null END) memory
   , (CASE WHEN (psku1.attribute_key = 'size') THEN psku1.attribute_value ELSE null END) size
   , (CASE WHEN (psku1.attribute_key = 'sim_type') THEN psku1.attribute_value ELSE null END) sim_type
   , (CASE WHEN (psku1.attribute_key = 'connectivity') THEN psku1.attribute_value ELSE null END) connectivity
   , p.created_at
   , p.deleted_at
   FROM
     (((((((
      SELECT
        psku.product_id product_id
      , psku.sku_id sku_id
      , psku.sku_name
      , pav.variation_name attribute_value
      , pa.variation_name attribute_key
      , psku.price
      , psku.min_salary
      , psku.stock
      FROM
        (((yomaplus_staging_product_skus psku
      LEFT JOIN yomaplus_staging_sku_attribute_values sav ON (sav.product_sku_id = psku.sku_id))
      LEFT JOIN yomaplus_staging_product_attribute_values pav ON (pav.product_attribute_value_id = sav.product_attribute_value_id))
      LEFT JOIN yomaplus_staging_product_attributes pa ON (pa.product_attribute_id = pav.product_attribute_id))
   )  psku1
   LEFT JOIN yomaplus_staging_products p ON (psku1.product_id = p.product_id))
   LEFT JOIN yomaplus_staging_productables p2 ON ((p2.product_id = p.product_id) AND (p2.productable_type = 'suppliers')))
   LEFT JOIN yomaplus_staging_suppliers s ON (s.supplier_id = p2.productable_id))
   LEFT JOIN yomaplus_staging_categories c ON (c.category_id = p.category_id))
   LEFT JOIN yomaplus_staging_brands b ON (b.id = p.brand_id))
   LEFT JOIN yomaplus_staging_brand_models bm ON (bm.brand_model_id = p.model_category_id))
   WHERE (psku1.product_id IN (SELECT product_id
FROM
  yomaplus_staging_products
))
)  a
GROUP BY 1, 2, 3, 4, 5, 6, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22

/* yomaplus_rpt_summary_data */
CREATE OR REPLACE VIEW "yomaplus_rpt_summary_data" AS 
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
, o.order_id
, o.order_number
, od.product_sku
, pro.name product_name
, brand.name brand_name
, c.name product_category
, s.name supplier_name
, s.email supplier_email
, b.name pickup_branch
, o.total_quantity
, o.total_amount
, o.total_min_salary
, od.monthly_price
, od.retail_price
, o.status order_status
, o.start_date order_start_date
, o.end_date order_end_date
, e.created_at employee_created_at
, e.updated_at employee_updated_at
, e.deleted_at employee_deleted_at
, u.created_at user_created_at
, u.updated_at user_updated_at
, u.deleted_at user_deleted_at
, o.created_at order_created_at
, o.updated_at order_updated_at
, o.deleted_at order_deleted_at
FROM
  (((((((((yomaplus_staging_employees e
LEFT JOIN yomaplus_staging_profiles p ON ("upper"(p.identification_number) = "upper"(e.identification_number)))
LEFT JOIN yomaplus_staging_users u ON (u.user_id = p.user_id))
LEFT JOIN yomaplus_staging_orders o ON (o.user_id = p.user_id))
LEFT JOIN yomaplus_staging_order_details od ON (od.order_id = o.order_id))
LEFT JOIN yomaplus_staging_products pro ON (pro.product_id = od.product_id))
LEFT JOIN yomaplus_staging_branches b ON (b.branch_id = od.pickup_id))
LEFT JOIN yomaplus_staging_brands brand ON (brand.id = pro.brand_id))
LEFT JOIN yomaplus_staging_categories c ON (c.category_id = pro.category_id))
LEFT JOIN yomaplus_staging_suppliers s ON (s.supplier_id = o.supplier_id))

/* yomaplus_rpt_suppliers_management */
CREATE OR REPLACE VIEW "yomaplus_rpt_suppliers_management" AS 
SELECT
  s.supplier_id supplier_id
, s.name supplier_name
, s.phone
, s.created_at
, s.updated_at
, count(DISTINCT p1.category_id) no_of_product_type
, count(DISTINCT p1.model_category_id) no_of_product_model
, sum((CASE WHEN ((p1.product_id = ps.product_id) AND (p1.status = 'in_stock') AND (ps.stock > 0)) THEN ps.stock END)) no_of_stock
, count(DISTINCT (CASE WHEN (p1.status = 'out_of_stock') THEN p1.model_category_id END)) no_of_out_of_stock_model
, b.total_pickup_locations
FROM
  ((((((yomaplus_staging_suppliers s
LEFT JOIN yomaplus_staging_productables p ON ((p.productable_id = s.supplier_id) AND (p.productable_type = 'suppliers')))
LEFT JOIN yomaplus_staging_products p1 ON (p1.product_id = p.product_id))
LEFT JOIN yomaplus_staging_categories c ON (c.category_id = p1.category_id))
LEFT JOIN yomaplus_staging_brand_models bm ON (bm.brand_model_id = p1.model_category_id))
LEFT JOIN yomaplus_staging_product_skus ps ON (ps.product_id = p1.product_id))
LEFT JOIN (
   SELECT
     supplier_id
   , count(DISTINCT branch_id) total_pickup_locations
   FROM
     yomaplus_staging_branches
   WHERE (deleted_at IS NULL)
   GROUP BY 1
)  b ON (b.supplier_id = s.supplier_id))
WHERE ((c.deleted_at IS NULL) AND (bm.deleted_at IS NULL) AND (s.deleted_at IS NULL))
GROUP BY 1, 2, 3, 4, 5, 10

/* yomaplus_rpt_suppliers_summary */
CREATE OR REPLACE VIEW "yomaplus_rpt_suppliers_summary" AS 
SELECT
  s.supplier_id
, s.name supplier_name
, s.phone
, s.email
, s.organization_type
, s.status supplier_status
, p.product_id
, p.name product_name
, sku.sku_name product_sku
, sku.stock product_sku_stock
, c.name product_category
, b.name product_brand
, p.status product_status
, p.deleted_at product_deleted_at
FROM
  ((((((yomaplus_staging_suppliers s
LEFT JOIN yomaplus_staging_productables pro_ables ON ((pro_ables.productable_id = s.supplier_id) AND (productable_type = 'suppliers')))
LEFT JOIN yomaplus_staging_products p ON (p.product_id = pro_ables.product_id))
LEFT JOIN yomaplus_staging_users u ON ((u.user_id = pro_ables.productable_id) AND (productable_type = 'users')))
LEFT JOIN yomaplus_staging_categories c ON (c.category_id = p.category_id))
LEFT JOIN yomaplus_staging_brands b ON (b.id = p.brand_id))
LEFT JOIN yomaplus_staging_product_skus sku ON (sku.product_id = p.product_id))

/* yomaplus_rpt_user_info */
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

/* yomaplus_rpt_users_summary */
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

/* yomaplus_v2_rpt_order_management */
CREATE OR REPLACE VIEW "yomaplus_v2_rpt_order_management" AS 
SELECT
  o.order_id order_id
, o.order_number
, u.user_name employee_name
, p.identification_number nric_passport_number
, p.entity entity_name
, u.phone phone_number
, u.email
, e.entity_hr_email
, o.total_amount price_mmk
, od.deposit deposit_percent
, ((od.retail_price * od.deposit) / 100) deposit_amount
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.status') AS varchar), '"', '') ELSE null END) payment_status
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.initiatorMsisdn') AS varchar), '"', '') ELSE null END) msisdn
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.merchantReferenceId') AS varchar), '"', '') ELSE null END) reference_id
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.transactionId') AS varchar), '"', '') ELSE null END) transaction_id
, (CASE WHEN (od.deposit_result IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(od.deposit_result, '$.requestTime') AS varchar), '"', '') ELSE null END) request_time
, od.min_salary min_salary_mmk
, o.total_min_salary total_min_salary_mmk
, od.monthly_price monthly_deduction_mmk
, cat.name product_type
, pro.name product
, od.product_sku sku_name
, array_join(array_agg(DISTINCT COALESCE(colors, '')), '') color
, array_join(array_agg(DISTINCT COALESCE(storage_capacity, '')), '') storage_capacity
, array_join(array_agg(DISTINCT COALESCE(memory, '')), '') memory
, array_join(array_agg(DISTINCT COALESCE(size, '')), '') size
, array_join(array_agg(DISTINCT COALESCE(sim_type, '')), '') dual_or_single_sim
, array_join(array_agg(DISTINCT COALESCE(connectivity, '')), '') connectvity_4g_wifi_etc
, ' ' liter
, ' ' horsepower
, ' ' processor
, b.name pickup_location
, s.name supplier
, o.status
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) OR (o.contract_type IS NULL))) THEN 'Yes' ELSE 'No' END) draft_without_id_selife_or_contract_uploaded
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NOT NULL) AND (o.contract_type IS NULL))) THEN 'Yes' ELSE 'No' END) draft_with_id_selfie_without_contract
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) AND (o.contract_type IS NOT NULL))) THEN 'Yes' ELSE 'No' END) draft_with_contract_uploaded_without_id
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) AND (o.contract_type IS NOT NULL)) AND (od.deposit IS NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_and_draft_with_no_deposit
, (CASE WHEN ((o.status = 'draft') AND (p.identity_front_image IS NULL) AND (od.deposit IS NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_and_draft_with_no_deposit_and_id_selfie
, (CASE WHEN ((o.status = 'draft') AND (o.contract_type IS NOT NULL) AND (od.deposit IS NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_and_draft_with_no_deposit_and_contract_uploaded
, (CASE WHEN ((o.status = 'draft') AND ((p.identity_front_image IS NULL) AND (o.contract_type IS NULL)) AND (od.deposit IS NOT NULL)) THEN 'Yes' ELSE 'No' END) chose_deposit_but_done_nothing
, o.created_at applied_date
, pending.pending_date
, approved.approved_date
, rejected.rejected_date
, ordered.ordered_date
, shipping.shipping_date
, ready.ready_date
, collected.collected_date
, cancelled.cancelled_date
, active.active_date
, (CASE WHEN (o.status IN ('closed', 'archived')) THEN o.updated_at ELSE null END) closed_date
, ao.created_at archived_date
, o.deleted_at deleted_date
, org.name organization_name
, o.start_date
, o.end_date
, od.service_fees admin_fees
, od.repayment_period loan_term
, rb.interest_rate
, cat1.name category_name
FROM
  (((((((((((((((((((((((yomaplus_staging_orders o
LEFT JOIN yomaplus_staging_order_details od ON (od.order_id = o.order_id))
LEFT JOIN yomaplus_staging_users u ON (u.user_id = o.user_id))
LEFT JOIN yomaplus_staging_profiles p ON (p.user_id = u.user_id))
LEFT JOIN yomaplus_staging_employees e ON ((e.identification_number = p.identification_number) AND (e.deleted_at IS NULL) AND (e.organization_id = p.organization_id)))
LEFT JOIN yomaplus_staging_products pro ON (pro.product_id = od.product_id))
LEFT JOIN yomaplus_staging_categories cat ON (cat.category_id = pro.category_id))
LEFT JOIN yomaplus_staging_categories cat1 ON (cat1.category_id = cat.category_parent_id))
LEFT JOIN yomaplus_staging_organizations org ON (org.organization_id = p.organization_id))
LEFT JOIN (
   SELECT
     psku2.sku_name
   , (CASE WHEN (psku2.attribute_key = 'colors') THEN psku2.attribute_value ELSE null END) colors
   , (CASE WHEN (psku2.attribute_key = 'storage_capacity') THEN psku2.attribute_value ELSE null END) storage_capacity
   , (CASE WHEN (psku2.attribute_key = 'memory') THEN psku2.attribute_value ELSE null END) memory
   , (CASE WHEN (psku2.attribute_key = 'size') THEN psku2.attribute_value ELSE null END) size
   , (CASE WHEN (psku2.attribute_key = 'sim_type') THEN psku2.attribute_value ELSE null END) sim_type
   , (CASE WHEN (psku2.attribute_key = 'connectivity') THEN psku2.attribute_value ELSE null END) connectivity
   FROM
     (
      SELECT
        psku.product_id product_id
      , psku.sku_id sku_id
      , psku.sku_name
      , pav.variation_name attribute_value
      , pa.variation_name attribute_key
      , psku.price
      , psku.min_salary
      , psku.stock
      FROM
        (((yomaplus_staging_product_skus psku
      LEFT JOIN yomaplus_staging_sku_attribute_values sav ON (sav.product_sku_id = psku.sku_id))
      LEFT JOIN yomaplus_staging_product_attribute_values pav ON (pav.product_attribute_value_id = sav.product_attribute_value_id))
      LEFT JOIN yomaplus_staging_product_attributes pa ON (pa.product_attribute_id = pav.product_attribute_id))
   )  psku2
)  psku1 ON (psku1.sku_name = od.product_sku))
LEFT JOIN yomaplus_staging_branches b ON (b.branch_id = od.pickup_id))
LEFT JOIN yomaplus_staging_suppliers s ON (s.supplier_id = o.supplier_id))
LEFT JOIN (
   SELECT
     order_id
   , updated_at pending_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'pending')
)  pending ON ((pending.order_id = o.order_id) AND (pending.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at approved_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'approved')
)  approved ON ((approved.order_id = o.order_id) AND (approved.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at rejected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'rejected')
)  rejected ON ((rejected.order_id = o.order_id) AND (rejected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ordered_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ordered')
)  ordered ON ((ordered.order_id = o.order_id) AND (ordered.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at shipping_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'shipping')
)  shipping ON ((shipping.order_id = o.order_id) AND (shipping.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ready_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ready')
)  ready ON ((ready.order_id = o.order_id) AND (ready.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at collected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'collected')
)  collected ON ((collected.order_id = o.order_id) AND (collected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at cancelled_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'cancelled')
)  cancelled ON ((cancelled.order_id = o.order_id) AND (cancelled.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at active_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'active')
)  active ON ((active.order_id = o.order_id) AND (active.rn = 1)))
LEFT JOIN yomaplus_staging_archived_orders ao ON (ao.order_id = o.order_id))
LEFT JOIN yomaplus_staging_category_ratebooks cr ON ((cr.category_id = pro.category_id) AND (cr.organization_id = p.organization_id)))
LEFT JOIN yomaplus_staging_rate_books rb ON (rb.rate_book_id = cr.rate_book_id))
WHERE (o.status <> 'archived')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61
UNION SELECT
  ao.order_id
, ao.order_number
, replace(CAST(json_extract(ao.user, '$.name') AS varchar), '"', '') employee_name
, replace(CAST(json_extract(ao.user, '$.profile.identification_number') AS varchar), '"', '') nric_passport_number
, replace(CAST(json_extract(ao.user, '$.profile.entity') AS varchar), '"', '') entity_name
, replace(CAST(json_extract(ao.user, '$.phone') AS varchar), '"', '') phone_number
, replace(CAST(json_extract(ao.user, '$.email') AS varchar), '"', '') email
, replace(CAST(json_extract(ao.user, '$.profile.entity_email') AS varchar), '"', '') entity_hr_email
, ao.total_amount price_mmk
, CAST(replace(CAST(json_extract(ao.order_detail, '$.deposit') AS varchar), '"', '') AS double) deposit_percent
, ((CAST(replace(CAST(json_extract(ao.order_detail, '$.retail_price') AS varchar), '"', '') AS int) * CAST(replace(CAST(json_extract(ao.order_detail, '$.deposit') AS varchar), '"', '') AS int)) / 100) deposit_amount
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.status') AS varchar), '"', '') ELSE null END) payment_status
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.initiatorMsisdn') AS varchar), '"', '') ELSE null END) msisdn
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.merchantReferenceId') AS varchar), '"', '') ELSE null END) reference_id
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.transactionId') AS varchar), '"', '') ELSE null END) transaction_id
, (CASE WHEN (json_extract(ao.order_detail, '$.deposit_result') IS NOT NULL) THEN replace(CAST(JSON_EXTRACT(ao.order_detail, '$.deposit_result.requestTime') AS varchar), '"', '') ELSE null END) request_time
, CAST(replace(CAST(json_extract(ao.order_detail, '$.min_salary') AS varchar), '"', '') AS int) min_salary_mmk
, CAST(replace(CAST(json_extract(ao.order_detail, '$.total_min_salary') AS varchar), '"', '') AS int) total_min_salary_mmk
, CAST(replace(CAST(json_extract(ao.order_detail, '$.monthly_price') AS varchar), '"', '') AS double) monthly_deduction_mmk
, c.name product_type
, CAST(json_extract(ao.order_detail, '$.product.name') AS varchar) product
, CAST(json_extract(ao.order_detail, '$.product_sku') AS varchar) sku_name
, array_join(array_agg(DISTINCT COALESCE(colors, '')), '') color
, array_join(array_agg(DISTINCT COALESCE(storage_capacity, '')), '') storage_capacity
, array_join(array_agg(DISTINCT COALESCE(memory, '')), '') memory
, array_join(array_agg(DISTINCT COALESCE(size, '')), '') size
, array_join(array_agg(DISTINCT COALESCE(sim_type, '')), '') dual_or_single_sim
, array_join(array_agg(DISTINCT COALESCE(connectivity, '')), '') connectvity_4g_wifi_etc
, ' ' liter
, ' ' horsepower
, ' ' processor
, b.name pickup_location
, CAST(json_extract(ao.supplier, '$.name') AS varchar) supplier
, ao.status
, 'No' draft_without_id_selife_or_contract_uploaded
, 'No' draft_with_id_selfie_without_contract
, 'No' draft_with_contract_uploaded_without_id
, 'No' chose_deposit_and_draft_with_no_deposit
, 'No' chose_deposit_and_draft_with_no_deposit_and_id_selfie
, 'No' chose_deposit_and_draft_with_no_deposit_and_contract_uploaded
, 'No' chose_deposit_but_done_nothing
, CAST(ao.applied_date AS timestamp) applied_date
, pending.pending_date
, approved.approved_date
, rejected.rejected_date
, ordered.ordered_date
, shipping.shipping_date
, ready.ready_date
, collected.collected_date
, cancelled.cancelled_date
, active.active_date
, closed.closed_date
, ao.created_at archived_date
, null deleted_date
, org.name organization_name
, ao.start_date
, ao.end_date
, null admin_fees
, CAST(json_extract(ao.order_detail, '$.repayment_period') AS int) loan_term
, rb.interest_rate
, c1.name categroy_name
FROM
  (((((((((((((((((yomaplus_staging_archived_orders ao
LEFT JOIN yomaplus_staging_categories c ON (CAST(json_extract(ao.order_detail, '$.product.category_id') AS int) = c.category_id))
LEFT JOIN yomaplus_staging_categories c1 ON (CAST(json_extract(ao.order_detail, '$.product.category_id') AS int) = c1.category_parent_id))
LEFT JOIN (
   SELECT
     psku2.sku_name
   , (CASE WHEN (psku2.attribute_key = 'colors') THEN psku2.attribute_value ELSE null END) colors
   , (CASE WHEN (psku2.attribute_key = 'storage_capacity') THEN psku2.attribute_value ELSE null END) storage_capacity
   , (CASE WHEN (psku2.attribute_key = 'memory') THEN psku2.attribute_value ELSE null END) memory
   , (CASE WHEN (psku2.attribute_key = 'size') THEN psku2.attribute_value ELSE null END) size
   , (CASE WHEN (psku2.attribute_key = 'sim_type') THEN psku2.attribute_value ELSE null END) sim_type
   , (CASE WHEN (psku2.attribute_key = 'connectivity') THEN psku2.attribute_value ELSE null END) connectivity
   FROM
     (
      SELECT
        psku.product_id product_id
      , psku.sku_id sku_id
      , psku.sku_name
      , pav.variation_name attribute_value
      , pa.variation_name attribute_key
      , psku.price
      , psku.min_salary
      , psku.stock
      FROM
        (((yomaplus_staging_product_skus psku
      LEFT JOIN yomaplus_staging_sku_attribute_values sav ON (sav.product_sku_id = psku.sku_id))
      LEFT JOIN yomaplus_staging_product_attribute_values pav ON (pav.product_attribute_value_id = sav.product_attribute_value_id))
      LEFT JOIN yomaplus_staging_product_attributes pa ON (pa.product_attribute_id = pav.product_attribute_id))
   )  psku2
)  psku1 ON (psku1.sku_name = CAST(json_extract(ao.order_detail, '$.product_sku') AS varchar)))
LEFT JOIN yomaplus_staging_branches b ON (b.branch_id = CAST(json_extract(ao.order_detail, '$.pickup_id') AS int)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at pending_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'pending')
)  pending ON ((pending.order_id = ao.order_id) AND (pending.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at approved_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'approved')
)  approved ON ((approved.order_id = ao.order_id) AND (approved.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at rejected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'rejected')
)  rejected ON ((rejected.order_id = ao.order_id) AND (rejected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ordered_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ordered')
)  ordered ON ((ordered.order_id = ao.order_id) AND (ordered.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at shipping_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'shipping')
)  shipping ON ((shipping.order_id = ao.order_id) AND (shipping.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at ready_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'ready')
)  ready ON ((ready.order_id = ao.order_id) AND (ready.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at collected_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'collected')
)  collected ON ((collected.order_id = ao.order_id) AND (collected.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at cancelled_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'cancelled')
)  cancelled ON ((cancelled.order_id = ao.order_id) AND (cancelled.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at active_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'active')
)  active ON ((active.order_id = ao.order_id) AND (active.rn = 1)))
LEFT JOIN (
   SELECT
     order_id
   , updated_at closed_date
   , row_number() OVER (PARTITION BY order_id, status ORDER BY updated_at DESC) rn
   FROM
     yomaplus_staging_order_histories
   WHERE (status = 'closed')
)  closed ON ((closed.order_id = ao.order_id) AND (closed.rn = 1)))
LEFT JOIN yomaplus_staging_organizations org ON (org.organization_id = CAST(json_extract(ao.user, '$.profile.organization_id') AS int)))
LEFT JOIN yomaplus_staging_category_ratebooks cr ON ((cr.category_id = CAST(json_extract(ao.product, '$.category_id') AS int)) AND (cr.organization_id = CAST(json_extract(ao.user, '$.profile.organization_id') AS int))))
LEFT JOIN yomaplus_staging_rate_books rb ON (rb.rate_book_id = cr.rate_book_id))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61