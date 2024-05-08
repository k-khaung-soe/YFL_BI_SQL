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