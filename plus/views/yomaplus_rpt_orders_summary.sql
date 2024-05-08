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