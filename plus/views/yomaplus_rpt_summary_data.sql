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