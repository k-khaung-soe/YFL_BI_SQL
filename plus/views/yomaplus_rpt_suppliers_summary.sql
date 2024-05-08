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
