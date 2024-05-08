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
