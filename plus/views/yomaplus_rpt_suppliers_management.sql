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