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
