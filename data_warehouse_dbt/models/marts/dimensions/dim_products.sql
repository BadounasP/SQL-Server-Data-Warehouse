{{ config(materialized='view') }}

-- Gold Product Dimension
-- Integrates CRM product data with ERP categories.
-- Uses persistent surrogate keys from SQL Server.

SELECT

    -- Retrieve the permanent product surrogate key.
    m.product_key AS product_key,

    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,

    pn.cat_id AS category_id,

    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,

    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date

FROM {{ ref('stg_crm_products') }} pn

-- Retrieve the persistent product key.
LEFT JOIN {{ source('surrogate_keys', 'product_key_map') }} m
    ON pn.prd_key = m.product_number

-- Enrich products with ERP category information.
LEFT JOIN {{ ref('stg_erp_product_categories') }} pc
    ON pn.cat_id = pc.id

-- Include only currently active product versions.
WHERE pn.prd_end_dt IS NULL
