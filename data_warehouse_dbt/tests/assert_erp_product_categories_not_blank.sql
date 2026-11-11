-- Detect category names that contain
-- only whitespace or are empty.

SELECT
    id,
    cat,
    subcat

FROM {{ ref('stg_erp_product_categories') }}

WHERE
    cat IS NOT NULL
    AND TRIM(cat) = ''
