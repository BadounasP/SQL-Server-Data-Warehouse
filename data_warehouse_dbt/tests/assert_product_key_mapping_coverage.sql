-- Test: Product surrogate-key mapping coverage
--
-- Every active CRM product must have an
-- assigned persistent surrogate key.
--
-- Historical inactive versions are excluded.
-- The test passes when zero rows are returned.

SELECT
    p.prd_id AS product_id,
    p.prd_key AS product_number

FROM {{ ref('stg_crm_products') }} AS p

LEFT JOIN {{ source('surrogate_keys', 'product_key_map') }} AS m
    ON p.prd_key = m.product_number

WHERE
    p.prd_end_dt IS NULL
    AND m.product_key IS NULL
