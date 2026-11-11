-- Return transactions with missing or nonpositive
-- quantities, sales amounts, or unit prices.

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_quantity,
    sls_sales,
    sls_price

FROM {{ ref('stg_crm_sales') }}

WHERE
       sls_quantity IS NULL
    OR sls_quantity <= 0
    OR sls_sales IS NULL
    OR sls_sales <= 0
    OR sls_price IS NULL
    OR sls_price <= 0
