{{ config(materialized='view') }}

-- Gold Sales Fact Model
-- Connects CRM sales transactions to the
-- customer and product dimensions.

SELECT
    sd.sls_ord_num  AS order_number,

    -- Dimension surrogate keys
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,

    -- Transaction dates
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

    -- Financial and quantity measures
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price

FROM {{ ref('stg_crm_sales') }} sd

LEFT JOIN {{ ref('dim_products') }} pr
    ON sd.sls_prd_key = pr.product_number

LEFT JOIN {{ ref('dim_customers') }} cu
    ON sd.sls_cust_id = cu.customer_id
