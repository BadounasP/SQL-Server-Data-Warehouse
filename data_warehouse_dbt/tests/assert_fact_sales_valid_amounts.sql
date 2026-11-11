-- Reject sales rows with missing or nonpositive
-- financial measures.

SELECT
    order_number,
    product_key,
    customer_key,
    sales_amount,
    quantity,
    price

FROM {{ ref('fact_sales') }}

WHERE
       sales_amount IS NULL
    OR sales_amount <= 0
    OR quantity IS NULL
    OR quantity <= 0
    OR price IS NULL
    OR price <= 0
