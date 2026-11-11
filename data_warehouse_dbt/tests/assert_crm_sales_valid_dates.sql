-- Identify inconsistent transaction date sequences.

SELECT
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt

FROM {{ ref('stg_crm_sales') }}

WHERE
    (
        sls_order_dt IS NOT NULL
        AND sls_ship_dt IS NOT NULL
        AND sls_ship_dt < sls_order_dt
    )
    OR
    (
        sls_order_dt IS NOT NULL
        AND sls_due_dt IS NOT NULL
        AND sls_due_dt < sls_order_dt
    )
    OR
    (
        sls_ship_dt IS NOT NULL
        AND sls_due_dt IS NOT NULL
        AND sls_due_dt < sls_ship_dt
    )
