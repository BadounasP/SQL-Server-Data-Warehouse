-- Returns product records with invalid date ranges.
-- A successful test returns zero rows.

SELECT
    prd_id,
    prd_key,
    prd_start_dt,
    prd_end_dt

FROM {{ ref('stg_crm_products') }}

WHERE
    prd_end_dt IS NOT NULL
    AND (
        prd_start_dt IS NULL
        OR prd_end_dt < prd_start_dt
    )
