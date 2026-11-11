{{ config(materialized='view') }}

WITH product_history AS (

    SELECT
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,

        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        ) AS next_start_date

    FROM {{ source('crm', 'crm_prd_info') }}

)

SELECT
    prd_id,

    -- Extract category ID and standardize its format
    REPLACE(
        SUBSTRING(prd_key, 1, 5),
        '-',
        '_'
    ) AS cat_id,

    -- Extract the product identifier
    SUBSTRING(
        prd_key, 7, LEN(prd_key)
    ) AS prd_key,

    prd_nm,

    -- Replace missing product costs with zero
    ISNULL(prd_cost, 0) AS prd_cost,

    -- Standardize product line codes
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'M'
            THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R'
            THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S'
            THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T'
            THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    -- End date is one day before the next version starts
    CAST(
        DATEADD(DAY, -1, next_start_date)
        AS DATE
    ) AS prd_end_dt

FROM product_history
