-- Verify that dimensional joins preserve
-- the number of cleaned sales transaction rows.

SELECT
    f.fact_rows,
    s.staging_rows

FROM (
    SELECT COUNT_BIG(*) AS fact_rows
    FROM {{ ref('fact_sales') }}
) f

CROSS JOIN (
    SELECT COUNT_BIG(*) AS staging_rows
    FROM {{ ref('stg_crm_sales') }}
) s

WHERE f.fact_rows <> s.staging_rows
