-- Detect birthdates occurring after today's date.

SELECT
    cid,
    bdate
FROM {{ ref('stg_erp_customers') }}
WHERE bdate > CAST(GETDATE() AS DATE)
