-- Detect customer IDs that still contain
-- the original NAS prefix.

SELECT
    cid
FROM {{ ref('stg_erp_customers') }}
WHERE cid LIKE 'NAS%'
