-- Detect customer IDs that still contain hyphens.
-- The test passes when zero rows are returned.

SELECT
    cid,
    cntry

FROM {{ ref('stg_erp_locations') }}

WHERE cid LIKE '%-%'
