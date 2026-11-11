-- Detect blank countries or country codes
-- that should have been standardized.

SELECT
    cid,
    cntry

FROM {{ ref('stg_erp_locations') }}

WHERE
    TRIM(cntry) = ''
    OR TRIM(cntry) IN ('DE', 'US', 'USA')
