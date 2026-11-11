{{ config(materialized='view') }}

WITH source_locations AS (

    SELECT
        cid,
        cntry

    FROM {{ source('erp', 'erp_loc_a101') }}

)

SELECT

    -- Remove hyphens from ERP customer identifiers
    REPLACE(
        cid,
        '-',
        ''
    ) AS cid,

    -- Standardize country names and handle missing values
    CASE
        WHEN TRIM(cntry) = 'DE'
            THEN 'Germany'

        WHEN TRIM(cntry) IN ('US', 'USA')
            THEN 'United States'

        WHEN TRIM(cntry) = ''
             OR cntry IS NULL
            THEN 'n/a'

        ELSE TRIM(cntry)

    END AS cntry

FROM source_locations
