{{ config(materialized='view') }}

WITH source_customers AS (

    SELECT
        cid,
        bdate,
        gen

    FROM {{ source('erp', 'erp_cust_az12') }}

)

SELECT

    -- Standardize ERP customer identifiers
    -- by removing the NAS prefix if present
    CASE
        WHEN cid LIKE 'NAS%'
            THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,

    -- Replace future birthdates with NULL
    CASE
        WHEN bdate > GETDATE()
            THEN NULL
        ELSE bdate
    END AS bdate,

    -- Standardize gender values
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE')
            THEN 'Female'

        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')
            THEN 'Male'

        ELSE 'n/a'
    END AS gen

FROM source_customers
