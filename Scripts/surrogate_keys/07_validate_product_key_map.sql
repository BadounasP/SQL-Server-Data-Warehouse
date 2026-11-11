/*
===============================================================================
Script: 07_validate_product_key_map.sql
Purpose: Verify product surrogate-key mapping consistency.
===============================================================================

Description:
    Compares the product dimension against its persistent
    mapping table, checking for missing or incorrect assignments.

Execution:
    - Read-only and safe to rerun.
    - Run after initializing the mapping table.

Expected Initial Result:
    295 dimension rows.
    295 mapping rows.
    Zero mismatches in either direction.
===============================================================================
*/

USE DataWarehouse;
GO

WITH dimension_only AS (

    SELECT product_key, product_number
    FROM dbt_dev.dim_products

    EXCEPT

    SELECT product_key, product_number
    FROM dbt_dev.product_key_map

),

mapping_only AS (

    SELECT product_key, product_number
    FROM dbt_dev.product_key_map

    EXCEPT

    SELECT product_key, product_number
    FROM dbt_dev.dim_products

)

SELECT
    (SELECT COUNT(*)
     FROM dbt_dev.dim_products)
        AS dimension_rows,

    (SELECT COUNT(*)
     FROM dbt_dev.product_key_map)
        AS mapping_rows,

    (SELECT COUNT(*)
     FROM dimension_only)
        AS mappings_missing_or_different,

    (SELECT COUNT(*)
     FROM mapping_only)
        AS unexpected_mappings;
GO
