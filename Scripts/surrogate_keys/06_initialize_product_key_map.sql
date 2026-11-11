/*
===============================================================================
Script: 06_initialize_product_key_map.sql
Purpose: Preserve existing product surrogate-key assignments.
===============================================================================

Description:
    Copies product keys and business product numbers from
    dbt_dev.dim_products into the persistent mapping table.

    IDENTITY_INSERT preserves the original surrogate keys.

Execution:
    - Run only once, after creating product_key_map.
    - Requires an empty mapping table.
    - Do not execute during regular dbt builds.

Expected Initial Result:
    295 product mappings.
===============================================================================
*/

USE DataWarehouse;
GO

-- Validate prerequisites before modifying data.
IF OBJECT_ID(N'dbt_dev.product_key_map', N'U') IS NULL
    THROW 50003, 'Product mapping table does not exist.', 1;

IF EXISTS (
    SELECT 1 FROM dbt_dev.product_key_map
)
    THROW 50004, 'Product mapping is not empty. Stop.', 1;

IF EXISTS (
    SELECT 1
    FROM dbt_dev.dim_products
    WHERE product_key IS NULL
       OR product_number IS NULL
)
    THROW 50005, 'Source product keys contain NULLs.', 1;

BEGIN TRY
    BEGIN TRANSACTION;

    SET IDENTITY_INSERT dbt_dev.product_key_map ON;

    INSERT INTO dbt_dev.product_key_map (
        product_key,
        product_number
    )
    SELECT
        product_key,
        product_number
    FROM dbt_dev.dim_products;

    SET IDENTITY_INSERT dbt_dev.product_key_map OFF;

    COMMIT TRANSACTION;

    SELECT
        COUNT(*) AS initialized_product_mappings
    FROM dbt_dev.product_key_map;

END TRY
BEGIN CATCH

    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    SET IDENTITY_INSERT dbt_dev.product_key_map OFF;

    THROW;

END CATCH;
GO
