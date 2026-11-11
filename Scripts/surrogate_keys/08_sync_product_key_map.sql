/*
===============================================================================
Script: 08_sync_product_key_map.sql
Purpose: Synchronize persistent surrogate keys for active products.
===============================================================================

Description:
    Identifies active CRM products that do not yet have a
    corresponding entry in dbt_dev.product_key_map.

    New product numbers receive permanent surrogate keys
    through SQL Server's IDENTITY mechanism.

    Existing mappings are never updated or deleted.

Execution:
    - Run after stg_crm_products is available.
    - Run before rebuilding Gold dimensions and facts.
    - Safe to rerun with unchanged source data.

Important:
    The Silver model contains historical product versions.
    Only active products (prd_end_dt IS NULL) are considered.

===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE dbt_dev.sync_product_key_map
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Validate active product identifiers.
        IF EXISTS (
            SELECT 1
            FROM dbt_dev.stg_crm_products
            WHERE prd_end_dt IS NULL
              AND (
                  prd_key IS NULL
                  OR LEN(prd_key) > 50
              )
        )
        BEGIN
            THROW 50006,
                'Invalid active product identifier.',
                1;
        END;

        -- Ensure only one active version exists per product.
        IF EXISTS (
            SELECT prd_key
            FROM dbt_dev.stg_crm_products
            WHERE prd_end_dt IS NULL
            GROUP BY prd_key
            HAVING COUNT(*) > 1
        )
        BEGIN
            THROW 50007,
                'Duplicate active product numbers detected.',
                1;
        END;

        -- Insert only products not yet mapped.
        INSERT INTO dbt_dev.product_key_map (
            product_number
        )
        SELECT
            p.prd_key

        FROM dbt_dev.stg_crm_products AS p

        WHERE p.prd_end_dt IS NULL
          AND NOT EXISTS (
              SELECT 1

              FROM dbt_dev.product_key_map AS m
                   WITH (UPDLOCK, HOLDLOCK)

              WHERE m.product_number = p.prd_key
          );

        DECLARE @inserted_rows INT = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT
            @inserted_rows AS new_product_mappings_added;

    END TRY
    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;

    END CATCH;
END;
GO

-- Execute the synchronization procedure.
EXEC dbt_dev.sync_product_key_map;
GO
