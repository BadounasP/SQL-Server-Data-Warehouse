/*
===============================================================================
Script: 04_sync_customer_key_map.sql
Purpose: Assign persistent surrogate keys to new CRM customers.
===============================================================================

Description:
    Identifies customers in the cleaned CRM staging model
    that do not yet exist in the customer key mapping table.

    Inserts only new customer IDs, allowing SQL Server IDENTITY
    to generate their surrogate keys.

    Existing customer mappings are never updated or deleted.

Execution:
    - Run after CRM staging data is available.
    - Run before rebuilding Gold customer and sales models.
    - Safe to execute repeatedly with unchanged source data.

===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE dbt_dev.sync_customer_key_map
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO dbt_dev.customer_key_map (
            customer_id
        )

        SELECT
            s.cst_id

        FROM dbt_dev.stg_crm_customers AS s

        WHERE s.cst_id IS NOT NULL
          AND NOT EXISTS (

              SELECT 1
              FROM dbt_dev.customer_key_map AS m
                   WITH (UPDLOCK, HOLDLOCK)

              WHERE m.customer_id = s.cst_id

          );

        DECLARE @inserted_rows INT = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT
            @inserted_rows AS new_customer_mappings_added;

    END TRY

    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;

    END CATCH
END;
GO

-- Synchronize customer mappings.
EXEC dbt_dev.sync_customer_key_map;
GO
