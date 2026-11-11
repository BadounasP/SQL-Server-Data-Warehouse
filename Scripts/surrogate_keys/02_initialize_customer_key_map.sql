/*
===============================================================================
Script: 02_initialize_customer_key_map.sql
Purpose: One-time migration of historical customer surrogate-key assignments.
===============================================================================

IMPORTANT:
    This is a historical migration script, NOT part of the daily Airflow/dbt
    pipeline and NOT a routine fresh-install initialization step.

    Run only if you have an existing, independently populated dim_customers
    model containing the original customer_key -> customer_id assignments,
    and a NEW, EMPTY dbt_dev.customer_key_map table.

    After the migration, dbt_dev.dim_customers may depend on customer_key_map.
    In that architecture this script is not a way to initialize from scratch.

Safety measures:
    - Explicit opt-in is required.
    - Existing mappings cause an error instead of being overwritten.
    - Source records are checked for NULLs and duplicate identifiers/keys.
    - All inserts run in one transaction with rollback on error.
    - IDENTITY_INSERT is turned OFF after the migration or on failure.

    DO NOT RUN AGAINST THE EXISTING WORKING DATA WAREHOUSE.
===============================================================================
*/

USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

-- SAFETY SWITCH: leave at 0 for normal repository use.
-- Change to 1 only for an explicitly approved one-time historical migration.
DECLARE @allow_initial_migration BIT = 0;

IF @allow_initial_migration <> 1
    THROW 51001,
        'Initialization blocked: explicit one-time migration approval required.',
        1;

IF @@TRANCOUNT <> 0
    THROW 51002, 'Run this migration outside an existing transaction.', 1;

IF OBJECT_ID(N'dbt_dev.customer_key_map', N'U') IS NULL
    THROW 51003, 'Customer mapping table does not exist.', 1;

IF OBJECT_ID(N'dbt_dev.dim_customers') IS NULL
    THROW 51004, 'Historical customer dimension does not exist.', 1;

DECLARE @identity_insert_enabled BIT = 0;
DECLARE @source_rows BIGINT = 0;
DECLARE @inserted_rows INT = 0;

BEGIN TRY
    BEGIN TRANSACTION;

    -- Hold an exclusive table lock until COMMIT/ROLLBACK so that another
    -- session cannot add mappings between the emptiness check and the insert.
    IF EXISTS (
        SELECT 1
        FROM dbt_dev.customer_key_map WITH (TABLOCKX, HOLDLOCK)
    )
        THROW 51005,
            'Customer mapping table is not empty. Existing keys are preserved.',
            1;

    SELECT @source_rows = COUNT_BIG(*)
    FROM dbt_dev.dim_customers;

    IF @source_rows = 0
        THROW 51006, 'Customer dimension is empty. Initialization stopped.', 1;

    IF EXISTS (
        SELECT 1
        FROM dbt_dev.dim_customers
        WHERE customer_id IS NULL
           OR customer_key IS NULL
           OR customer_key <= 0
    )
        THROW 51007, 'Source contains NULL or invalid customer keys.', 1;

    IF EXISTS (
        SELECT customer_id
        FROM dbt_dev.dim_customers
        GROUP BY customer_id
        HAVING COUNT_BIG(*) > 1
    )
        THROW 51008, 'Source contains duplicate customer IDs.', 1;

    IF EXISTS (
        SELECT customer_key
        FROM dbt_dev.dim_customers
        GROUP BY customer_key
        HAVING COUNT_BIG(*) > 1
    )
        THROW 51009, 'Source contains duplicate customer surrogate keys.', 1;

    SET IDENTITY_INSERT dbt_dev.customer_key_map ON;
    SET @identity_insert_enabled = 1;

    INSERT INTO dbt_dev.customer_key_map (
        customer_key,
        customer_id
    )
    SELECT
        customer_key,
        customer_id
    FROM dbt_dev.dim_customers;

    SET @inserted_rows = @@ROWCOUNT;

    IF CONVERT(BIGINT, @inserted_rows) <> @source_rows
        THROW 51010, 'Inserted row count differs from source row count.', 1;

    SET IDENTITY_INSERT dbt_dev.customer_key_map OFF;
    SET @identity_insert_enabled = 0;

    COMMIT TRANSACTION;

    SELECT @inserted_rows AS initialized_customer_mappings;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;

    -- Setting IDENTITY_INSERT is session-scoped; a rollback does not
    -- guarantee that the setting is OFF, so explicitly clean it up.
    IF @identity_insert_enabled = 1
    BEGIN
        BEGIN TRY
            SET IDENTITY_INSERT dbt_dev.customer_key_map OFF;
        END TRY
        BEGIN CATCH
            PRINT 'Warning: check IDENTITY_INSERT state before reusing this session.';
        END CATCH;
    END;

    THROW;
END CATCH;
GO
