/*
===============================================================================
Script: proc_load_bronze.sql
Procedure: bronze.load_bronze
Purpose: Load CRM and ERP source files into the Bronze layer.
===============================================================================

Description:
    Performs a transactional full refresh of six Bronze tables.

    - Removes previously loaded rows.
    - Imports source CSV files using BULK INSERT.
    - Rejects unexpectedly empty source tables.
    - Rolls back the entire load if any stage fails.
    - Propagates errors to the calling orchestration tool.

Execution:
    Deploy the procedure first using CREATE OR ALTER.

    Execute separately using:
        EXEC bronze.load_bronze;

Important:
    This procedure replaces Bronze data on each successful run.
    Source CSV files must be accessible to SQL Server.
    Update the local file paths when moving environments.
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE [bronze].[load_bronze]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @batch_start DATETIME2(3) = SYSDATETIME();

    BEGIN TRY

        -- All six source loads belong to one transaction.
        BEGIN TRANSACTION;

        PRINT '========================================';
        PRINT 'Starting Bronze Layer Load';
        PRINT '========================================';

        -- =========================================================
        -- CRM: Customers
        -- =========================================================

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\DataWarehouse\ingest\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        IF NOT EXISTS (
            SELECT 1 FROM bronze.crm_cust_info
        )
            THROW 51001, 'CRM customers loaded zero rows.', 1;

        PRINT 'CRM customers loaded successfully.';

        -- =========================================================
        -- CRM: Products
        -- =========================================================

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\DataWarehouse\ingest\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        IF NOT EXISTS (
            SELECT 1 FROM bronze.crm_prd_info
        )
            THROW 51002, 'CRM products loaded zero rows.', 1;

        PRINT 'CRM products loaded successfully.';

        -- =========================================================
        -- CRM: Sales
        -- =========================================================

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\DataWarehouse\ingest\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        IF NOT EXISTS (
            SELECT 1 FROM bronze.crm_sales_details
        )
            THROW 51003, 'CRM sales loaded zero rows.', 1;

        PRINT 'CRM sales loaded successfully.';

        -- =========================================================
        -- ERP: Locations
        -- =========================================================

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\DataWarehouse\ingest\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        IF NOT EXISTS (
            SELECT 1 FROM bronze.erp_loc_a101
        )
            THROW 51004, 'ERP locations loaded zero rows.', 1;

        PRINT 'ERP locations loaded successfully.';

        -- =========================================================
        -- ERP: Customers
        -- =========================================================

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\DataWarehouse\ingest\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        IF NOT EXISTS (
            SELECT 1 FROM bronze.erp_cust_az12
        )
            THROW 51005, 'ERP customers loaded zero rows.', 1;

        PRINT 'ERP customers loaded successfully.';

        -- =========================================================
        -- ERP: Product Categories
        -- =========================================================

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\DataWarehouse\ingest\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        IF NOT EXISTS (
            SELECT 1 FROM bronze.erp_px_cat_g1v2
        )
            THROW 51006, 'ERP categories loaded zero rows.', 1;

        PRINT 'ERP product categories loaded successfully.';

        -- Commit only after ALL six loads succeed.
        COMMIT TRANSACTION;

        PRINT '========================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT '========================================';

        SELECT
            DATEDIFF(
                MILLISECOND,
                @batch_start,
                SYSDATETIME()
            ) AS duration_ms;

    END TRY

    BEGIN CATCH

        -- Restore the previous Bronze data on failure.
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        PRINT '========================================';
        PRINT 'BRONZE LAYER LOAD FAILED';
        PRINT 'Transaction rolled back.';
        PRINT '========================================';

        -- Report the failure to the orchestration tool.
        THROW;

    END CATCH;
END;
GO
