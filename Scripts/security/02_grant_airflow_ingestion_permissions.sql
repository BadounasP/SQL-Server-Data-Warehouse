/*
    Purpose:
    Grant the dedicated Airflow ingestion account
    permissions to refresh the Bronze layer.

    Run as an authorized SQL Server administrator.
*/

USE master;
GO

-- Required for SQL Server BULK INSERT operations.
GRANT ADMINISTER BULK OPERATIONS TO [airflow_ingest];
GO

USE DataWarehouse;
GO

GRANT CONNECT TO [airflow_ingest];

-- Permit execution of the existing ingestion procedure.
GRANT EXECUTE
ON OBJECT::bronze.load_bronze
TO [airflow_ingest];

-- Permit read-only validation of Bronze data.
GRANT SELECT ON SCHEMA::bronze TO [airflow_ingest];

-- INSERT is required for bulk loading.
-- ALTER is needed by the procedure's TRUNCATE TABLE operations.

GRANT INSERT, ALTER
ON OBJECT::bronze.crm_cust_info TO [airflow_ingest];

GRANT INSERT, ALTER
ON OBJECT::bronze.crm_prd_info TO [airflow_ingest];

GRANT INSERT, ALTER
ON OBJECT::bronze.crm_sales_details TO [airflow_ingest];

GRANT INSERT, ALTER
ON OBJECT::bronze.erp_cust_az12 TO [airflow_ingest];

GRANT INSERT, ALTER
ON OBJECT::bronze.erp_loc_a101 TO [airflow_ingest];

GRANT INSERT, ALTER
ON OBJECT::bronze.erp_px_cat_g1v2 TO [airflow_ingest];
GO
