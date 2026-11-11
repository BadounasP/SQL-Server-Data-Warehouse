/*
===============================================================================
Script: 01_grant_airflow_dbt_permissions.sql
Purpose: Grant permissions to the Airflow/dbt service account.
===============================================================================
*/

USE DataWarehouse;
GO

-- Allow connections to the database.
GRANT CONNECT TO [airflow_dbt];

-- Read raw Bronze source data.
GRANT SELECT ON SCHEMA::bronze TO [airflow_dbt];

-- Read existing original Gold models for validation.
GRANT SELECT ON SCHEMA::gold TO [airflow_dbt];

-- Read and manage dbt development models.
GRANT SELECT ON SCHEMA::dbt_dev TO [airflow_dbt];

-- Allow dbt to create views and tables.
GRANT CREATE VIEW TO [airflow_dbt];
GRANT CREATE TABLE TO [airflow_dbt];

-- Allow dbt to manage objects in its development schema.
GRANT ALTER ON SCHEMA::dbt_dev TO [airflow_dbt];

-- Execute persistent surrogate-key synchronization.
GRANT EXECUTE
ON OBJECT::dbt_dev.sync_customer_key_map
TO [airflow_dbt];

GRANT EXECUTE
ON OBJECT::dbt_dev.sync_product_key_map
TO [airflow_dbt];
GO
