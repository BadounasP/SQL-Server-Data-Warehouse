/*
===============================================================================
Script: 02_grant_audit_permissions.sql
Purpose: Grant least-privilege audit access to Apache Airflow.

The airflow_audit account can insert pipeline events
but does not receive general warehouse write access.
===============================================================================
*/

USE DataWarehouse;
GO

-- Create the database user for the existing SQL login.
IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = N'airflow_audit'
)
BEGIN
    CREATE USER [airflow_audit]
    FOR LOGIN [airflow_audit];
END;
GO

-- Allow access to the DataWarehouse database.
GRANT CONNECT TO [airflow_audit];

-- Allow inserting new audit events only.
GRANT INSERT
ON OBJECT::audit.pipeline_run_events
TO [airflow_audit];
GO
