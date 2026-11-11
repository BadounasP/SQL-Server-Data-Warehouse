/*
===========================================================
Data Warehouse - Safe Database Initialization
===========================================================

Purpose:
    Create the DataWarehouse database and required schemas
    only if they do not already exist.

Safety:
    - Does not drop or recreate an existing database.
    - Does not delete existing tables or data.
    - Does not modify surrogate key mappings.
    - Does not delete pipeline audit history.

Run during initial environment setup only.
===========================================================
*/

USE master;
GO

-- Create the database only if it does not exist.
IF DB_ID(N'DataWarehouse') IS NULL
BEGIN
    EXEC(N'CREATE DATABASE [DataWarehouse]');
    PRINT 'DataWarehouse database created.';
END
ELSE
BEGIN
    PRINT 'DataWarehouse already exists. Preserving it.';
END;
GO

USE DataWarehouse;
GO

-- Bronze: raw source data.
IF SCHEMA_ID(N'bronze') IS NULL
    EXEC(N'CREATE SCHEMA [bronze] AUTHORIZATION [dbo]');
GO

-- Silver: retained for compatibility with older SQL scripts.
IF SCHEMA_ID(N'silver') IS NULL
    EXEC(N'CREATE SCHEMA [silver] AUTHORIZATION [dbo]');
GO

-- Gold: retained for compatibility with older SQL scripts.
IF SCHEMA_ID(N'gold') IS NULL
    EXEC(N'CREATE SCHEMA [gold] AUTHORIZATION [dbo]');
GO

-- dbt: current Silver and Gold models and key mappings.
IF SCHEMA_ID(N'dbt_dev') IS NULL
    EXEC(N'CREATE SCHEMA [dbt_dev] AUTHORIZATION [dbo]');
GO

-- Audit: persistent Airflow pipeline execution events.
IF SCHEMA_ID(N'audit') IS NULL
    EXEC(N'CREATE SCHEMA [audit] AUTHORIZATION [dbo]');
GO

PRINT 'Database initialization checks completed.';
GO
