/*
===============================================================================
Script: 01_create_customer_key_map.sql
Purpose: Create a persistent customer surrogate key mapping table.
===============================================================================

Description:
    Creates the dbt_dev.customer_key_map table to maintain a permanent
    relationship between CRM customer identifiers and warehouse surrogate keys.

    The table uses SQL Server IDENTITY to generate surrogate keys and
    PRIMARY KEY / UNIQUE constraints to prevent duplicate assignments.

    This addresses the limitations of ROW_NUMBER(), where inserting new
    customers can change previously assigned surrogate keys.

Execution:
    - Run once during the initial warehouse setup.
    - Requires the DataWarehouse database and dbt_dev schema.
    - Must execute before 02_initialize_customer_key_map.sql.
    - Do not drop or recreate the table during regular pipeline runs.

Expected Result:
    An empty customer_key_map table with:
        customer_key : BIGINT IDENTITY, PRIMARY KEY
        customer_id  : INT, UNIQUE, NOT NULL

===============================================================================
*/

USE DataWarehouse;
GO

-- Safety check: never overwrite an existing mapping.
IF OBJECT_ID(
    N'dbt_dev.customer_key_map', N'U'
) IS NOT NULL
BEGIN
    THROW 50001,
        'Customer key mapping already exists. Stop.',
        1;
END;
GO

CREATE TABLE dbt_dev.customer_key_map (

    customer_key BIGINT IDENTITY(1,1) NOT NULL
        CONSTRAINT PK_customer_key_map
        PRIMARY KEY,

    customer_id INT NOT NULL
        CONSTRAINT UQ_customer_key_map_customer_id
        UNIQUE

);
GO
