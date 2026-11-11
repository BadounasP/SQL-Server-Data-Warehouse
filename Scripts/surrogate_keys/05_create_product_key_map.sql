/*
===============================================================================
Script: 05_create_product_key_map.sql
Purpose: Create a persistent product surrogate-key mapping table.
===============================================================================

Description:
    Maintains permanent mappings between business product numbers
    and warehouse surrogate keys.

    Uses IDENTITY for new product keys and UNIQUE constraints
    to prevent duplicate product-number assignments.

Execution:
    - Run once during initial setup.
    - Do not drop or recreate during normal pipeline runs.
===============================================================================
*/

USE DataWarehouse;
GO

-- Prevent accidental recreation of the mapping.
IF OBJECT_ID(N'dbt_dev.product_key_map') IS NOT NULL
BEGIN
    THROW 50002,
        'Product key mapping already exists. Stop.',
        1;
END;

CREATE TABLE dbt_dev.product_key_map (

    product_key BIGINT IDENTITY(1,1) NOT NULL
        CONSTRAINT PK_product_key_map
        PRIMARY KEY,

    product_number NVARCHAR(50) NOT NULL
        CONSTRAINT UQ_product_key_map_product_number
        UNIQUE

);
GO
