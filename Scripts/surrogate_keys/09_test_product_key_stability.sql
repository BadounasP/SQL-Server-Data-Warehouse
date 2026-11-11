/*
===============================================================================
Script: 09_test_product_key_stability.sql
Purpose: Demonstrate stable product surrogate keys.
===============================================================================

Description:
    Creates a temporary copy of the product key mapping,
    simulates the insertion of a new product, and checks
    that existing surrogate-key assignments remain unchanged.

    Uses a local temporary table (#product_key_test).
    Does not modify the real product_key_map or Bronze data.

Expected Results:
    Original mappings: 295
    Simulated mappings: 296
    Missing original mappings: 0
    New product receives a new surrogate key.

===============================================================================
*/

USE DataWarehouse;
GO

-- Create an isolated temporary mapping.
CREATE TABLE #product_key_test (

    product_key BIGINT IDENTITY(1,1) NOT NULL
        PRIMARY KEY,

    product_number NVARCHAR(50) NOT NULL
        UNIQUE

);

-- Preserve all existing product key assignments.
SET IDENTITY_INSERT #product_key_test ON;

INSERT INTO #product_key_test (
    product_key,
    product_number
)
SELECT
    product_key,
    product_number
FROM dbt_dev.product_key_map;

SET IDENTITY_INSERT #product_key_test OFF;

-- Simulate a completely new product.
DECLARE @new_product NVARCHAR(50);

SET @new_product =
    N'TEST_' + CONVERT(NVARCHAR(36), NEWID());

INSERT INTO #product_key_test (
    product_number
)
VALUES (
    @new_product
);

-- Verify that existing mappings remain unchanged.
SELECT

    (SELECT COUNT(*)
     FROM dbt_dev.product_key_map)
        AS original_mapping_rows,

    (SELECT COUNT(*)
     FROM #product_key_test)
        AS simulated_mapping_rows,

    (SELECT product_key
     FROM #product_key_test
     WHERE product_number = @new_product)
        AS new_product_key,

    (SELECT COUNT(*)
     FROM (
         SELECT product_key, product_number
         FROM dbt_dev.product_key_map

         EXCEPT

         SELECT product_key, product_number
         FROM #product_key_test
     ) AS missing)
        AS original_mappings_changed;

-- Remove the temporary test table.
DROP TABLE #product_key_test;
GO
