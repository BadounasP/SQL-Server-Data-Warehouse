/*
===============================================================================
Bronze Layer Validation
Purpose: Verify row counts after running bronze.load_bronze.
This query is read-only and does not modify any data.
===============================================================================
*/

USE DataWarehouse;
GO

SELECT
    'crm_cust_info' AS table_name,
    COUNT_BIG(*) AS row_count
FROM bronze.crm_cust_info

UNION ALL

SELECT
    'crm_prd_info',
    COUNT_BIG(*)
FROM bronze.crm_prd_info

UNION ALL

SELECT
    'crm_sales_details',
    COUNT_BIG(*)
FROM bronze.crm_sales_details

UNION ALL

SELECT
    'erp_cust_az12',
    COUNT_BIG(*)
FROM bronze.erp_cust_az12

UNION ALL

SELECT
    'erp_loc_a101',
    COUNT_BIG(*)
FROM bronze.erp_loc_a101

UNION ALL

SELECT
    'erp_px_cat_g1v2',
    COUNT_BIG(*)
FROM bronze.erp_px_cat_g1v2;
