/*
===============================================================================
Script: 03_validate_customer_key_map.sql
Purpose: Validate customer surrogate key mapping consistency.
===============================================================================

Description:
    Compares dbt_dev.dim_customers with dbt_dev.customer_key_map
    to confirm that customer identifiers and surrogate keys match.

    The validation checks:
        1. Number of records in the customer dimension.
        2. Number of records in the customer key mapping table.
        3. Missing mappings or mismatched surrogate keys.

Execution:
    - Run after initializing customer_key_map.
    - Can be rerun whenever mapping consistency needs verification.
    - Read-only: does not insert, update, or delete any records.

Expected Result (Initial Migration):
    dimension_rows : 18,484
    mapping_rows   : 18,484
    key_mismatches : 0

Important:
    The current validation compares the mapping with the existing
    dimension. After switching the dimension to persistent keys,
    additional independent validation will be needed to detect
    unintended key reassignment across pipeline executions.

===============================================================================
*/

USE DataWarehouse;
GO

SELECT

    (SELECT COUNT(*)
     FROM dbt_dev.dim_customers)
        AS dimension_rows,

    (SELECT COUNT(*)
     FROM dbt_dev.customer_key_map)
        AS mapping_rows,

    (SELECT COUNT(*)
     FROM dbt_dev.dim_customers d
     LEFT JOIN dbt_dev.customer_key_map m
        ON d.customer_id = m.customer_id
     WHERE m.customer_key IS NULL
        OR d.customer_key <> m.customer_key)
        AS key_mismatches;
