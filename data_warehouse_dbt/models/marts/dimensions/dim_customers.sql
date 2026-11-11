{{ config(materialized='view') }}

-- Gold Customer Dimension
-- Uses persistent surrogate keys from SQL Server.
-- Integrates cleaned CRM and ERP customer information.

SELECT
    m.customer_key AS customer_key,

    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,

    la.cntry AS country,

    ci.cst_marital_status AS marital_status,

    CASE
        WHEN ci.cst_gndr != 'n/a'
            THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date

FROM {{ ref('stg_crm_customers') }} ci

-- Retrieve the customer's permanent surrogate key.
LEFT JOIN {{ source('surrogate_keys', 'customer_key_map') }} m
    ON ci.cst_id = m.customer_id

-- Enrich CRM customers with ERP demographics.
LEFT JOIN {{ ref('stg_erp_customers') }} ca
    ON ci.cst_key = ca.cid

-- Enrich CRM customers with ERP locations.
LEFT JOIN {{ ref('stg_erp_locations') }} la
    ON ci.cst_key = la.cid
