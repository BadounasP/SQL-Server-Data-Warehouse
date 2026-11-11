-- ERP Product Categories
-- Preserve the original Bronze-to-Silver mapping.
-- No cleaning or filtering is applied at this stage.

{{ config(materialized='view') }}


SELECT
    id,
    cat,
    subcat,
    maintenance

FROM {{ source('erp', 'erp_px_cat_g1v2') }}
