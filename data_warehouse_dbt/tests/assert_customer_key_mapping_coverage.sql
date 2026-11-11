-- Test: Customer surrogate-key mapping coverage
--
-- Every cleaned CRM customer must have an
-- assigned persistent surrogate key.
--
-- The test passes when zero rows are returned.

SELECT
    c.cst_id AS customer_id

FROM {{ ref('stg_crm_customers') }} AS c

LEFT JOIN {{ source('surrogate_keys', 'customer_key_map') }} AS m
    ON c.cst_id = m.customer_id

WHERE
    m.customer_key IS NULL
