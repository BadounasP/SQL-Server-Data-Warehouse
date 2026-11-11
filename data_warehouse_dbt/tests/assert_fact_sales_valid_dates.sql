-- Reject inconsistent transaction date sequences.

SELECT
    order_number,
    order_date,
    shipping_date,
    due_date

FROM {{ ref('fact_sales') }}

WHERE
    (
        order_date IS NOT NULL
        AND shipping_date IS NOT NULL
        AND shipping_date < order_date
    )
    OR
    (
        order_date IS NOT NULL
        AND due_date IS NOT NULL
        AND due_date < order_date
    )
    OR
    (
        shipping_date IS NOT NULL
        AND due_date IS NOT NULL
        AND due_date < shipping_date
    )
