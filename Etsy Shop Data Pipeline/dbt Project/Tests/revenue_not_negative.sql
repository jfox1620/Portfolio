SELECT *
FROM {{ ref('fact_orders') }}
WHERE grandtotal < 0