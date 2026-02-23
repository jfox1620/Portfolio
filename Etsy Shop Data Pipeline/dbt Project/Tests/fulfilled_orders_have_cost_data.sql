SELECT *
FROM {{ ref('fact_orders') }}
WHERE fulfillment_status in ('Paid','In Production','Shipped','In Transit','Delivered')
  AND production_cost_total IS NULL