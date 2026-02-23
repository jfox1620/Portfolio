SELECT *
FROM {{ ref('fact_orders') }}
WHERE (is_shipped = TRUE
  AND shipped_timestamp IS NULL)
  OR (is_shipped = False
  AND shipped_timestamp IS NOT NULL)