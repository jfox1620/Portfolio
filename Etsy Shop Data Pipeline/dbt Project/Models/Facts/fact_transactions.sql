{{ config(
    schema='analytics',
    materialized='table'
) }}

SELECT
    transaction_id,
    receipt_id,
    product_id,
    quantity,
    price,
    paid_timestamp,
    shipped_timestamp
FROM {{ ref('transactions') }}
WHERE transaction_id is not null