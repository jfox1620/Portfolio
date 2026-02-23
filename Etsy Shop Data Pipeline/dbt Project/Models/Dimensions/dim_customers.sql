{{ config(
    schema='analytics',
    materialized='table'
) }}

SELECT
    r.buyer_user_id                                   AS customer_id,
    MAX(r.customer_name)                              AS name,
    MAX(r.customer_email)                             AS email,
    MAX(r.city)                                       AS city,    
    COALESCE(MAX(r.country_iso),MAX(f.country))       AS country,
    MIN(r.created_timestamp)                          AS first_order_timestamp,
    MAX(r.created_timestamp)                          AS last_order_timestamp
FROM {{ ref('receipts') }} r
LEFT JOIN {{ ref('fulfillments') }} f on f.buyer_user_id = r.buyer_user_id
WHERE r.buyer_user_id IS NOT NULL
GROUP BY r.buyer_user_id