{{ config(
    schema='analytics',
    materialized='table'
) }}

SELECT DISTINCT
    product_id,
    sku,
    listing_id,
    listing_name,
    listing_type    AS product_type,
    size,
    thickness,
    frame,
    is_digital
FROM {{ ref('transactions') }}
WHERE product_id IS NOT NULL