{{ config(
    schema='analytics',
    materialized='view'
) }}

SELECT
    transaction.value:transaction_id::NUMBER as transaction_id,
    transaction.value:receipt_id::NUMBER as receipt_id,
    transaction.value:product_id::NUMBER as product_id,
    transaction.value:listing_id::NUMBER as listing_id,
    TO_TIMESTAMP(transaction.value:created_timestamp::TIMESTAMP_TZ) AS created_timestamp,
    transaction.value:sku::STRING as sku,
    TRIM(SPLIT_PART(transaction.value:title::STRING, '|', 1)) AS listing_name,
    INITCAP(TRIM(SPLIT_PART(transaction.value:title::STRING, '|', 2))) AS listing_type,
        MAX(IFF(variation.value:formatted_name::STRING = 'Size', variation.value:formatted_value::STRING, NULL)) AS size,
    MAX(IFF(variation.value:formatted_name::STRING = 'Thickness', variation.value:formatted_value::STRING, NULL)) AS thickness,
    MAX(IFF(variation.value:formatted_name::STRING LIKE '%Frame%', variation.value:formatted_value::STRING, NULL)) AS frame,
    transaction.value:is_digital::BOOLEAN as is_digital,
    transaction.value:quantity::NUMBER as quantity,
    transaction.value:price:amount::FLOAT
        / NULLIF(transaction.value:price:divisor::FLOAT,0) AS price,
    TO_TIMESTAMP(transaction.value:paid_timestamp::TIMESTAMP_TZ) AS paid_timestamp,
    TO_TIMESTAMP(transaction.value:shipped_timestamp::TIMESTAMP_TZ) AS shipped_timestamp,
    t.ingested_at,
    t.source_file
FROM true_form_designs.raw_ingest.etsy_transactions t,
    LATERAL FLATTEN(input => t.raw_json:data) transaction,
    LATERAL FLATTEN(input => transaction.value:variations) variation
GROUP BY 1,2,3,4,5,6,7,8,12,13,14,15,16,17,18