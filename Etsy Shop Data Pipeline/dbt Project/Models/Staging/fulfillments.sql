{{ config(
    schema='analytics',
    materialized='view'
) }}

SELECT
    fulfillment.value:id::STRING AS fulfillment_id,
    fulfillment.value:orderReferenceId::NUMBER AS receipt_id,
    fulfillment.value:customerReferenceId::NUMBER AS buyer_user_id,
    fulfillment.value:country::STRING AS country,
    TO_TIMESTAMP(fulfillment.value:orderedAt::TIMESTAMP_TZ) AS ordered_at,
    fulfillment.value:financialStatus::STRING AS financial_status,
    fulfillment.value:fulfillmentStatus::STRING AS fulfillment_status,
    fulfillment.value:delayDays::NUMBER AS delay_days,
    fulfillment.value:subtotal::FLOAT AS subtotal,
    fulfillment.value:shipping::FLOAT AS shipping_cost,
    fulfillment.value:tax::FLOAT AS tax,
    fulfillment.value:totalInclVat::FLOAT AS grand_total,
    TO_TIMESTAMP(fulfillment.value:createdAt::TIMESTAMP_TZ) AS created_at,
    TO_TIMESTAMP(fulfillment.value:updatedAt::TIMESTAMP_TZ) AS updated_at,
    f.ingested_at,
    f.source_file
FROM true_form_designs.raw_ingest.gelato_fulfillments f,
    LATERAL FLATTEN(input => f.raw_json:data) fulfillment
WHERE fulfillment.value:channel::STRING = 'etsy'