{{ config(
    schema='analytics',
    materialized='view'
) }}

SELECT
    receipt.value:receipt_id::NUMBER AS receipt_id,
    receipt.value:status::STRING AS status,
    receipt.value:buyer_user_id::NUMBER AS buyer_user_id,
    receipt.value:name::STRING AS customer_name,
    receipt.value:buyer_email::STRING AS customer_email,
    receipt.value:city::STRING AS city,
    receipt.value:country_iso::STRING AS country_iso,
    receipt.value:total_price:amount::FLOAT
        / NULLIF(receipt.value:total_price:divisor::FLOAT,0) AS total_price,
    receipt.value:discount_amt:amount::FLOAT
        / NULLIF(receipt.value:discount_amt:divisor::FLOAT,0) AS discount_amt,
    receipt.value:subtotal:amount::FLOAT
        / NULLIF(receipt.value:subtotal:divisor::FLOAT,0) AS subtotal,
    receipt.value:total_shipping_cost:amount::FLOAT
        / NULLIF(receipt.value:total_shipping_cost:divisor::FLOAT,0) AS total_shipping_cost,
    receipt.value:total_tax_cost:amount::FLOAT
        / NULLIF(receipt.value:total_tax_cost:divisor::FLOAT,0) AS total_tax_cost,
    receipt.value:total_vat_cost:amount::FLOAT
        / NULLIF(receipt.value:total_vat_cost:divisor::FLOAT,0) AS total_vat_cost,
    receipt.value:grandtotal:amount::FLOAT
        / NULLIF(receipt.value:grandtotal:divisor::FLOAT,0) AS grandtotal,
    receipt.value:is_paid::BOOLEAN AS is_paid,
    receipt.value:is_shipped::BOOLEAN AS is_shipped,
    refund.value:amount:amount::FLOAT
        / NULLIF(refund.value:amount:divisor::FLOAT,0) AS refund_amount,
    refund.value:reason::STRING AS refund_reason,
    TO_TIMESTAMP(refund.value:created_timestamp::TIMESTAMP_TZ) AS refund_timestamp,
    TO_TIMESTAMP(receipt.value:created_timestamp::TIMESTAMP_TZ) AS created_timestamp,
    TO_TIMESTAMP(receipt.value:updated_timestamp::TIMESTAMP_TZ) AS updated_timestamp,
    r.ingested_at,
    r.source_file
FROM true_form_designs.raw_ingest.etsy_receipts r,
LATERAL FLATTEN(input => r.raw_json:data) receipt, 
LATERAL FLATTEN(
          input => receipt.value:refunds,
          OUTER => TRUE
      ) refund


