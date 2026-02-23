{{ config(
    schema='analytics',
    materialized='table'
) }}

SELECT
    r.receipt_id,
    r.status,
    r.buyer_user_id                             AS customer_id,
    r.total_price,
    r.discount_amt                              AS discount_amount,
    r.subtotal,
    r.total_tax_cost                            AS tax_cost,
    r.grandtotal,
    r.is_paid,
    r.is_shipped,
    r.created_timestamp                         AS ordered_timestamp,
    r.refund_amount,
    r.refund_reason,
    r.refund_timestamp,
    f.fulfillment_status,
    f.subtotal                                  AS production_subtotal,
    f.shipping_cost                             AS production_shipping_cost,
    f.tax                                       AS production_tax,
    f.grand_total                               AS production_cost_total,
    f.updated_at                                AS shipped_timestamp,
    (r.subtotal - COALESCE(f.grand_total,0))    AS estimated_profit_excl_fees
FROM {{ ref('receipts') }} r
LEFT JOIN {{ ref('fulfillments') }} f on r.receipt_id = f.receipt_id
WHERE r.receipt_id is not null