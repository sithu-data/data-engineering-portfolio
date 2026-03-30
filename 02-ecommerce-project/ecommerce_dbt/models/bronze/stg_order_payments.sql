with source as (
    select * from {{ source('raw', 'olist_order_payments') }}
),
staged as (
    select
        order_id,
        payment_sequential        as payment_sequence,
        payment_type,
        payment_installments      as installments,
        payment_value             as payment_amount
    from source
)
select * from staged