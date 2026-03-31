with source as (
    select * from {{ source('raw', 'olist_order_items') }}
),
staged as (
    select
        order_id,
        order_item_id                   as item_sequence,
        product_id,
        seller_id,
        shipping_limit_date::timestamp  as shipping_limit_at,
        price                           as item_price,
        freight_value                   as freight_value
    from source
)
select * from staged