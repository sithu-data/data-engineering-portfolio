with items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select order_id, order_status, purchased_at
    from {{ ref('int_orders_enriched') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
categories as (
    select * from {{ ref('stg_product_categories') }}
),
joined as (
    select
        p.product_id,
        coalesce(c.category_name_english,
                 p.category_name_portuguese,
                 'Unknown')         as category_name,
        p.weight_grams,
        p.photo_count,
        i.order_id,
        o.order_status,
        o.purchased_at,
        i.item_price,
        i.freight_value,
        i.item_price + i.freight_value  as total_item_value
    from items i
    left join orders o      using (order_id)
    left join products p    using (product_id)
    left join categories c  using (category_name_portuguese)
    where o.order_status = 'delivered'
)
select * from joined