with items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select * from {{ ref('int_customer_orders') }}
    where order_status = 'delivered'
),
sellers as (
    select * from {{ ref('stg_sellers') }}
),
seller_metrics as (
    select
        i.seller_id,
        s.city                              as seller_city,
        s.state                             as seller_state,
        count(distinct i.order_id)          as total_orders,
        count(distinct i.product_id)        as unique_products,
        sum(i.item_price)                   as total_revenue,
        avg(i.item_price)                   as avg_item_price,
        sum(i.freight_value)                as total_freight_charged,
        avg(o.delivery_days)                as avg_delivery_days,
        round(
            sum(case when o.delivered_on_time then 1 else 0 end)
            / count(*) * 100
        , 2)                                as on_time_rate,
        avg(o.review_score)                 as avg_review_score,
        min(o.purchased_at)                 as first_sale_date,
        max(o.purchased_at)                 as last_sale_date
    from items i
    left join orders o  using (order_id)
    left join sellers s using (seller_id)
    group by 1, 2, 3
)
select * from seller_metrics