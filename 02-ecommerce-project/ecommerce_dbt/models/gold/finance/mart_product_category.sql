with product_revenue as (
    select * from {{ ref('int_product_revenue') }}
),
reviews as (
    select
        order_id,
        rating
    from {{ ref('stg_order_reviews') }}
    qualify row_number() over (
        partition by order_id
        order by review_created_at desc
    ) = 1
),
category_metrics as (
    select
        p.category_name,
        date_trunc('month', p.purchased_at)         as order_month,
        count(distinct p.order_id)                  as total_orders,
        count(distinct p.product_id)                as unique_products,
        sum(p.item_price)                           as total_revenue,
        avg(p.item_price)                           as avg_item_price,
        sum(p.freight_value)                        as total_freight,
        avg(p.freight_value)                        as avg_freight,
        avg(p.weight_grams)                         as avg_weight_grams,
        avg(r.rating)                               as avg_review_score,
        sum(p.item_price) / nullif(
            sum(sum(p.item_price)) over (
                partition by date_trunc('month', p.purchased_at)
            ), 0
        ) * 100                                     as revenue_share_pct
    from product_revenue p
    left join reviews r using (order_id)
    group by 1, 2
)
select * from category_metrics
order by order_month, total_revenue desc