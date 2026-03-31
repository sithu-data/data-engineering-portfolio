with orders as (
    select * from {{ ref('int_orders_enriched') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
items as (
    select
        order_id,
        sum(item_price)                 as order_revenue,
        sum(freight_value)              as order_freight,
        sum(item_price + freight_value) as order_total_value,
        count(*)                        as item_count
    from {{ ref('stg_order_items') }}
    group by 1
),
payments as (
    select
        order_id,
        sum(payment_amount)             as total_paid,
        count(distinct payment_type)    as payment_methods_used,
        max(installments)               as max_installments
    from {{ ref('stg_order_payments') }}
    group by 1
),
reviews as (
    select
        order_id,
        rating,
        review_title,
        review_body
    from {{ ref('stg_order_reviews') }}
    qualify row_number() over (
        partition by order_id
        order by review_created_at desc
    ) = 1
),
joined as (
    select
        o.order_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.delivery_days,
        o.delivered_on_time,
        c.customer_unique_id,
        c.city                          as customer_city,
        c.state                         as customer_state,
        i.order_revenue,
        i.order_freight,
        i.order_total_value,
        coalesce(i.item_count, 0)       as item_count,
        p.total_paid,
        p.payment_methods_used,
        p.max_installments,
        r.rating                        as review_score,
        r.review_title,
        r.review_body
    from orders o
    left join customers c   using (customer_id)
    left join items i       using (order_id)
    left join payments p    using (order_id)
    left join reviews r     using (order_id)
)
select * from joined