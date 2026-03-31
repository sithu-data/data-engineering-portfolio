with orders as (
    select * from {{ ref('int_customer_orders') }}
    where order_status = 'delivered'
),
daily as (
    select
        date_trunc('day', purchased_at)         as order_date,
        date_trunc('week', purchased_at)        as order_week,
        date_trunc('month', purchased_at)       as order_month,
        count(distinct order_id)                as total_orders,
        count(distinct customer_unique_id)      as unique_customers,
        sum(order_revenue)                      as total_revenue,
        avg(order_revenue)                      as avg_order_value,
        sum(order_freight)                      as total_freight,
        avg(delivery_days)                      as avg_delivery_days,
        sum(case when delivered_on_time then 1 else 0 end) as on_time_deliveries,
        round(
            sum(case when delivered_on_time then 1 else 0 end)
            / count(*) * 100
        , 2)                                    as on_time_rate,
        avg(review_score)                       as avg_review_score
    from orders
    group by 1, 2, 3
)
select * from daily
order by order_date