with orders as (
    select * from {{ ref('int_customer_orders') }}
    where order_status = 'delivered'
        and delivery_days is not null
),
daily_ops as (
    select
        date_trunc('month', purchased_at)           as order_month,
        customer_state,
        count(distinct order_id)                    as total_orders,
        avg(delivery_days)                          as avg_delivery_days,
        min(delivery_days)                          as min_delivery_days,
        max(delivery_days)                          as max_delivery_days,
        percentile_cont(0.5) within group (
            order by delivery_days
        )                                           as median_delivery_days,
        percentile_cont(0.95) within group (
            order by delivery_days
        )                                           as p95_delivery_days,
        sum(case when delivered_on_time
            then 1 else 0 end)                      as on_time_count,
        sum(case when not delivered_on_time
            then 1 else 0 end)                      as late_count,
        round(
            sum(case when delivered_on_time
                then 1 else 0 end)
            / count(*) * 100
        , 2)                                        as on_time_rate,
        avg(order_freight)                          as avg_freight_cost,
        avg(review_score)                           as avg_review_score,
        -- late orders tend to get worse reviews
        avg(case when not delivered_on_time
            then review_score end)                  as avg_review_score_late,
        avg(case when delivered_on_time
            then review_score end)                  as avg_review_score_on_time
    from orders
    group by 1, 2
)
select * from daily_ops
order by order_month, total_orders desc