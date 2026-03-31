-- ML-ready feature table for churn prediction
with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
    where order_status = 'delivered'
),
features as (
    select
        customer_unique_id,
        count(distinct order_id)                            as total_orders,
        sum(order_revenue)                                  as total_revenue,
        avg(order_revenue)                                  as avg_order_value,
        min(order_revenue)                                  as min_order_value,
        max(order_revenue)                                  as max_order_value,
        avg(item_count)                                     as avg_items_per_order,
        avg(review_score)                                   as avg_review_score,
        min(review_score)                                   as min_review_score,
        avg(delivery_days)                                  as avg_delivery_days,
        sum(case when delivered_on_time then 1 else 0 end)  as on_time_deliveries,
        count(distinct customer_state)                      as states_ordered_from,
        count(distinct payment_methods_used)                as distinct_payment_methods,
        avg(max_installments)                               as avg_installments,
        datediff('day',
            min(purchased_at),
            max(purchased_at))                              as customer_lifespan_days,
        datediff('day',
            max(purchased_at),
            current_date())                                 as days_since_last_order,
        case
            when datediff('day',
                max(purchased_at),
                current_date()) > 180
            then 1 else 0
        end                                                 as is_churned
    from customer_orders
    group by 1
)
select * from features