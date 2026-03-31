with orders as (
    select * from {{ ref('stg_orders') }}
),
enriched as (
    select
        *,
        datediff('day', purchased_at, delivered_at)     as delivery_days,
        case
            when delivered_at <= estimated_delivery_at
            then true else false
        end                                             as delivered_on_time,
        case
            when order_status = 'delivered'             then 5
            when order_status = 'shipped'               then 4
            when order_status = 'approved'              then 3
            when order_status = 'processing'            then 2
            when order_status = 'canceled'              then 0
            else 1
        end                                             as order_status_rank
    from orders
)
select * from enriched