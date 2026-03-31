with orders as (
    select * from {{ ref('int_customer_orders') }}
    where order_status = 'delivered'
),
first_orders as (
    select
        customer_unique_id,
        date_trunc('month', min(purchased_at))  as cohort_month
    from orders
    group by 1
),
order_months as (
    select
        o.customer_unique_id,
        f.cohort_month,
        date_trunc('month', o.purchased_at)     as order_month,
        datediff(
            'month',
            f.cohort_month,
            date_trunc('month', o.purchased_at)
        )                                       as period_number
    from orders o
    join first_orders f using (customer_unique_id)
),
cohort_size as (
    select
        cohort_month,
        count(distinct customer_unique_id)      as cohort_customers
    from first_orders
    group by 1
),
retention as (
    select
        cohort_month,
        period_number,
        count(distinct customer_unique_id)      as retained_customers
    from order_months
    group by 1, 2
)
select
    r.cohort_month,
    r.period_number,
    cs.cohort_customers,
    r.retained_customers,
    round(
        r.retained_customers / cs.cohort_customers * 100
    , 2)                                        as retention_rate
from retention r
join cohort_size cs using (cohort_month)
order by 1, 2