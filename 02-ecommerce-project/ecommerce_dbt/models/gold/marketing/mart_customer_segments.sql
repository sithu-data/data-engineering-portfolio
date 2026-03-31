with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
    where order_status = 'delivered'
),
rfm_base as (
    select
        customer_unique_id,
        customer_city,
        customer_state,
        max(purchased_at)                                   as last_order_date,
        count(distinct order_id)                            as frequency,
        sum(order_revenue)                                  as monetary,
        avg(order_revenue)                                  as avg_order_value,
        avg(review_score)                                   as avg_review_score,
        sum(item_count)                                     as total_items_bought,
        datediff('day', max(purchased_at), current_date())  as recency
    from customer_orders
    group by 1, 2, 3
),
rfm_scored as (
    select *,
        ntile(5) over (order by recency asc)    as r_score,
        ntile(5) over (order by frequency desc) as f_score,
        ntile(5) over (order by monetary desc)  as m_score
    from rfm_base
),
segmented as (
    select *,
        r_score + f_score + m_score             as rfm_total,
        case
            when r_score >= 4 and f_score >= 4 and m_score >= 4
                then 'Champions'
            when r_score >= 3 and f_score >= 3
                then 'Loyal Customers'
            when r_score >= 4 and f_score <= 2
                then 'Recent Customers'
            when r_score >= 3 and f_score <= 3 and m_score >= 3
                then 'Potential Loyalists'
            when r_score <= 2 and f_score >= 3
                then 'At Risk'
            when r_score <= 2 and f_score >= 4
                then 'Cannot Lose Them'
            when r_score <= 1 and f_score <= 1
                then 'Lost'
            else 'Needs Attention'
        end                                     as customer_segment
    from rfm_scored
)
-- Note: ntile() can assign duplicate scores to customers with identical
-- recency/frequency/monetary values. We dedup here keeping the highest
-- rfm_total score per customer to guarantee one row per customer_unique_id.
select * from segmented
qualify row_number() over (
    partition by customer_unique_id
    order by rfm_total desc
) = 1