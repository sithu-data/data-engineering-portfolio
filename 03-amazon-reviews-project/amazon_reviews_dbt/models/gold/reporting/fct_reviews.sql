with reviews as (
    select * from {{ ref('int_reviews_enriched') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_product as (
    select * from {{ ref('dim_product') }}
),

dim_store as (
    select * from {{ ref('dim_store') }}
),

dim_reviewer as (
    select * from {{ ref('dim_reviewer') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['r.asin', 'r.user_id', 'r.review_date', 'r.review_title', 'r.review_text']) }}
                                                as review_key,

        -- Foreign keys
        d.date_key                              as date_key,
        p.product_key                           as product_key,
        s.store_key                             as store_key,
        rv.reviewer_key                         as reviewer_key,

        -- Natural keys (keep for drilling)
        r.asin,
        r.parent_asin,
        r.user_id,

        -- Measures
        r.rating,
        r.cortex_sentiment_score,
        r.helpful_vote,
        r.review_text_length,

        -- Flags and labels
        r.verified_purchase,
        r.is_helpful,
        r.rating_sentiment,
        r.cortex_sentiment_label,

        -- Degenerate dimensions
        r.review_title,
        r.review_date

    from reviews r
    left join dim_date d
        on d.full_date = r.review_date::date
    left join dim_product p
        on p.parent_asin = r.parent_asin
    left join dim_store s
        on s.store = r.store
    left join dim_reviewer rv
        on rv.user_id = r.user_id
)

select * from final