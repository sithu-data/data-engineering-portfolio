with source as (
    select * from {{ ref('int_reviews_enriched') }}
),

aggregated as (
    select
        parent_asin,
        product_title,
        store,
        main_category,
        price_tier,
        rating,
        cortex_sentiment_label,
        verified_purchase,

        count(*)                                        as total_reviews,
        sum(helpful_vote)                               as total_helpful_votes,
        avg(helpful_vote)                               as avg_helpful_votes,
        avg(review_text_length)                         as avg_review_length,

        -- Helpfulness rate
        round(
            sum(case when is_helpful then 1 else 0 end)
            / nullif(count(*), 0) * 100, 2
        )                                               as helpfulness_rate,

        -- Long reviews tend to be more helpful
        avg(case when review_text_length > 200
            then helpful_vote end)                      as avg_helpful_votes_long_reviews,

        avg(case when review_text_length <= 200
            then helpful_vote end)                      as avg_helpful_votes_short_reviews

    from source
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select * from aggregated