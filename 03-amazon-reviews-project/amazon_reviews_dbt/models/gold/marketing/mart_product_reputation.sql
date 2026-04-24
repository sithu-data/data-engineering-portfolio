with source as (
    select * from {{ ref('int_reviews_enriched') }}
),

aggregated as (
    select
        parent_asin,
        product_title,
        store,
        main_category,
        price,
        price_tier,
        review_volume_tier,

        count(*)                                        as total_reviews,
        avg(rating)                                     as avg_rating,
        avg(cortex_sentiment_score)                     as avg_sentiment_score,

        -- Composite reputation score (weighted avg of rating + sentiment)
        round(
            (avg(rating) / 5 * 0.6)
            + ((avg(cortex_sentiment_score) + 1) / 2 * 0.4), 4
        )                                               as reputation_score,

        sum(case when cortex_sentiment_label = 'positive' then 1 else 0 end) as positive_count,
        sum(case when cortex_sentiment_label = 'negative' then 1 else 0 end) as negative_count,
        sum(helpful_vote)                               as total_helpful_votes,

        -- Rating vs sentiment mismatch flag
        case
            when avg(rating) >= 4
            and avg(cortex_sentiment_score) < 0         then true
            when avg(rating) <= 2
            and avg(cortex_sentiment_score) > 0.3       then true
            else false
        end                                             as sentiment_rating_mismatch,

        min(review_date)                                as first_review_date,
        max(review_date)                                as latest_review_date

    from source
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from aggregated