with reviews as (
    select * from {{ ref('int_reviews_cleaned') }}
),

products as (
    select * from {{ ref('int_product_metadata_cleaned') }}
),

enriched as (
    select
        r.asin,
        r.parent_asin,
        r.user_id,
        r.rating,
        r.review_title,
        r.review_text,
        r.helpful_vote,
        r.verified_purchase,
        r.review_date,
        r.review_year,
        r.review_month,
        r.review_text_length,
        r.is_helpful,
        r.rating_sentiment,
        r.cortex_sentiment_score,

        -- Sentiment classification from Cortex score
        case
            when r.cortex_sentiment_score >= 0.3  then 'positive'
            when r.cortex_sentiment_score <= -0.3 then 'negative'
            else 'neutral'
        end                                        as cortex_sentiment_label,

        -- Product context
        p.product_title,
        p.store,
        p.main_category,
        p.average_rating,
        p.rating_number,
        p.price,
        p.price_tier,
        p.review_volume_tier

    from reviews r
    left join products p
        on r.parent_asin = p.parent_asin
)

select * from enriched