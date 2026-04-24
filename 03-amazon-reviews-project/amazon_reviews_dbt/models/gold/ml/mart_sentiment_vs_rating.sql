with source as (
    select * from {{ ref('int_reviews_enriched') }}
),

comparison as (
    select
        parent_asin,
        product_title,
        store,
        main_category,
        price_tier,
        review_year,

        rating,
        rating_sentiment,
        cortex_sentiment_label,
        cortex_sentiment_score,

        -- Agreement flag between star rating and Cortex sentiment
        case
            when rating_sentiment = cortex_sentiment_label  then true
            else false
        end                                             as sentiment_agrees_with_rating,

        -- Mismatch type for analysis
        case
            when rating_sentiment = 'positive'
            and cortex_sentiment_label = 'negative'     then 'high_rating_negative_text'
            when rating_sentiment = 'negative'
            and cortex_sentiment_label = 'positive'     then 'low_rating_positive_text'
            when rating_sentiment != cortex_sentiment_label then 'minor_mismatch'
            else 'aligned'
        end                                             as mismatch_type,

        review_text_length,
        helpful_vote,
        verified_purchase,
        review_date

    from source
)

select * from comparison