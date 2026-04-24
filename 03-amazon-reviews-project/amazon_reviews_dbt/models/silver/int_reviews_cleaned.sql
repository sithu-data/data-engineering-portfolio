with source as (
    select * from {{ ref('stg_electronics_reviews') }}
),

cleaned as (
    select
        asin,
        parent_asin,
        user_id,
        rating,
        review_title,
        review_text,
        helpful_vote,
        verified_purchase,
        review_date,

        -- Derived fields (business logic belongs in Silver)
        year(review_date)                          as review_year,
        month(review_date)                         as review_month,
        length(review_text)                        as review_text_length,
        helpful_vote > 0                           as is_helpful,
        case
            when rating >= 4 then 'positive'
            when rating = 3  then 'neutral'
            else 'negative'
        end                                        as rating_sentiment,

        -- Cortex AI sentiment score (-1 negative to +1 positive)
        snowflake.cortex.sentiment(review_text)    as cortex_sentiment_score,

        loaded_at
    from source
    where
        review_text is not null
        and length(trim(review_text)) > 10
        and rating is not null
)

select * from cleaned