with source as (
    select * from {{ ref('stg_electronics_reviews') }}
),

metadata as (
    select distinct parent_asin
    from {{ ref('stg_electronics_metadata') }}
),

cleaned as (
    select
        s.asin,
        s.parent_asin,
        s.user_id,
        s.rating,
        s.review_title,
        s.review_text,
        s.helpful_vote,
        s.verified_purchase,
        s.review_date,
        year(s.review_date)                            as review_year,
        month(s.review_date)                           as review_month,
        length(s.review_text)                          as review_text_length,
        s.helpful_vote > 0                             as is_helpful,
        case
            when s.rating >= 4 then 'positive'
            when s.rating = 3  then 'neutral'
            else 'negative'
        end                                            as rating_sentiment,
        snowflake.cortex.sentiment(s.review_text)      as cortex_sentiment_score,
        s.loaded_at
    from source s
    inner join metadata m
        on s.parent_asin = m.parent_asin
    where
        s.review_text is not null
        and length(trim(s.review_text)) > 10
        and s.rating is not null

    -- Deduplicate exact duplicate rows from source
    qualify row_number() over (
        partition by s.asin, s.user_id, s.review_date, s.review_title, s.review_text
        order by s.loaded_at desc
    ) = 1
)

select * from cleaned