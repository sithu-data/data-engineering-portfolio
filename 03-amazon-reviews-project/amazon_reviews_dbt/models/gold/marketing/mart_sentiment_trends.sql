with source as (
    select * from {{ ref('int_reviews_enriched') }}
),

aggregated as  (
    select 
        review_year,
        review_month,
        main_category,
        store,
        price_tier,

        count(*)                                        as total_reviews,
        avg(rating)                                     as avg_rating,
        avg(cortex_sentiment_score)                     as avg_cortex_sentiment_score,

        sum(case when cortex_sentiment_label = 'positive' then 1 else 0 end) as positive_reviews,
        sum(case when cortex_sentiment_label = 'neutral'  then 1 else 0 end) as neutral_reviews,
        sum(case when cortex_sentiment_label = 'negative' then 1 else 0 end) as negative_reviews,
        
        round(
            sum(case when cortex_sentiment_label = 'positive' then 1 else 0 end)
            / nullif(count(*), 0) * 100, 2
        )                                               as positive_pct,

        sum(case when verified_purchase = true then 1 else 0 end) as verified_reviews,
        avg(case when verified_purchase = true then cortex_sentiment_score end) as verified_avg_sentiment

    from source
    group by 1, 2, 3, 4, 5
)

select * from aggregated