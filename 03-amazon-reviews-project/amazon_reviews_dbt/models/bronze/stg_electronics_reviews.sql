with source as (
    select
        raw_data,
        loaded_at
    from AMAZON_REVIEWS_RAW_DB.RAW.RAW_ELECTRONICS_REVIEWS
),

flattened as (
    select
        raw_data:asin::varchar                as asin,
        raw_data:parent_asin::varchar         as parent_asin,
        raw_data:user_id::varchar             as user_id,
        raw_data:rating::integer              as rating,
        raw_data:title::varchar               as review_title,
        raw_data:text::varchar                as review_text,
        raw_data:helpful_vote::integer        as helpful_vote,
        raw_data:verified_purchase::boolean   as verified_purchase,
        raw_data:timestamp::bigint            as review_timestamp_ms,
        to_timestamp(
            raw_data:timestamp::bigint / 1000
        )                                     as review_date,
        loaded_at
    from source
)

select * from flattened