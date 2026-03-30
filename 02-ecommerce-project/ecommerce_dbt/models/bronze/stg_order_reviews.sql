with source as (
    select * from {{ source('raw', 'olist_order_reviews') }}
),
staged as (
    select
        review_id,
        order_id,
        review_score                       as rating,
        review_comment_title               as review_title,
        review_comment_message             as review_body,
        review_creation_date::timestamp    as review_created_at,
        review_answer_timestamp::timestamp as review_answered_at
    from source
)
select * from staged