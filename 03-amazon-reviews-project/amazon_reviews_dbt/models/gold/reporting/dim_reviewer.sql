with source as (
    select distinct
        user_id
    from {{ ref('int_reviews_cleaned') }}
    where user_id is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as reviewer_key,
        user_id
    from source
)

select * from final