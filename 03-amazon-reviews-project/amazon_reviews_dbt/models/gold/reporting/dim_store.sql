with source as (
    select distinct
        store,
        main_category,
        review_volume_tier
    from {{ ref('int_product_metadata_cleaned') }}
    where store is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['store']) }}
                                                as store_key,
        store,
        main_category,
        review_volume_tier
    from source
)

select * from final