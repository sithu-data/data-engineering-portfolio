with source as (
    select * from {{ ref('int_product_metadata_cleaned') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['parent_asin']) }}
                                                as product_key,
        parent_asin,
        product_title,
        store,
        main_category,
        price,
        price_tier,
        average_rating,
        rating_number,
        review_volume_tier
    from source
)

select * from final