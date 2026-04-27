with source as (
    select distinct
        store
    from {{ ref('int_product_metadata_cleaned') }}
    where store is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['store']) }} as store_key,
        store
    from source
)

select * from final