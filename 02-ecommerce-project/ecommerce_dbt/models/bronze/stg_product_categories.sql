with source as (
    select * from {{ source('raw', 'product_category_names') }}
),
staged as (
    select
        product_category_name           as category_name_portuguese,
        product_category_name_english   as category_name_english
    from source
)
select * from staged