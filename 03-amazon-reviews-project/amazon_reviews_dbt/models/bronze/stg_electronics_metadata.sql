with source as (
    select
        raw_data,
        loaded_at
    from AMAZON_REVIEWS_RAW_DB.RAW.RAW_ELECTRONICS_METADATA
),

flattened as (
    select
        raw_data:parent_asin::varchar          as parent_asin,
        raw_data:title::varchar                as product_title,
        raw_data:store::varchar                as store,
        raw_data:main_category::varchar        as main_category,
        raw_data:average_rating::float         as average_rating,
        raw_data:rating_number::integer        as rating_number,
        raw_data:price::float                  as price,
        raw_data:categories::variant           as categories,
        raw_data:features::variant             as features,
        raw_data:description::variant          as description,
        raw_data:details::variant              as details,
        loaded_at
    from source
)

select * from flattened