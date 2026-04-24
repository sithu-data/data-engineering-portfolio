with source as (
    select * from {{ ref('stg_electronics_metadata') }}
),

cleaned as (
    select
        parent_asin,
        product_title,
        store,
        main_category,
        average_rating,
        rating_number,

        -- Safe cast price to handle non-numeric values
        try_cast(price::varchar as float)          as price,

        case
            when try_cast(price::varchar as float) is null    then 'unknown'
            when try_cast(price::varchar as float) < 20       then 'budget'
            when try_cast(price::varchar as float) between 20 and 100 then 'mid_range'
            when try_cast(price::varchar as float) > 100      then 'premium'
        end                                        as price_tier,

        case
            when rating_number >= 1000  then 'high_volume'
            when rating_number >= 100   then 'mid_volume'
            else 'low_volume'
        end                                        as review_volume_tier,

        loaded_at
    from source
    where product_title is not null
)

select * from cleaned