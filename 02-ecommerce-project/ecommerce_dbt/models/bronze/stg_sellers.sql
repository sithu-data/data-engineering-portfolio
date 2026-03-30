with source as (
    select * from {{ source('raw', 'olist_sellers') }}
),
staged as (
    select
        seller_id                 as seller_id,
        seller_zip_code_prefix    as zip_code,
        seller_city               as city,
        seller_state              as state
    from source
)
select * from staged