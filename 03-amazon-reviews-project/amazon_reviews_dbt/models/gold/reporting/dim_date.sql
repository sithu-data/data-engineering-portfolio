with date_spine as (
    select
        dateadd(day, seq4(), '1998-01-01'::date) as date_day
    from table(generator(rowcount => 10000))  -- generous buffer, filter controls the end
),

final as (
    select
        to_varchar(date_day, 'YYYYMMDD')::integer       as date_key,
        date_day                                        as full_date,
        year(date_day)                                  as year,
        quarter(date_day)                               as quarter,
        month(date_day)                                 as month,
        to_varchar(date_day, 'MON')                     as month_name_short,
        to_varchar(date_day, 'MMMM')                    as month_name,
        to_varchar(date_day, 'MON YYYY')                as month_year,
        weekofyear(date_day)                            as week_of_year,
        dayofmonth(date_day)                            as day_of_month,
        dayofweek(date_day)                             as day_of_week,
        to_varchar(date_day, 'DY')                      as day_name,
        case when dayofweek(date_day) in (0, 6)
            then true else false
        end                                             as is_weekend
    from date_spine
    where date_day between '1998-01-01' and '2023-12-31'  -- start and end clearly visible
)

select * from final