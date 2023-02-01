with

unknown_date as (

    select
        19000101 as date_id,
        cast ( '1900-01-01' as date format 'YYYY-MM-DD' ) as date,
        1900 as year,
        -1 as quarter,
        -1 as month,
        -1 as day,
        'N/A' as week_day_desc
),

date_spine as (

    {{
        dbt_utils.date_spine(
            datepart = "DAY",
            start_date = "CAST('2000-01-01' as date)",
            end_date = "CAST('2025-01-01' as date)"
        )
    }}
),

date_keys as (

    select
        cast( date_DAY as date ) as date_key
    
    from date_spine
),

date_keys_extended as (
    
    select
        cast(
            extract( year from date_key ) ||
            right ('0' || cast( extract( month from date_key ) as string ), 2 ) ||
            right ('0' || cast( extract( day from date_key ) as string ), 2 ) as int
        ) as date_id,
        date_key as date,
        extract( year from date_key ) as year,
        extract( quarter from date_key ) as quarter,
        extract( month from date_key ) as month,
        extract ( day from date_key ) as day,
        case extract( dayofweek from date_key )   
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            else 'Saturday'
        end as week_day_desc
    
    from date_keys
    order by 1
),

dim_date as (

    select * from unknown_date

    union all

    select * from date_keys_extended
)

select * from dim_date