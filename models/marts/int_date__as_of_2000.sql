with

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

dim_date as (
    
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
)

select * from dim_date