{{
    config(
        materialized = "view"
    )
}}

with

dim_date as (

    select
        date_id as last_activity_date_id,
        date as last_activity_date,
        year as last_activity_year,
        quarter as last_activity_quarter,
        month as last_activity_month,
        day as last_activity_day,
        week_day_desc as last_activity_week_day_desc

    from {{ ref('int_date__as_of_2000') }}

)

select * from dim_date