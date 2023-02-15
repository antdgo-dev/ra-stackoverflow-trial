{{
    config(
        materialized = "view"
    )
}}

with

dim_date as (

    select
        date_id as creation_date_id,
        date as creation_date,
        year as creation_year,
        quarter as creation_quarter,
        month as creation_month,
        day as creation_day,
        week_day_desc as creation_week_day_desc
    
    from {{ ref('int_date__as_of_2000') }}

)

select * from dim_date