{{
    config(
        materialized = "view"
    )
}}

with

dim_date as (

    select
        date_id as last_edit_date_id,
        date as last_edit_date,
        year as last_edit_year,
        quarter as last_edit_quarter,
        month as last_edit_month,
        day as last_edit_day,
        week_day_desc as last_edit_week_day_desc
    
    from {{ ref('int_date__as_of_2000') }}

)

select * from dim_date