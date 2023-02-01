{{
    config(
        materialized = "view"
    )
}}

with

dim_date as (

    select * from {{ ref('int_date__as_of_2000') }}

)

select * from dim_date