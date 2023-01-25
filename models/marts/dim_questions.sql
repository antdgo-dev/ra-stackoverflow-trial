with

questions as (

    select * from {{ ref('stg_questions') }}
)

select * from questions