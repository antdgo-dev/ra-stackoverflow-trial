with

unknown_question as (

    select
        -1 as question_id,
        'Unknown' as title,
        'Unknown' as body
),

questions as (

    select *
    from {{ ref('stg_questions') }}
),

all_questions as (

    select * from unknown_question
    union all
    select * from questions
),

dim_questions as (

    select
        row_number() over ( order by question_id ) as rowid, *
    
    from all_questions
)

select * from dim_questions