with

questions as (

    select 
        row_number() over ( order by question_id ) as rowid, * 
    
    from {{ ref('stg_questions') }}
)

select * from questions