with

posts_questions as (

    select *

    from {{ ref('stg_posts_questions') }}
),

dim_owner as (

    select
        rowid,
        owner_id

    from {{ ref( 'dim_owner' ) }}
),

dim_question as (

    select
        rowid,
        question_id

    from {{ ref( 'dim_question' ) }}
),

dim_creation_date as (

    select
        date_id,
        date
    
    from {{ ref( 'dim_creation_date' ) }}
),

dim_last_activity_date as (

    select
        date_id,
        date
    
    from {{ ref( 'dim_last_activity_date' ) }}
),

dim_last_edit_date as (

    select
        date_id,
        date
    
    from {{ ref( 'dim_last_edit_date' ) }}
),

fct_posts_questions as (

    select

        -- dimensions
        ifnull( dim_question.rowid, 1 ) as question_rowid,
        ifnull( dim_owner.rowid, 1 ) as owner_rowid,
        ifnull( dim_creation_date.date_id, 19000101 ) as creation_date_id,
        ifnull( dim_last_activity_date.date_id, 19000101 ) as last_activity_date_id,
        ifnull( dim_last_edit_date.date_id, 19000101 ) as last_edit_date_id,
        
        -- measures
        posts_questions.answer_count,
        posts_questions.comment_count,
        posts_questions.favorite_count,
        posts_questions.view_count,
        posts_questions.score

    from posts_questions
    
    left join dim_question on posts_questions.question_id = dim_question.question_id
    left join dim_owner on posts_questions.owner_id = dim_owner.owner_id
    left join dim_creation_date on cast( posts_questions.creation_date as date ) = dim_creation_date.date
    left join dim_last_activity_date on cast( posts_questions.last_activity_date as date ) = dim_last_activity_date.date
    left join dim_last_edit_date on cast( posts_questions.last_edit_date as date ) = dim_last_edit_date.date
)

select * from fct_posts_questions