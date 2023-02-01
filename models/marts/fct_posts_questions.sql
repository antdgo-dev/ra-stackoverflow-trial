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
        ifnull( posts_questions.answer_count, 0) as answer_count,
        ifnull( posts_questions.comment_count, 0) as comment_count,
        ifnull( posts_questions.favorite_count, 0) as favorite_count,
        ifnull( posts_questions.view_count, 0) as view_count,
        ifnull( posts_questions.score, 0) as score

    from posts_questions
    
    left join dim_question on posts_questions.question_id = dim_question.question_id
    left join dim_owner on posts_questions.owner_id = dim_owner.owner_id
    left join dim_creation_date on cast( posts_questions.creation_date as date ) = dim_creation_date.date
    left join dim_last_activity_date on cast( posts_questions.last_activity_date as date ) = dim_last_activity_date.date
    left join dim_last_edit_date on cast( posts_questions.last_edit_date as date ) = dim_last_edit_date.date
)

select * from fct_posts_questions