{{
    config(
        materialized = "view"
    )
}}

with

bdg_tags as (

    select *
    from {{ ref( 'bdg_tags' ) }}
),

dim_tag as (

    select *
    from {{ ref( 'dim_tag' ) }}
),

dim_question as (

    select rowid
    from {{ ref( 'dim_question' ) }}
),

fct_posts_questions as (

    select

        question_rowid,
        creation_date_id,

        answer_count,
        view_count
    
    from {{ ref( 'fct_posts_questions' ) }}
),

tbl_totals_by_tag as (

    select
        t.tag,

        count(b.question_rowid) as questions,
        f.creation_date_id,

        sum(f.answer_count) as answers,
        sum(f.view_count) as views
    
    from bdg_tags as b

    left join dim_tag as t on b.tag_rowid = t.rowid
    left join dim_question as q on b.question_rowid = q.rowid
    left join fct_posts_questions as f on f.question_rowid = q.rowid

    group by
        t.tag,
        f.creation_date_id

)

select * from tbl_totals_by_tag