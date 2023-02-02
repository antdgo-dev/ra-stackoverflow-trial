with

questions_tags as (

  select *
  from {{ ref( 'stg_tags' ) }}

),

split_tags as (

  select
    question_id,
    split(tags, '|') as some_tags
  
  from questions_tags

),

bridge_keys as (

  select distinct
    question_id,
    flattened_tag

  from split_tags
  cross join split_tags.some_tags as flattened_tag
),

dim_question as (

    select
        rowid,
        question_id
    
    from {{ ref( 'dim_question' ) }}
),

dim_tag as (

    select *
    from {{ ref( 'dim_tag' ) }}
),

bdg_tags as (

  select
    dim_question.rowid as question_rowid,
    dim_tag.rowid as tag_rowid

  from bridge_keys

  left join dim_question on bridge_keys.question_id = dim_question.question_id
  left join dim_tag on bridge_keys.flattened_tag = dim_tag.tag
)

select * from bdg_tags