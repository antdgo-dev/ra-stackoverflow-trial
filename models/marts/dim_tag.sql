with

split_tags as (

  select split(tags, '|') as some_tags
  from {{ ref( 'stg_tags' ) }}

),

all_tags as (

  select distinct tag
  from split_tags
  cross join split_tags.some_tags as tag

),

dim_tag as (

  select
    row_number() over ( order by tag ) as rowid,
    tag
  
  from all_tags
)

select * from dim_tag