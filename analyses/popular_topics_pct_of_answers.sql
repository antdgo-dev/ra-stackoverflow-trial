with

selected_tags as (

  select
    b.tag,
    sum( b.questions ) as n_questions, 
    sum( b.answers ) as n_answers,
    round( sum( b.answers ) / sum ( b.questions ), 2 ) as ratio_aq

  from {{ ref( 'tbl_totals_by_tag' ) }} as b

  where
    b.tag in ( 
        
        {% for selected_tag in get_opportunity_tags(500, 20, 1500, 5000) -%}
            '{{ selected_tag }}'
            {%- if not loop.last -%}
                ,
            {%- endif %}
        {% endfor %}
    )
    
  group by b.tag

),

total_answers as (

  select
    sum(n_answers) as sum_answers
  
  from selected_tags
)

select
  tag,
  round( 100 * ( n_answers / sum_answers ), 1 ) as pct

from selected_tags
cross join total_answers