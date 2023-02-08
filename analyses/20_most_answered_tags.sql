select 
  b.tag, 
  sum(b.questions) as n_questions, 
  sum(b.answers ) as n_answers,
  round( sum(b.answers) / sum(b.questions), 2 ) as ratio_aq

from {{ ref( 'tbl_totals_by_tag' ) }} as b

group by
  b.tag

order by
  n_answers desc

limit 20