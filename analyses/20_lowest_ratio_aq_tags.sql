select 
  b.tag, 
  sum(b.questions) as n_questions, 
  sum(b.answers ) as n_answers,
  round( sum(b.answers) / sum(b.questions), 2 ) as ratio_aq

from {{ ref( 'tbl_totals_by_tag' ) }} as b

group by
  b.tag

having
  sum(b.answers) > 500

order by
  ratio_aq

limit 20