select
    id as question_id,
    tags
  
from {{ source( 'stackoverflow', 'posts_questions_202209' ) }}

where id is not null