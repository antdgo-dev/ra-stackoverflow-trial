select
    id as question_id,
    tags
  
from {{ source( 'stackoverflow_src', 'posts_questions' ) }}

where id is not null