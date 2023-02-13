select
    id as question_id,
    title

from {{ source( 'stackoverflow_src', 'posts_questions' ) }}

where id is not null