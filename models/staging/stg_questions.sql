select
    id as question_id,
    title,
    body

from {{ source('stackoverflow', 'posts_questions_202209') }}