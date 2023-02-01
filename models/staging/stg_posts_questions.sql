select
    -- ids
    id as question_id,
    owner_user_id as owner_id,

    -- dates
    creation_date,
    last_activity_date,
    last_edit_date,

    -- measures
    answer_count,
    comment_count,
    favorite_count,
    view_count,
    score

from {{ source( 'stackoverflow', 'posts_questions_202209' ) }}