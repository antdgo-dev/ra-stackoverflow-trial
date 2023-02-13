{{ config( schema = 'src' ) }}

select
    -- questions
    id,
    title,

    -- owners
    owner_user_id,
    owner_display_name,

    -- tags
    tags,

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

from {{ source( 'public_stackoverflow', 'posts_questions' ) }}

{{ limit_data( 'creation_date', '2022-08-01', '2022-09-25' ) }}