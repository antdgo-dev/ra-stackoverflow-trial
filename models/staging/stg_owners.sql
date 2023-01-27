select distinct
    owner_user_id as owner_id,
    owner_display_name as owner_name
    
from {{ source( 'stackoverflow', 'posts_questions_202209' ) }}

where owner_user_id is not null