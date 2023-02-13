select distinct
    owner_user_id as owner_id,
    owner_display_name as owner_name
    
from {{ source( 'stackoverflow_src', 'posts_questions' ) }}

where owner_user_id is not null