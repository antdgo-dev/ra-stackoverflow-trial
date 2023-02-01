with

unknown_owner as (

    select
        -1 as owner_id,
        'Unknown' as owner_name
),

owners as (

    select *
    from {{ ref('stg_owners') }}
),

unique_owners_id as (

    select
        owner_id,
        count(*)

    from owners

    group by owner_id
    having count(*) < 2
),

unique_owners as (

    select
        owners.owner_id,
        owners.owner_name

    from owners

    join unique_owners_id on owners.owner_id = unique_owners_id.owner_id
),

duplicate_owners_id as (

    select
        owner_id,
        count(*)
    
    from owners

    group by owner_id
    having count(*) > 1
),

deduplicate_owners as (

    select
        owners.owner_id,
        owners.owner_name

    from owners

    join duplicate_owners_id on owners.owner_id = duplicate_owners_id.owner_id
    
    where owners.owner_name is not null
),

all_owners as (

    select owner_id, owner_name from unknown_owner

    union all
    
    select owner_id, owner_name from unique_owners
    
    union all
    
    select owner_id, owner_name from deduplicate_owners
),

dim_owner as (

    select
        row_number() over ( order by owner_id ) as rowid, *
        
    from all_owners
)

select * from dim_owner