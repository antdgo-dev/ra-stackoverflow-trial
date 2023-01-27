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

all_owners as (

    select * from unknown_owner
    union all
    select * from owners
),

dim_owners as (

    select
        row_number() over ( order by owner_id ) as rowid, *
        
    from all_owners
)

select * from dim_owners