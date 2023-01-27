with

owners as (

    select
        row_number() over ( order by owner_id ) as rowid, *
        
    from {{ ref('stg_owners') }}
)

select * from owners