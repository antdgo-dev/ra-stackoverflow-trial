with

owners as (

    select * from {{ ref('stg_owners') }}
)

select * from owners