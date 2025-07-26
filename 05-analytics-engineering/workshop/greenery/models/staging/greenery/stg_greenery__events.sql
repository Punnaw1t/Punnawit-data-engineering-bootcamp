with

source as (

    select * from {{ source('greenery', 'events') }}

)

, renamed_recasted as (

    select
        event_id as event_guid
        ,session_id as session_guid
        ,page_url as event_page_url
        ,created_at as event_created_at_utc
        ,event_type as event_type
        ,user
        ,order
        ,product
        
    from source

)

select * from renamed_recasted