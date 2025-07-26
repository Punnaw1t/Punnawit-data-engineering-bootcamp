
-- Transform 

with

source as (

    select * from {{ source('networkrail', 'movements') }}

)

, renamed_recasted as (

   select 
            event_type ,
            actual_timestamp as actual_timestamp_utc , 
            event_source , 
            train_id , 
            variation_status ,
            toc_id  
            


    from source

)

, final as (

    select 
            event_type ,
            actual_timestamp as actual_timestamp_utc , 
            event_source , 
            train_id , 
            variation_status ,
            toc_id

    from source


)

select * from final