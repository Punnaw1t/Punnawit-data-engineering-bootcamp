with

users as (

    select * from {{ ref('stg_greenery__users') }}

)

, final as (

    select
        -- Changed user_guid to user_id based on the error message
        count(distinct user_id) as user_count

    from users

)

select * from final