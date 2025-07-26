with

orders as (

    select * from {{ ref('stg_greenery__orders') }}

)

, final as (

    select
        -- Changed order_guid to order_id based on the error message
        count(distinct order_id) as order_count

    from orders

)

select * from final