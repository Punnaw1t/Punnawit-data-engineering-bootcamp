with

int_orders_addresses__joined as (

        select * from {{ ref('int_orders_addresses__joined') }}

)

, final as (

    select
        state,
        -- Now 'order_id' should exist in int_orders_addresses__joined
        count(order_id) as number_of_orders

    from int_orders_addresses__joined
    group by state
    order by 2 desc
    limit 1

)

select * from final