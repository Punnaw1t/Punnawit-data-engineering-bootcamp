with

orders as (

    select * from {{ ref('stg_greenery__orders') }}

)

, addresses as (

    select * from {{ ref('stg_greenery__addresses') }}

)

, joined as (

    select
        -- Columns from the 'orders' table (aliased as o)
        o.order_id,                     -- Corrected from order_guid
        o.user_id,                      -- Corrected from user_guid
        o.promo_id,                     -- Corrected from promo_guid
        o.address_id,                   -- Corrected from address_guid
        o.created_at,                   -- Corrected from order_created_at_utc
        o.order_cost,                   -- Corrected from order_cost_usd
        o.shipping_cost,                -- Corrected from shipping_cost_usd
        o.order_total,                  -- Corrected from order_total_usd
        o.tracking_id,                  -- Corrected from tracking_guid
        o.shipping_service,
        o.estimated_delivery_at,        -- Corrected from estimated_delivery_at_utc
        o.delivered_at,                 -- Corrected from delivered_at_utc
        o.status as order_status,       -- Corrected from order_status, aliased to keep original name

        -- Columns from the 'addresses' table (aliased as a)
        a.address,
        a.zipcode,
        a.state,
        a.country
    
    from orders as o
    join addresses as a
    -- Corrected the join key based on schema
    on o.address_id = a.address_id

)

select * from joined