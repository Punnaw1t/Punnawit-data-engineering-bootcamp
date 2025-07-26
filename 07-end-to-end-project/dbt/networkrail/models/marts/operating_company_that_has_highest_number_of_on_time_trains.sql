with

fct_movements as (

    select * from {{ ref('fct_movements') }}

)

select
    company_name
    , variation_status
    , count(1) as record_count

from fct_movements
where
    variation_status = 'LATE'
    and date(actual_timestamp_utc) >= date_add(current_date(), interval -3 day)
group by 1, 2
order by 3 desc
limit 1
