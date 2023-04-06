{{ config(materialized="view", target = 'dbt_models') }} --Create as views s.t. we don't have to refresh them but still always have the newest data

--removing entries with a vendor id that is null
with rental_data as
(
    select *
    from {{ source('core', 'stuttgart')}}
    where hash_code is not null
)
select
    -- identifier
    cast(hash_code as string) as listing_id,
    cast(post_code as integer) as post_code,
    
    -- timestamps
    cast(time_stamp as timestamp) as time_stamp,
    
    -- appartment description
    cast(borough as string) as borough_name,
    cast(title as string) as title,

    -- rental numeric data
    cast(price as numeric) as price,
    cast(posting_qm as numeric) as square_meters,
    cast(posting_room_count as numeric) as room_count,
    cast(price_per_qm as numeric) as price_per_square_meters,

from rental_data

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}
    limit 100
{% endif %}