{{ config(materialized='table') }}

with rental_data as (
    select * from {{ ref('fact_rental_stuttgart') }}
)
    select 
    --Grouping Columns 
    post_code as post_code,
    --date_trunc('month', pickup_datetime) as revenue_month, 
    --Note: For BQ use instead: 
    date_trunc(time_stamp, month) as listings_month, 
    date_trunc(time_stamp, year) as listings_year,

    -- Revenue calculation 
    count(listing_id) as number_of_listings,
    avg(price) as avg_price,
    avg(square_meters) as avg_posting_square_meter,
    avg(room_count) as avg_posting_room_count,
    avg(price_per_square_meters) as avg_posting_price_per_square_meter,


    from rental_data
    where post_code is not null
    group by 1,2,3