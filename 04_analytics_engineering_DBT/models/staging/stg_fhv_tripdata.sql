{{ config(materialized='view') }}

-- with tripdata as (
--   select *,
--     row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
--   from {{ source('staging','stg_fhv_tripdata') }}
--   where dispatching_base_num is not null
--     and extract(year from pickup_datetime) = 2019 -- year filter
-- )

-- select
--     -- identifiers
--     {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,
--     dispatching_base_num,
--     PULocationID as pickup_location_id,
--     DOLocationID as dropoff_location_id,
    
--     -- timestamps
--     cast(pickup_datetime as timestamp) as pickup_datetime,
--     cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
--     -- other info
--     SR_Flag as sr_flag,
--     Affiliated_base_number as affiliated_base_number

-- from tripdata
-- where rn = 1

select 
    dispatching_base_num,
    cast(PULocationID as integer) as pickup_location_id,
    cast(DOLocationID as integer) as dropoff_location_id, 

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    sr_flag
    from {{ source('staging','stg_fhv_tripdata') }}
    where extract(year from pickup_datetime) = 2019
