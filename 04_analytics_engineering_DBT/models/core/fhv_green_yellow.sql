with fhv_rides as (
    select
        'FHV' as service,
        count(*) as ride_count
    from {{ ref('fact_fhv_trips') }}
    where TIMESTAMP_TRUNC(pickup_datetime, MONTH) = TIMESTAMP('2019-07-01')
),

green_rides as (
    select
        'Green' as service,
        count(*) as ride_count
    from {{ source('staging', 'green_tripdata') }}
    where TIMESTAMP_TRUNC(pickup_datetime, MONTH) = TIMESTAMP('2019-07-01')
),

yellow_rides as (
    select
        'Yellow' as service,
        count(*) as ride_count
    from {{ source('staging', 'yellow_tripdata') }}
    where TIMESTAMP_TRUNC(pickup_datetime, MONTH) = TIMESTAMP('2019-07-01')
)

select * from fhv_rides
union all
select * from green_rides
union all
select * from yellow_rides
