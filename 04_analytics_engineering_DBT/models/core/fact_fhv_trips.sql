{{ config(materialized='table') }}

with fhv_trips as (
    select
        *,
        -- Generate a unique identifier for each trip if needed
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select
        locationid,
        borough,
        zone
    from {{ ref('dim_zones') }}
    where borough is not null and borough != 'Unknown'
)

select
    ft.*,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone
from fhv_trips ft
join dim_zones pz on ft.pickup_location_id = pz.locationid
join dim_zones dz on ft.dropoff_location_id = dz.locationid

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=false) %}



{% endif %}
