{{ config(materialized='table') }}

with fvh_data as (
    Select * from {{ ref('stg_fhv_tripdata') }} 
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
) 

select 
    fvh_data.pickup_datetime,
    fvh_data.dropoff_datetime,
    fvh_data.pickup_location_id,
    pickup_location.zone as pickup_zone,
    pickup_location.borough as pickup_borough,
    fvh_data.dropoff_location_id,
    dropoff_location.zone as dropoff_zone,
    dropoff_location.borough as dropoff_borough
from fvh_data
inner join dim_zones as pickup_location
on fvh_data.pickup_location_id = pickup_location.locationid
inner join dim_zones as dropoff_location
on fvh_data.dropoff_location_id = dropoff_location.locationid