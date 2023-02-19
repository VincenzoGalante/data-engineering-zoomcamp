{{ config(materialized='view') }} 

Select
    cast(dispatching_base_num as string) as dispatch_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationID as integer) as pickup_location_id,
    cast(DOlocationID as integer) as dropoff_location_id,
    cast(SR_Flag as integer) as sr_flag,
    cast(Affiliated_base_number as string) as affiliate_num
    
from {{ source('staging','fhv_taxi_data_local') }} 

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}