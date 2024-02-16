{{
  config(
    materialized = "view"
    )
}}

with source as (
    select * 
    from {{ source('staging', 'ext_green_tripdata') }}
    WHERE vendorid is not null
    QUALIFY(ROW_NUMBER() OVER(PARTITION BY vendorid,lpep_pickup_datetime))=1
)
,renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendorid','lpep_pickup_datetime']) }} as trip_id,
        CAST(vendorid     as INT64) as vendor_id,
        CAST(ratecodeid   as INT64) as rate_code_id,
        CAST(pulocationid as INT64) as pu_location_id,
        CAST(dolocationid as INT64) as do_location_id,
        
        -- vendorid,    
        -- ratecodeid,  
        -- pulocationid,
        -- dolocationid,
        
        TIMESTAMP_MICROS(CAST(lpep_pickup_datetime /1000 as INT64)) as pickup_datetime,
        TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime/1000 as INT64)) as dropoff_datetime ,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        {{ get_payment_type_description('payment_type') }} AS payment_type_description,
        trip_type,
        congestion_surcharge

    from source

)

SELECT * 
FROM renamed

-- dbt build --select stg_ext_green_tripdata --vars '{'is_test_run': false}'
{% if var('is_test_run', default=true) %}
limit 100
{%- endif %}