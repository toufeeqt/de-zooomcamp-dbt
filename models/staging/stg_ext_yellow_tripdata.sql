{{
  config(
    materialized = "view"
    )
}}

with source as (
    select * 
    from {{ source('staging', 'ext_yellow_tripdata') }}
    WHERE vendorid is not null
    QUALIFY(ROW_NUMBER() OVER(PARTITION BY vendorid,tpep_pickup_datetime))=1
)
,renamed as (
    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,    
        {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
        {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

        -- timestamps
        TIMESTAMP_MICROS(CAST(tpep_pickup_datetime  /1000 as INT64)) as pickup_datetime,
        TIMESTAMP_MICROS(CAST(tpep_dropoff_datetime /1000 as INT64)) as dropoff_datetime,
        
        -- trip info
        store_and_fwd_flag,
        {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        -- yellow cabs are always street-hail
        1 as trip_type,
        
        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(0 as numeric) as ehail_fee,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description
    from source

)

SELECT * 
FROM renamed
-- dbt build --select stg_ext_green_tripdata --vars '{'is_test_run': false}'
{% if var('is_test_run', default=true) %}
limit 100
{%- endif %}