{{
    config(
        materialized='view'
    )
}}

with source as (
    select * 
    from {{ source('staging', 'ext_fhv_tripdata') }}
    WHERE dispatching_base_num IS NOT NULL
),
renamed as (
    select
        dispatching_base_num,
        {{ bigint_to_timestamp("pickup_datetime") }} as pickup_datetime,
        {{ bigint_to_timestamp("dropoff_datetime") }} as dropoff_datetime,

        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
        
        sr_flag,
        affiliated_base_number
    from source
)
select * 
from renamed
-- dbt build --select stg_ext_green_tripdata --vars '{'is_test_run': false}'
-- dbt build --vars '{'is_test_run': false}'
{% if var('is_test_run', default=true) %}
limit 100
{%- endif %}