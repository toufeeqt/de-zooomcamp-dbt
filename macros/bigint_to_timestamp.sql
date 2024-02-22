{#
    This macro returns a timestamp from a bigint value
#}

{% macro bigint_to_timestamp(bigint) -%}
    TIMESTAMP_MICROS(CAST({{ bigint }}  /1000 as INT64))
{%- endmacro %}