{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}

{{ ga4.ga4_goal_ecommerce(
    source_name='ga4_raw__tourism',
    table_name='ecommerce_goal',
    dash_source_name='dash_union__tourism',
    dash_table_name='dash_union__tourism'
) }}
