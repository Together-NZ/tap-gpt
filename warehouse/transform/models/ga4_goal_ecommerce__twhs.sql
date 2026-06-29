{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='ga4_transformed__twhs',
    alias='ga4_goal_ecommerce__twhs',
) }}

{{ ga4.ga4_goal_ecommerce(
    source_name='ga4_raw__twhs',
    table_name='ecommerce_goal',
    dash_source_name='dash_union__twhs',
    dash_table_name='dash_union__twhs'
) }}
