{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='ga4_transformed__noel_leeming',
    alias='ga4_goal_ecommerce__noel_leeming',
) }}

{{ ga4.ga4_goal_ecommerce(
    source_name='ga4_raw__noel_leeming',
    table_name='ecommerce_goal',
    dash_source_name='dash_union__noel_leeming',
    dash_table_name='dash_union__noel_leeming'
) }}
