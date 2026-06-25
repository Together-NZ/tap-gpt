{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='ga4_transformed__twhs',
    alias='ga4_goal_a__twhs',
) }}

{{ ga4.ga4_goal_a(source_name='ga4_raw__twhs', table_name='goal',dash_union_source_name='dash_union__twhs',dash_union_table_name='dash_union__twhs') }}
