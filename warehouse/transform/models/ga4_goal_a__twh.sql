{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='ga4_transformed__twh',
    alias='ga4_goal_a__twh',
) }}

{{ ga4.ga4_goal_a(source_name='ga4_raw__twh', table_name='goal',dash_union_source_name='dash_union__twh',dash_union_table_name='dash_union__twh') }}
