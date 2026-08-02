{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}
{{ ga4.ga4_goal_a(
    source_name='ga4_raw__retirement_commission',
    table_name='goal',
    dash_union_source_name='dash_union__retirement_commission',
    dash_union_table_name='dash_union__retirement_commission'
) }}
