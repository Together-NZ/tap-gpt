{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}

{{ ga4.ga4_goal_a_session(
    source_name='ga4_raw__tourism',
    table_name='session_goal',
    plan_code='tourism',
    dash_union_source_name='dash_union__tourism',
    dash_union_table_name='dash_union__tourism'
) }}
