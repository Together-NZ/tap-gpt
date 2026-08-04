{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}

{{ ga4.ga4_goal_a_session(
    source_name='ga4_raw__fasd',
    table_name='session_goal',
    dash_union_source_name='dash_union__fasd',
    dash_union_table_name='dash_union__fasd'
) }}
