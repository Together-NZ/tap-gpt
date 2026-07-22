{{ config(
    materialized='table',
) }}

{{ ga4.ga4_goal_a(
    source_name='ga4_raw',
    table_name='goal',
    plan_code=env_var('PLAN_CODE', 'bs'),
    dash_union_source_name='dash_table',
    dash_union_table_name='dash_union'
) }}
