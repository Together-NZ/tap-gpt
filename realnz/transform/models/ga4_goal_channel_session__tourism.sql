{{ config(
    materialized='table'
) }}

{{ ga4.ga4_goal_channel(
    source_name='dash_union__tourism',
    table_name='dash_union__tourism',
    plan_code='tourism',
    ga4_goal_a_model='ga4_goal_a_session__tourism'
) }}
