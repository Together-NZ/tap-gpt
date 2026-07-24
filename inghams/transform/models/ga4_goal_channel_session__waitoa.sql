{{ config(materialized='table') }}

{{ ga4.ga4_goal_channel(
    source_name='dash_union__waitoa',
    table_name='dash_union__waitoa',
    plan_code=env_var('PLAN_CODE_WAITOA', 'wai'),
    ga4_goal_a_model='ga4_goal_a_session__waitoa'
) }}
