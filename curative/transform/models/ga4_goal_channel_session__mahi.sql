{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(
    source_name='dash_union__mahi',
    table_name='dash_union__mahi',
    plan_code=env_var('PLAN_CODE_MAHI', 'mahi'),
    ga4_goal_a_model='ga4_goal_a_session__mahi'
) }}
