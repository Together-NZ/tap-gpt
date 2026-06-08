{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union__beervana', table_name='dash_union__beervana', plan_code=env_var('PLAN_CODE', 'ber'), ga4_goal_a_model='ga4_goal_a__beervana') }}