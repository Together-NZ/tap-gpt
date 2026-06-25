{{ config(
    materialized='table',
    schema='ga4_transformed__twhs',
    alias='ga4_goal_channel_goal__twhs',
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union__twhs', table_name='dash_union__twhs', plan_code=env_var('PLAN_CODE', 'twhs'), ga4_goal_a_model='ga4_goal_a__twhs') }}
