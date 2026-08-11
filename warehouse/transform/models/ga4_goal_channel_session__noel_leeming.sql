{{ config(
    materialized='table',
    schema='ga4_transformed__noel_leeming',
    alias='ga4_goal_channel_session__noel_leeming',
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union__noel_leeming', table_name='dash_union__noel_leeming', plan_code=env_var('PLAN_CODE', 'twh'), ga4_goal_a_model='ga4_goal_a_session__noel_leeming') }}
