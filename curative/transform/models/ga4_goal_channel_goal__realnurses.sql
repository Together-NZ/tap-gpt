{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(
    source_name='dash_union__realnurses',
    table_name='dash_union__realnurses',
    plan_code=env_var('PLAN_CODE_REALNURSES', 'realnurses'),
    ga4_goal_a_model='ga4_goal_a__realnurses'
) }}
