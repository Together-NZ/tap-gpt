{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(
    source_name='dash_union__retirement_commission',
    table_name='dash_union__retirement_commission',
    plan_code=env_var('PLAN_CODE_GA4', 'retirement_commission'),
    ga4_goal_a_model='ga4_goal_a__retirement_commission'
) }}
