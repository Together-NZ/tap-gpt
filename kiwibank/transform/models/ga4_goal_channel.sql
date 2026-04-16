{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union__everyday_banking_retail_deposit', table_name='dash_union__everyday_banking_retail_deposit', plan_code=env_var('PLAN_CODE_GA4', 'kiwibank'), ga4_goal_a_model='ga4_goal_a') }}
