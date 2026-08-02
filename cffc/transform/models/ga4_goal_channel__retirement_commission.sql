{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel_final(
    ga4_session_source_name='ga4_session_transformed__retirement_commission',
    ga4_session_table_name='ga4_goal_channel_session__retirement_commission',
    ga4_goal_source_name='ga4_goal_transformed__retirement_commission',
    ga4_goal_table_name='ga4_goal_channel_goal__retirement_commission'
) }}
