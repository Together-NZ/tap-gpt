{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel_final(ga4_session_source_name='ga4_session_transformed__career', ga4_session_table_name='ga4_goal_channel__career_session', ga4_goal_source_name='ga4_goal_transformed__career', ga4_goal_table_name='ga4_goal_channel__career_goal') }}
