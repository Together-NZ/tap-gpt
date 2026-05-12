{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel_final(ga4_session_source_name='ga4_session_transformed__education', ga4_session_table_name='ga4_goal_channel__education_session', ga4_goal_source_name='ga4_goal_transformed__education', ga4_goal_table_name='ga4_goal_channel__education_goal') }}
