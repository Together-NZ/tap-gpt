{{ config(
    materialized='table',
) }}
{{ga4.ga4_goal_channel_final(ga4_session_source_name='ga4_session_transformed__wop', ga4_session_table_name='ga4_goal_channel_session__wop', ga4_goal_source_name='ga4_goal_transformed__wop', ga4_goal_table_name='ga4_goal_channel_goal__wop')}}