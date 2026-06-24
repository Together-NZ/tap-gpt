{{ config(
    materialized='table',
    schema='ga4_transformed__twhs',
    alias='ga4_goal_channel__twhs',
) }}
{{ga4.ga4_goal_channel_final(ga4_session_source_name='ga4_session_transformed__twhs', ga4_session_table_name='ga4_goal_channel_session__twhs', ga4_goal_source_name='ga4_goal_transformed__twhs', ga4_goal_table_name='ga4_goal_channel_goal__twhs')}}
