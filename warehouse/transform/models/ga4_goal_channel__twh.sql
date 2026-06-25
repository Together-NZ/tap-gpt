{{ config(
    materialized='table',
    schema='ga4_transformed__twh',
    alias='ga4_goal_channel__twh',
) }}
{{ga4.ga4_goal_channel_final(ga4_session_source_name='ga4_session_transformed__twh', ga4_session_table_name='ga4_goal_channel_session__twh', ga4_goal_source_name='ga4_goal_transformed__twh', ga4_goal_table_name='ga4_goal_channel_goal__twh')}}
