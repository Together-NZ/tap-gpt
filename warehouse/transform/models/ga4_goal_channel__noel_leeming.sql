{{ config(
    materialized='table',
    schema='ga4_transformed__noel_leeming',
    alias='ga4_goal_channel__noel_leeming',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}
{{ga4.ga4_goal_channel_final(ga4_session_source_name='ga4_session_transformed__noel_leeming', ga4_session_table_name='ga4_goal_channel_session__noel_leeming', ga4_goal_source_name='ga4_goal_transformed__noel_leeming', ga4_goal_table_name='ga4_goal_channel_goal__noel_leeming')}}
