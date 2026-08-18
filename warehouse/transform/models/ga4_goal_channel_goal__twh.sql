{{ config(
    materialized='table',
    schema='ga4_transformed__twh',
    alias='ga4_goal_channel_goal__twh',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union__twh', table_name='dash_union__twh', plan_code=env_var('PLAN_CODE', 'twh'), ga4_goal_a_model='ga4_goal_a__twh') }}
