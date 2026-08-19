{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}
{{ ga4.ga4_goal_channel(
    source_name='dash_union__lotus',
    table_name='dash_union__lotus',
    plan_code=env_var('PLAN_CODE_GA4', 'lotus'),
    ga4_goal_a_model='ga4_goal_a_session__lotus'
) }}
