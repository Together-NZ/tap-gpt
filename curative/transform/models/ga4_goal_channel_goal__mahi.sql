{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}
{{ ga4.ga4_goal_channel(
    source_name='dash_union__mahi',
    table_name='dash_union__mahi',
    plan_code=env_var('PLAN_CODE_MAHI', 'mahi'),
    ga4_goal_a_model='ga4_goal_a__mahi'
) }}
