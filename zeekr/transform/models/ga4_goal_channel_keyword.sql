{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}

{{ ga4.ga4_goal_channel_keyword(
    source_name='ga4_raw',
    table_name='keyword_goal',
    dash_union_source_name='dash_union',
    dash_union_table_name='dash_union'
) }}
