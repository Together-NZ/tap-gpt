{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}

{{ ga4.ga4_goal_ecommerce(
    source_name='ga4_raw',
    table_name='ecommerce_goal',
    dash_source_name='dash_union',
    dash_table_name='dash_union'
) }}
