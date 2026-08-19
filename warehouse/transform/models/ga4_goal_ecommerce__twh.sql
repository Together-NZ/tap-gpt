{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    schema='ga4_transformed__twh',
    alias='ga4_goal_ecommerce__twh',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}

{{ ga4.ga4_goal_ecommerce(
    source_name='ga4_raw__twh',
    table_name='ecommerce_goal',
    dash_source_name='dash_union__twh',
    dash_table_name='dash_union__twh'
) }}
