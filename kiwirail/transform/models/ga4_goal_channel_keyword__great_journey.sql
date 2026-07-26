{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}

{{ ga4.ga4_goal_channel_keyword(
    source_name='ga4_raw__great_journey',
    table_name='keyword_goal',
    dash_union_source_name='dash_union__great_journey',
    dash_union_table_name='dash_union__great_journey'
) }}
