{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ dash_table_general_process.dash_union_search(
    source_name='dash_table_search__hr_career',
    table_name='dash_table_search__hr_career',
    sub_brands='null'
) }}
