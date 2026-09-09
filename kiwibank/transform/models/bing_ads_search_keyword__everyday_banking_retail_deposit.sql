{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}
{{ google_ads.bing_ads_search_keyword(
    client_id=3325545009,
    sa360_dataset='search_ads_360_kiwibank',
    transfer_id=9771479395,
) }}
