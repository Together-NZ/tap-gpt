{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}
{{ google_ads.bing_ads_search_keyword(
    client_id=6806887247,
    sa360_dataset='sa360_contact',
    transfer_id=4291961021,
) }}