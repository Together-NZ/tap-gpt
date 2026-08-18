{{ config(
    materialized='table',
    alias='bing_ads_keyword__tourism',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.bing_ads_search_keyword(client_id=1704016095) }}