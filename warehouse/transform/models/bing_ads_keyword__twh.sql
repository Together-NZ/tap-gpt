{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twh',
    alias='bing_ads_keyword__twh',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.bing_ads_search_keyword(client_id=4824241958) }}
