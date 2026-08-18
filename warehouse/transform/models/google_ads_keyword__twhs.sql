{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twhs',
    alias='google_ads_keyword__twhs',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.google_ads_search_keyword(client_id=3958866521) }}
