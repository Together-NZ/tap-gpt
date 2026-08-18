{{ config(
    materialized='table',
    schema='google_ads_search_transformed',
    alias='google_ads_keyword',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.google_ads_search_keyword(client_id=5628751301) }}
