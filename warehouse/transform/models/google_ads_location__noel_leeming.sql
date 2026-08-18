{{ config(
    materialized='table',
    schema='google_ads_search_transformed__noel_leeming',
    alias='google_ads_location__noel_leeming',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.google_ads_search_location(client_id=7068886290) }}
