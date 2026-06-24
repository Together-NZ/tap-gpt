{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twhs',
    alias='bing_ads_search__twhs',
) }}

{{ google_ads.bing_ads_search_warehouse(client_id=4407882180) }}
