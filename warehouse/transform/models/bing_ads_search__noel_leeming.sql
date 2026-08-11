{{ config(
    materialized='table',
    schema='google_ads_search_transformed__noel_leeming',
    alias='bing_ads_search__noel_leeming',
) }}

{{ google_ads.bing_ads_search_warehouse(client_id=7068886290) }}
