{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twh',
    alias='bing_ads_search__twh',
) }}

{{ google_ads.bing_ads_search_sa360(client_id=4824241958) }}
