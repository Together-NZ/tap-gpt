{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twh',
    alias='bing_ads_keyword__twh',
) }}

{{ google_ads.bing_ads_search_keyword(client_id=4824241958) }}
