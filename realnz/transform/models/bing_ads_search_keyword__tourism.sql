{{ config(
    materialized='table',
    alias='bing_ads_keyword__tourism'
) }}

{{ google_ads.bing_ads_search_keyword(client_id=1704016095) }}