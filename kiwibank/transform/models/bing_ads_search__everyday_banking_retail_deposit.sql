{{ config(
    materialized='table',
) }}
{{ google_ads.bing_ads_search_kiwibank(client_id=6942648948) }}