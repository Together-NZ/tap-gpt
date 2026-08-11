{{ config(
    materialized='table',
) }}
{{ google_ads.bing_ads_search_keyword(client_id=3325545009) }}