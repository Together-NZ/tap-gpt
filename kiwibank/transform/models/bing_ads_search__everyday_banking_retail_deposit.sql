{{ config(
    materialized='table',
) }}
{{ google_ads.bing_ads_search_sa360(client_id=3325545009) }}