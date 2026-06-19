{{ config(
    materialized='table',
) }}
{{ google_ads.google_ads_search_keyword(client_id=3642900300) }}