{{ config(
    materialized='table',
    alias='google_ads_keyword__mountain'
) }}

{{ google_ads.google_ads_search_keyword(client_id=2977297812) }}
