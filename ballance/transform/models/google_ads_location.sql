{{ config(
    materialized='table',
) }}

{{ google_ads.google_ads_search_location(client_id=3998638864) }}
