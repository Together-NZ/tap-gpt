{{ config(
    materialized='table',
    schema='google_ads_search_transformed',
    alias='google_ads_location',
) }}

{{ google_ads.google_ads_search_location(client_id=5628751301) }}
