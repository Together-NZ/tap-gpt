

{{ config(
    materialized='table',
    schema='google_ads_search_transformed',
    alias='google_ads_search',
) }}

{{ google_ads.google_ads_search(client_id=5628751301) }}
