{{ config(
    materialized='table',
    schema='google_ads_search_transformed__mountain',
    alias='google_ads_location__mountain',
) }}

{{ google_ads.google_ads_search_location(client_id=2977297812) }}
