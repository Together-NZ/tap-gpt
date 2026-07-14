{{ config(
    materialized='table',
    schema='google_ads_search_transformed__mountain',
    alias='google_ads_search__mountain',
) }}

{{ google_ads.google_ads_search(client_id=2977297812) }}
