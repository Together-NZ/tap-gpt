{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twhs',
    alias='google_ads_search__twhs',
) }}

{{ google_ads.google_ads_search(client_id=3958866521) }}
