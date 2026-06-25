{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twhs',
    alias='google_ads_keyword__twhs',
) }}

{{ google_ads.google_ads_search_keyword(client_id=3958866521) }}
