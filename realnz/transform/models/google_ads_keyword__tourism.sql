{{ config(
    materialized='table',
    schema='google_ads_search_transformed__tourism',
    alias='google_ads_keyword__tourism',
) }}

{{ google_ads.google_ads_search_keyword(client_id=5773966980) }}
