{{ config(
    materialized='table',
    schema='google_ads_search_transformed__noel_leeming',
    alias='google_ads_location__noel_leeming',
) }}

{{ google_ads.google_ads_search_location(client_id=7068886290) }}
