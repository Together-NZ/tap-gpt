{{ config(
    materialized='table',
    schema='google_ads_search_transformed__volvo',
    alias='google_ads_location__volvo',
) }}

{{ google_ads.google_ads_search_location(client_id=4308178898) }}
