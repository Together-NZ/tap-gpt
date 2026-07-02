{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twh',
    alias='google_ads_location__twh',
) }}

{{ google_ads.google_ads_search_location(client_id=5427952977) }}
