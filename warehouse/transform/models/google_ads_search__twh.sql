{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twh',
    alias='google_ads_search__twh',
) }}

{{ google_ads.google_ads_search(client_id=5427952977) }}
