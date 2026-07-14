{{ config(
    materialized='table',
    alias='google_ads_search__tourism'
) }}

{{ google_ads.google_ads_search(client_id=5773966980) }}
