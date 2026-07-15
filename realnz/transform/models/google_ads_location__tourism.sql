{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_location__tourism'
) }}

{{ google_ads.google_ads_search_location(client_id=5773966980) }}
