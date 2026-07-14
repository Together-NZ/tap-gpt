{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_search__tourism'
) }}

{{ google_ads.google_ads_search(client_id=5773966980) }}
