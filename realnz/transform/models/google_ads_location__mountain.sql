{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_location__mountain'
) }}

{{ google_ads.google_ads_search_location(client_id=2977297812) }}
