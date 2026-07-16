{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_location_marketing'
) }}

{{ google_ads.google_ads_search_location(client_id=7073793042) }}
