{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_keyword__mountain'
) }}

{{ google_ads.google_ads_search_keyword(client_id=2977297812) }}
