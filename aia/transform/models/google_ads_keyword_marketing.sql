{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_keyword_marketing'
) }}

{{ google_ads.google_ads_search_keyword(client_id=7073793042) }}
