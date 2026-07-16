{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_demand__marketing'
) }}

{{ google_ads.google_ads_demand(client_id=7073793042) }}
