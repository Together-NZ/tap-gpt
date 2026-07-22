{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
) }}

{{ google_ads.google_ads_search(client_id=9671916287) }}
