{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
) }}

{{ google_ads.google_ads_search_keyword(client_id=6433737339) }}
