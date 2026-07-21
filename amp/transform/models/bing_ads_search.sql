{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
) }}

{{ google_ads.bing_ads_search_sa360(client_id=7701452964) }}
