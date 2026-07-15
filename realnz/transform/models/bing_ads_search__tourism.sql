{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='bing_ads_search__tourism'
) }}

{{ google_ads.bing_ads_search_sa360(client_id=1704016095) }}
