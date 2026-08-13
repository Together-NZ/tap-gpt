{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__ENERGY', 'google_ads_search_transformed__energy'),
    alias='bing_ads_search_keyword__energy',
) }}
SELECT * FROM {{ source('google_ads_search', 'bing_ads_search_keyword') }} 
WHERE LOWER(campaign_name) NOT IN (
    SELECT DISTINCT campaign_name FROM {{ source('google_ads_search__broadband', 'bing_ads_search_keyword__broadband') }}, {{ source('google_ads_search__mobile', 'bing_ads_search_keyword__mobile') }}
)
