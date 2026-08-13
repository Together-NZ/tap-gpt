{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__MOBILE', 'google_ads_search_transformed__mobile'),
    alias='bing_ads_search_keyword__mobile',
) }}
SELECT * FROM {{ source('google_ads_search', 'bing_ads_search_keyword') }} 
WHERE LOWER(campaign_name) LIKE '%mobile%'
