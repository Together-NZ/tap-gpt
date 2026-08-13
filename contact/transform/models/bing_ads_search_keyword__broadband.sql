{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__BROADBAND', 'google_ads_search_transformed__broadband'),
    alias='bing_ads_search_keyword__broadband',
) }}
SELECT * FROM {{ source('google_ads_search', 'bing_ads_search_keyword') }} 
WHERE LOWER(campaign_name) LIKE '%broadband%'
