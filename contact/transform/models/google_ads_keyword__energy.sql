{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__ENERGY', 'google_ads_search_transformed__energy'),
    alias='google_ads_keyword__energy',
) }}
SELECT * FROM {{ source('google_ads_search', 'google_ads_keyword') }} 
WHERE LOWER(campaign_name) NOT LIKE '%mobile%'
  AND LOWER(campaign_name) NOT LIKE '%broadband%'
