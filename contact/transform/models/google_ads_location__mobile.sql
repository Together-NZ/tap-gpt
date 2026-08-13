{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__MOBILE', 'google_ads_search_transformed__mobile'),
    alias='google_ads_location__mobile',
) }}
SELECT * FROM {{ source('google_ads_search', 'google_ads_location') }} 
WHERE LOWER(campaign_name) LIKE '%mobile%'
