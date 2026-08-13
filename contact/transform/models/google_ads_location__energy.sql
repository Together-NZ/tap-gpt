{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__ENERGY', 'google_ads_search_transformed__energy'),
    alias='google_ads_location__energy',
) }}
SELECT * FROM {{ source('google_ads_search', 'google_ads_location') }} 
WHERE LOWER(campaign_name) NOT IN (
    SELECT DISTINCT campaign_name FROM {{ source('google_ads_search__broadband', 'google_ads_location__broadband') }}, {{ source('google_ads_search__mobile', 'google_ads_location__mobile') }}
)
