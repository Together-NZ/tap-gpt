{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__ENERGY', 'google_ads_search_transformed__energy'),
    alias='bing_ads_search_keyword__energy',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}
SELECT * FROM {{ source('google_ads_search', 'bing_ads_search_keyword') }} 
WHERE LOWER(campaign_name) NOT LIKE '%mobile%'
  AND LOWER(campaign_name) NOT LIKE '%broadband%'
