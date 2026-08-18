{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX__MOBILE', 'google_ads_search_transformed__mobile'),
    alias='google_ads_keyword__mobile',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}
SELECT * FROM {{ source('google_ads_search', 'google_ads_keyword') }} 
WHERE LOWER(campaign_name) LIKE '%mobile%'
