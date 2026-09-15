{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_location__mountain',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.google_ads_search_location(
    client_id=2977297812,
    funnel_search_default='BOOK'
) }}
