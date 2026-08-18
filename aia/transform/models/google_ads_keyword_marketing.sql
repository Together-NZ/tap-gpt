{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_keyword_marketing',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.google_ads_search_keyword(client_id=7073793042) }}
