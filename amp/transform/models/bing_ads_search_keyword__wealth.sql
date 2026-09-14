{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.bing_ads_search_keyword(client_id=7701452964) }} where not (
    LOWER(campaign_name) like '%gi%' or lower(campaign_name) like '%general insurance%' 
or lower(campaign_name) like '%car%' or lower(campaign_name) like '%home%' or lower(campaign_name) like '%content%'
)
