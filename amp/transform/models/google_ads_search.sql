{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
) }}
{{google_ads.google_ads_search(client_id=7705475878)}} 
and LOWER(campaign_name) like '%gi%' or lower(campaign_name) like '%general insurance%' 
or lower(campaign_name) like '%car%' or lower(campaign_name) like '%home%' or lower(campaign_name) like '%content%'
