{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    schema='google_ads_search_transformed__volvo',
    alias='google_ads_keyword__volvo',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

{{ google_ads.google_ads_search_keyword(client_id=4308178898) }}
