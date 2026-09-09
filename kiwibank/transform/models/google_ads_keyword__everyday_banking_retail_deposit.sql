{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

(
{{ google_ads.google_ads_search_keyword(client_id=3244960310) }}
)

UNION ALL

(
{{ google_ads.google_ads_search_keyword(client_id=8385365194) }}
)
