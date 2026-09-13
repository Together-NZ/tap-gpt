{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_demand_2'
) }}

{{ google_ads.google_ads_demand(client_id=2950734523) }}
