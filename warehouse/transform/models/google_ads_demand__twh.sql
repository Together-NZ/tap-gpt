{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='google_ads_search_transformed__twh',
    alias='google_ads_demand__twh',
) }}

{{ google_ads.google_ads_demand(client_id=5427952977) }}
