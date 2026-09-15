{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='google_ads_search_transformed__volvo',
    alias='google_ads_search__volvo',
) }}

{{ google_ads.google_ads_search(client_id=4308178898) }}
