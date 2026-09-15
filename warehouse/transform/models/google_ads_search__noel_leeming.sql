{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='google_ads_search_transformed__noel_leeming',
    alias='google_ads_search__noel_leeming',
) }}

{{ google_ads.google_ads_search(client_id=7068886290) }}
