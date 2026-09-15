{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
    schema='google_ads_search_transformed__twhs',
    alias='google_ads_search__twhs',
) }}

{{ google_ads.google_ads_search(client_id=3958866521) }}
