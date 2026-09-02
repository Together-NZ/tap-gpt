{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}
{{ google_ads.google_ads_search(client_id=4362586476) }}
