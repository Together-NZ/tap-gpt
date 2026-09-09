{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twh',
    alias='bing_ads_search__twh',
) }}

{{ google_ads.bing_ads_search_sa360(
    client_id=4824241958,
    sa360_dataset='sa360_data_transfer_warehouse',
    transfer_id=8223689145,
) }}
