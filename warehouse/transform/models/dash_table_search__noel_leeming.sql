{{ config(
    materialized='table',
    schema='dash_table_search__noel_leeming',
    alias='dash_table_search__noel_leeming',
) }}

WITH dash_table AS (


    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads__noel_leeming',
        table_name='google_ads_search__noel_leeming'
    ) }}
)

{{ dash_table_general_process.dash_table_search_general_process('INTENT') }}
