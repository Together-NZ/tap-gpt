{{ config(
    materialized='table',
    schema='dash_table_search__twhs',
    alias='dash_table_search__twhs',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.bing_ads_search(
        source_name='google_ads__twhs',
        table_name='bing_ads_search__twhs'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads__twhs',
        table_name='google_ads_search__twhs'
    ) }}
)

{{ dash_table_general_process.dash_table_search_general_process('INTENT') }}
