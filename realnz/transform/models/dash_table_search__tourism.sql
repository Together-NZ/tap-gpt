{{ config(
    materialized='table',
    alias='dash_table_search__tourism'
) }}

WITH dash_table AS (
    {{ dash_table_general_process.bing_ads_search(
        source_name='google_ads_search_transformed__tourism',
        table_name='bing_ads_search__tourism'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads_search_transformed__tourism',
        table_name='google_ads_search__tourism'
    ) }}
)

{{ dash_table_general_process.dash_table_search_general_process('BOOK') }}
