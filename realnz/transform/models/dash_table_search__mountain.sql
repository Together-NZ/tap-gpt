{{ config(
    materialized='table',
    alias='dash_table_search__mountain'
) }}

WITH dash_table AS (
    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads_search_transformed__mountain',
        table_name='google_ads_search__mountain'
    ) }}
)

{{ dash_table_general_process.dash_table_search_general_process('BOOK') }}
