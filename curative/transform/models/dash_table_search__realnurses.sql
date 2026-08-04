{{ config(
    materialized='table',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads__realnurses',
        table_name='google_ads_search__realnurses'
    ) }}
)

{{ dash_table_general_process.dash_table_search_general_process('INTENT') }}
