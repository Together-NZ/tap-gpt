{{ config(
    materialized='table',
) }}
with dash_table AS (
{{dash_table_general_process.google_ads_search(source_name='google_ads__credit_card', table_name='google_ads_search__credit_card')}}
)
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}
